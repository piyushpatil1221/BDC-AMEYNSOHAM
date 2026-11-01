"""Eviction ablation (the core CA2 experiment): same replay with TTL/eviction ON vs OFF.

Runs on the host (stdlib only) and drives Docker:
  for mode in (on, off):
    wipe all state -> start the core stack -> submit the Flink job with --eviction <mode>
    -> replay the same subset window -> every 10 s sample Flink's latest checkpoint
       (full state size, incremental size, duration) and the TaskManager's container memory.
A watchdog cancels a run if the TaskManager reaches 90% of its 1400 MiB limit and records the
cutoff (expected for OFF on long replays; the cutoff point is itself a result).
Writes reports/ablation.csv + reports/ablation_summary.json, then plots in the eval container.

Usage: python eval/ablation.py [--speedup 180] [--minutes 1440]
"""
from __future__ import annotations

import argparse
import csv
import json
import subprocess
import time
import urllib.request
from pathlib import Path

FLINK = "http://localhost:8081"
TM_LIMIT_MIB = 1400
WATCHDOG = 0.90
CORE = ["docker", "compose", "--profile", "core"]
ALL = ["docker", "compose", "--profile", "core", "--profile", "tools", "--profile", "submit", "--profile", "monitoring"]


def sh(args: list[str]) -> str:
    return subprocess.run(args, check=True, capture_output=True, text=True).stdout


def get(url: str):
    with urllib.request.urlopen(url, timeout=10) as r:
        return json.loads(r.read())


def wait(pred, timeout: float, what: str) -> None:
    end = time.time() + timeout
    while time.time() < end:
        try:
            if pred():
                return
        except Exception:
            pass
        time.sleep(3)
    raise SystemExit(f"timed out waiting for {what}")


def tm_mem_mib() -> float:
    out = sh(["docker", "stats", "--no-stream", "--format", "{{.Name}}|{{.MemUsage}}"])
    for line in out.splitlines():
        name, usage = line.split("|")
        if "taskmanager" in name:
            used = usage.split("/")[0].strip()
            num = float(used.rstrip("KMGiB"))
            return num * 1024 if used.endswith("GiB") else num / 1024 if used.endswith("KiB") else num
    return float("nan")


def running_job() -> str | None:
    jobs = [j for j in get(f"{FLINK}/jobs/overview")["jobs"] if j["state"] == "RUNNING"]
    return jobs[0]["jid"] if jobs else None


def run(mode: str, speedup: float, minutes: float, rows: list[dict]) -> dict:
    print(f"\n=== eviction {mode.upper()}: resetting stack", flush=True)
    sh(ALL + ["down", "-v"])
    sh(CORE + ["up", "-d"])
    wait(lambda: "exited" in sh(["docker", "inspect", "-f", "{{.State.Status}}", "sohamameybdc-init-1"]), 300, "init")
    wait(lambda: get(f"{FLINK}/overview")["slots-total"] >= 2, 180, "Flink slots")
    sh(["docker", "compose", "--profile", "core", "--profile", "submit", "run", "--rm", "--no-deps", "flink-submit",
        "--eviction", mode, "--speedup", str(speedup)])
    wait(lambda: running_job() is not None, 120, "job RUNNING")
    jid = running_job()
    time.sleep(10)

    replay = subprocess.Popen(["docker", "compose", "--profile", "core", "--profile", "tools", "run", "--rm", "--no-deps",
                               "replay", "replay.producer", "--speedup", str(speedup), "--max-minutes", str(minutes)],
                              stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    t0 = time.time()
    replay_done_at = None
    cutoff = None
    peak_mem = 0.0
    while True:
        time.sleep(10)
        t = time.time() - t0
        mem = tm_mem_mib()
        peak_mem = max(peak_mem, mem)
        cp = get(f"{FLINK}/jobs/{jid}/checkpoints")
        last = (cp.get("latest") or {}).get("completed") or {}
        row = {"mode": mode, "t_s": round(t), "event_min": round(t * speedup / 60),
               "state_mb": round(last.get("state_size", 0) / 1e6, 3),
               "checkpointed_mb": round(last.get("checkpointed_size", 0) / 1e6, 3),
               "checkpoint_ms": last.get("end_to_end_duration"),
               "checkpoints_ok": cp["counts"]["completed"], "checkpoints_failed": cp["counts"]["failed"],
               "tm_mem_mib": round(mem, 1)}
        rows.append(row)
        print(f"  t={row['t_s']:>4}s  state={row['state_mb']:>7} MB  ckpt={row['checkpoint_ms']} ms  "
              f"tm={row['tm_mem_mib']} MiB", flush=True)
        if mem >= WATCHDOG * TM_LIMIT_MIB:
            cutoff = {"t_s": row["t_s"], "event_min": row["event_min"], "tm_mem_mib": mem, "state_mb": row["state_mb"]}
            print(f"  WATCHDOG: TaskManager at {mem:.0f} MiB (>= {WATCHDOG:.0%}); cancelling", flush=True)
            urllib.request.urlopen(urllib.request.Request(f"{FLINK}/jobs/{jid}?mode=cancel", method="PATCH"), timeout=10)
            replay.kill()
            break
        if replay_done_at is None and replay.poll() is not None:
            replay_done_at = time.time()
        if replay_done_at is not None and time.time() - replay_done_at > 75:  # let 2 more checkpoints land
            break
    mode_rows = [r for r in rows if r["mode"] == mode and r["state_mb"] > 0]
    return {"mode": mode, "speedup": speedup, "event_minutes": minutes,
            "final_state_mb": mode_rows[-1]["state_mb"] if mode_rows else None,
            "max_state_mb": max((r["state_mb"] for r in mode_rows), default=None),
            "max_checkpoint_ms": max((r["checkpoint_ms"] or 0 for r in mode_rows), default=None),
            "mean_checkpoint_ms": round(sum(r["checkpoint_ms"] or 0 for r in mode_rows) / max(len(mode_rows), 1)),
            "peak_tm_mem_mib": round(peak_mem, 1), "watchdog_cutoff": cutoff}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--speedup", type=float, default=180)
    ap.add_argument("--minutes", type=float, default=1440, help="event-time minutes replayed per run")
    args = ap.parse_args()
    out = Path("reports")
    out.mkdir(exist_ok=True)
    rows: list[dict] = []
    summary = [run(mode, args.speedup, args.minutes, rows) for mode in ("on", "off")]
    with (out / "ablation.csv").open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0]))
        w.writeheader()
        w.writerows(rows)
    (out / "ablation_summary.json").write_text(json.dumps(summary, indent=2))
    print(json.dumps(summary, indent=2))
    sh(ALL + ["down"])
    subprocess.run(["docker", "compose", "--profile", "tools", "run", "--rm", "--no-deps", "eval", "eval.plot_ablation"],
                   check=True)


if __name__ == "__main__":
    main()
