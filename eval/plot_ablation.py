"""Plot reports/ablation.csv -> three single-measure charts (one y-axis each) + a markdown table."""
from __future__ import annotations

import csv
import json
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

REPORTS = Path("/w/reports")
# Reference categorical slots 1-2 (validated adjacent pair), light surface and text tokens.
COLORS = {"on": "#2a78d6", "off": "#eb6834"}
LABELS = {"on": "TTL + LRU eviction ON", "off": "Eviction OFF"}
SURFACE, TEXT, MUTED, GRID = "#fcfcfb", "#0b0b0b", "#52514e", "#e4e3df"


MAX_EVENT_MIN = None  # set from the summary: only the replayed window, not the drain afterwards


def series(rows, mode, key):
    pts = [(int(r["event_min"]) / 60, float(r[key])) for r in rows
           if r["mode"] == mode and r[key] not in ("", "None") and float(r["state_mb"]) > 0
           and (MAX_EVENT_MIN is None or int(r["event_min"]) <= MAX_EVENT_MIN)]
    return [p[0] for p in pts], [p[1] for p in pts]


def chart(rows, key, title, ylabel, fname, cutoff=None, limit=None, legend_loc="upper left"):
    fig, ax = plt.subplots(figsize=(8, 4.2), dpi=150)
    fig.patch.set_facecolor(SURFACE)
    ax.set_facecolor(SURFACE)
    ends = {}
    for mode in ("on", "off"):
        x, y = series(rows, mode, key)
        if not x:
            continue
        ax.plot(x, y, color=COLORS[mode], linewidth=2, label=LABELS[mode], solid_capstyle="round")
        ends[mode] = (x[-1], y[-1])
    # Direct end labels; nudge them apart when the two lines finish close together.
    span = max((v for _, v in ends.values()), default=1) or 1
    close = len(ends) == 2 and abs(ends["on"][1] - ends["off"][1]) < 0.08 * span
    for mode, (xe, ye) in ends.items():
        dy = (7 if ends[mode][1] >= ends[{"on": "off", "off": "on"}[mode]][1] else -7) if close else 0
        ax.annotate(LABELS[mode], (xe, ye), xytext=(6, dy), textcoords="offset points",
                    color=TEXT, fontsize=8, va="center")
    if limit is not None:
        ax.axhline(limit, color=MUTED, linewidth=1, linestyle="--")
        ax.annotate(f"watchdog ({limit:.0f})", (0, limit), xytext=(2, 3), textcoords="offset points",
                    color=MUTED, fontsize=8)
    if cutoff:
        ax.axvline(cutoff["event_min"] / 60, color=MUTED, linewidth=1, linestyle=":")
        ax.annotate("OFF run stopped", (cutoff["event_min"] / 60, ax.get_ylim()[1]), xytext=(3, -12),
                    textcoords="offset points", color=MUTED, fontsize=8)
    ax.set_title(title, color=TEXT, fontsize=11, loc="left")
    ax.set_xlabel("Event time replayed (hours)", color=MUTED, fontsize=9)
    ax.set_ylabel(ylabel, color=MUTED, fontsize=9)
    ax.grid(True, color=GRID, linewidth=0.8)
    ax.set_axisbelow(True)
    for s in ("top", "right"):
        ax.spines[s].set_visible(False)
    for s in ("left", "bottom"):
        ax.spines[s].set_color(GRID)
    ax.tick_params(colors=MUTED, labelsize=8)
    ax.set_ylim(bottom=0)
    ax.margins(x=0.12)
    ax.legend(frameon=False, fontsize=8, labelcolor=TEXT, loc=legend_loc)
    fig.tight_layout()
    fig.savefig(REPORTS / fname, facecolor=SURFACE)
    plt.close(fig)


def main() -> None:
    global MAX_EVENT_MIN
    rows = list(csv.DictReader((REPORTS / "ablation.csv").open()))
    summary = json.loads((REPORTS / "ablation_summary.json").read_text())
    MAX_EVENT_MIN = summary[0]["event_minutes"]
    cutoff = next((s["watchdog_cutoff"] for s in summary if s["mode"] == "off" and s["watchdog_cutoff"]), None)
    chart(rows, "state_mb", "Flink keyed state size (full checkpoint)", "MB", "ablation_state_size.png", cutoff)
    chart(rows, "checkpoint_ms", "Checkpoint duration", "ms", "ablation_checkpoint_duration.png", cutoff,
          legend_loc="upper right")
    chart(rows, "tm_mem_mib", "TaskManager container memory", "MiB", "ablation_memory.png", cutoff,
          limit=1400 * 0.9, legend_loc="lower right")

    lines = ["# Eviction ablation", "",
             f"Same replay ({summary[0]['event_minutes'] / 60:.0f} h of event time at {summary[0]['speedup']:g}x), "
             "fresh state per run.", "",
             "| Run | Final state (MB) | Max state (MB) | Mean checkpoint (ms) | Max checkpoint (ms) | Peak TM memory (MiB) | Watchdog cutoff |",
             "|---|---|---|---|---|---|---|"]
    for s in summary:
        c = s["watchdog_cutoff"]
        cut = f"at {c['event_min'] / 60:.1f} h event time ({c['tm_mem_mib']:.0f} MiB)" if c else "not reached"
        lines.append(f"| {LABELS[s['mode']]} | {s['final_state_mb']} | {s['max_state_mb']} | {s['mean_checkpoint_ms']} | "
                     f"{s['max_checkpoint_ms']} | {s['peak_tm_mem_mib']} | {cut} |")
    (REPORTS / "ablation.md").write_text("\n".join(lines) + "\n")
    print("\n".join(lines))


if __name__ == "__main__":
    main()
