"""Steady /recommend traffic for latency metrics: N requests/s against currently active users.

Usage: python -m replay.loadgen [--rps 20] [--seconds 60] [--url http://serving:8000]
"""
from __future__ import annotations

import argparse
import json
import random
import statistics
import threading
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor


def get(url: str, timeout: float = 2.0):
    with urllib.request.urlopen(url, timeout=timeout) as r:
        return json.loads(r.read())


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", default="http://serving:8000")
    ap.add_argument("--rps", type=float, default=20)
    ap.add_argument("--seconds", type=float, default=60)
    args = ap.parse_args()

    users: list[str] = []
    results: list[tuple[float, str]] = []
    lock = threading.Lock()

    def refresh_users() -> None:
        nonlocal users
        fresh = [u["user_id"] for u in get(f"{args.url}/users/active?limit=200")]
        users = fresh or users

    def one(uid: str) -> None:
        try:
            r = get(f"{args.url}/recommend/{uid}?k=10")
            with lock:
                results.append((r["latency_ms"], r["path"]))
        except Exception:
            with lock:
                results.append((float("nan"), "error"))

    refresh_users()
    if not users:
        raise SystemExit("no active users yet: run the replay first")
    end = time.time() + args.seconds
    next_refresh = time.time() + 10
    with ThreadPoolExecutor(max_workers=8) as pool:
        while time.time() < end:
            pool.submit(one, random.choice(users))
            if time.time() > next_refresh:
                refresh_users()
                next_refresh = time.time() + 10
            time.sleep(1 / args.rps)

    lat = sorted(l for l, p in results if p != "error")
    q = lambda f: lat[min(len(lat) - 1, int(f * len(lat)))] if lat else float("nan")  # noqa: E731
    paths = {p: sum(1 for _, x in results if x == p) for p in {x for _, x in results}}
    print(f"requests {len(results)} | p50 {q(0.5):.1f} ms | p95 {q(0.95):.1f} ms | p99 {q(0.99):.1f} ms | "
          f"mean {statistics.fmean(lat) if lat else float('nan'):.1f} ms | paths {paths}")


if __name__ == "__main__":
    main()
