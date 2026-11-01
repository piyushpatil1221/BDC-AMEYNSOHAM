"""Build the replay subset: first N days of MIND-small train, sampled to ~U users.

Usage: python -m replay.subset [--days 2] [--users 5000]
Writes <subset_dir>/behaviors.tsv (time as epoch ms, sorted by time) and <subset_dir>/news.tsv
(only articles that appear as impression candidates, i.e. the articles the replay will publish).
"""
import argparse
import csv
import random
import shutil
from pathlib import Path

from replay import config
from replay.mind import read_behaviors, write_behaviors

DAY_MS = 24 * 3600 * 1000


def main() -> None:
    cfg = config.load()
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=cfg["replay"]["days"])
    ap.add_argument("--users", type=int, default=cfg["replay"]["users"])
    ap.add_argument("--seed", type=int, default=cfg["replay"]["seed"])
    args = ap.parse_args()

    raw = Path(cfg["data"]["raw_dir"]) / "train"
    out = Path(cfg["data"]["subset_dir"])
    out.mkdir(parents=True, exist_ok=True)

    # Two streaming passes so only the sampled rows are ever held in memory (container limit 400 MB).
    behaviors = raw / "behaviors.tsv"
    t0 = min(r.ts_ms for r in read_behaviors(behaviors))
    end = t0 + args.days * DAY_MS
    users = sorted({r.user_id for r in read_behaviors(behaviors) if r.ts_ms < end})
    keep = set(random.Random(args.seed).sample(users, min(args.users, len(users))))
    subset = sorted((r for r in read_behaviors(behaviors) if r.ts_ms < end and r.user_id in keep),
                    key=lambda r: (r.ts_ms, r.impression_id))
    write_behaviors(out / "behaviors.tsv", subset)

    needed = {nid for r in subset for nid, _ in r.candidates}
    with (raw / "news.tsv").open(encoding="utf-8", newline="") as src, \
         (out / "news.tsv").open("w", encoding="utf-8", newline="") as dst:
        for line in src:
            if line.split("\t", 1)[0] in needed:
                dst.write(line)
    shutil.copyfile(raw / "news.tsv", out / "news_full.tsv")  # full catalogue for offline eval

    n_cands = sum(len(r.candidates) for r in subset)
    n_clicks = sum(c for r in subset for _, c in r.candidates)
    print(f"window: {args.days} day(s) from t0={t0} | users in window: {len(users)} -> sampled {len(keep)}")
    print(f"impressions: {len(subset)} | candidate events: {n_cands} | clicks: {n_clicks} "
          f"(CTR {n_clicks / max(n_cands, 1):.3%}) | articles: {len(needed)}")


if __name__ == "__main__":
    csv.field_size_limit(1 << 24)
    main()
