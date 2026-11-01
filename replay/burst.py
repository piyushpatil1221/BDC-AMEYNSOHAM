"""Breaking-news burst: flood one category with new articles and high-CTR clicks (drift demo).

Usage: python -m replay.burst [--category sports] [--articles 12] [--minutes 90] [--rate 40] [--ctr 0.35]

Runs alongside (or after) the normal replay. It reads the current event time from the tail of
events.raw, publishes `--articles` synthetic articles in that category, then for `--minutes` of
event time emits `--rate` impressions per event-minute with click probability `--ctr`, paced at
the replay speed-up. A marker on control.bursts records the burst start (ground truth for the
drift-detection lag metric).
"""
from __future__ import annotations

import argparse
import json
import random
import time
from pathlib import Path

from confluent_kafka import Consumer, Producer, TopicPartition

from replay import config
from replay.mind import read_behaviors, read_news


def current_event_time(bootstrap: str, topic: str) -> int:
    c = Consumer({"bootstrap.servers": bootstrap, "group.id": "burst-clock", "enable.auto.commit": False})
    lo, hi = c.get_watermark_offsets(TopicPartition(topic, 0), timeout=10)
    if hi <= lo:
        raise SystemExit("events.raw is empty: start the replay first")
    c.assign([TopicPartition(topic, 0, hi - 1)])
    msg = None
    while msg is None or msg.error():
        msg = c.poll(5)
    c.close()
    return json.loads(msg.value())["ts"]


def main() -> None:
    cfg = config.load()
    ap = argparse.ArgumentParser()
    ap.add_argument("--category", default="sports")
    ap.add_argument("--articles", type=int, default=12)
    ap.add_argument("--minutes", type=float, default=90, help="burst duration in event-time minutes")
    ap.add_argument("--rate", type=int, default=40, help="impressions per event-time minute")
    ap.add_argument("--ctr", type=float, default=0.35)
    ap.add_argument("--speedup", type=float, default=cfg["replay"]["speedup"])
    ap.add_argument("--seed", type=int, default=7)
    args = ap.parse_args()

    rng = random.Random(args.seed)
    topics, bs = cfg["kafka"]["topics"], cfg["kafka"]["bootstrap"]
    subset = Path(cfg["data"]["subset_dir"])
    news = read_news(subset / "news.tsv")
    pool = [n for n in news.values() if n["category"] == args.category]
    if not pool:
        raise SystemExit(f"no articles in category {args.category!r}; try one of {sorted({n['category'] for n in news.values()})}")
    users = sorted({imp.user_id for imp in read_behaviors(subset / "behaviors.tsv", epoch_ms=True)})

    start_ts = current_event_time(bs, topics["events_raw"])
    burst_id = int(time.time()) % 100_000
    p = Producer({"bootstrap.servers": bs, "linger.ms": 20})
    now = lambda: int(time.time() * 1000)  # noqa: E731

    article_ids = []
    for i in range(args.articles):
        src = rng.choice(pool)
        aid = f"B{burst_id:05d}{i:03d}"
        article_ids.append(aid)
        p.produce(topics["articles_raw"], key=aid, value=json.dumps({
            "article_id": aid, "publish_ts": start_ts, "category": args.category, "subcategory": src["subcategory"],
            "title": f"BREAKING: {src['title']}", "abstract": src["abstract"], "url": src["url"], "emitted_at": now()}))
    p.flush(10)
    start_wall = now()
    p.produce(topics["bursts"], value=json.dumps({"burst_id": burst_id, "category": args.category,
                                                 "start_event_ts": start_ts, "start_wall_ts": start_wall,
                                                 "articles": args.articles, "ctr": args.ctr, "minutes": args.minutes}))
    print(f"[burst {burst_id}] {args.articles} '{args.category}' articles at event time {start_ts}", flush=True)
    time.sleep(1.0)  # let the articles get ahead of their first impressions (cold start stays real)

    n = clicks = 0
    total = int(args.minutes * args.rate)
    for j in range(total):
        ts = start_ts + int(j * 60_000 / args.rate) + 1000
        delay = start_wall / 1000 + (ts - start_ts) / 1000 / args.speedup - time.time()
        if delay > 0:
            time.sleep(delay)
        user, aid = rng.choice(users), rng.choice(article_ids)
        base = {"user_id": user, "article_id": aid, "impression_id": f"b{burst_id}-{j}", "ts": ts}
        p.produce(topics["events_raw"], key=user, value=json.dumps(
            {"event_id": f"burst{burst_id}:{j}:impression", "type": "impression", **base, "emitted_at": now()}))
        n += 1
        if rng.random() < args.ctr:
            p.produce(topics["events_raw"], key=user, value=json.dumps(
                {"event_id": f"burst{burst_id}:{j}:click", "type": "click", **base, "emitted_at": now()}))
            clicks += 1
        p.poll(0)
    p.flush(10)
    print(f"[burst {burst_id}] done: {n} impressions, {clicks} clicks over {args.minutes:g} event-minutes", flush=True)


if __name__ == "__main__":
    main()
