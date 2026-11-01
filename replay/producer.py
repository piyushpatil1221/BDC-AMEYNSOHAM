"""Replay the MIND subset into Kafka in event-time order at a configurable speed-up.

Usage: python -m replay.producer [--speedup 60] [--max-minutes N]

* Articles go to `articles.raw`, keyed by article id. MIND has no publish time, so each
  article is "published" `publish_offset_s` before its first impression. That keeps cold
  start real: the article reaches the pipeline with zero interactions, moments before
  users can see it.
* Every impression candidate becomes an `impression` event and, if clicked, also a `click`
  event, on `events.raw` keyed by user id. Events carry only the article id; Kafka Streams
  enriches them with category/subcategory.
* Event time lives in the payload (`ts`, `publish_ts`, epoch ms, year 2019). Kafka's record
  timestamp is left as ingestion time so the broker's 1 h retention isn't applied to
  2019 timestamps (which would delete records immediately).
"""
from __future__ import annotations

import argparse
import heapq
import json
import time
from pathlib import Path
from typing import Iterator

from confluent_kafka import Producer

from replay import config
from replay.mind import read_behaviors, read_news

# Tie-break order for items with the same event time: article before its events, impression before click.
ARTICLE, IMPRESSION, CLICK = 0, 1, 2
Item = tuple[int, int, str, str, dict]  # (ts_ms, order, topic, key, payload)


def article_items(subset: Path, topic: str, offset_ms: int) -> list[Item]:
    news = read_news(subset / "news.tsv")
    first_seen: dict[str, int] = {}
    for imp in read_behaviors(subset / "behaviors.tsv", epoch_ms=True):
        for nid, _ in imp.candidates:
            first_seen.setdefault(nid, imp.ts_ms)  # rows are time-sorted, so first hit = earliest
    items = []
    for nid, first in first_seen.items():
        meta = news[nid]
        publish = first - offset_ms
        items.append((publish, ARTICLE, topic, nid, {
            "article_id": nid, "publish_ts": publish, "category": meta["category"],
            "subcategory": meta["subcategory"], "title": meta["title"],
            "abstract": meta["abstract"], "url": meta["url"]}))
    items.sort(key=lambda it: (it[0], it[3]))
    return items


def event_items(subset: Path, topic: str) -> Iterator[Item]:
    for imp in read_behaviors(subset / "behaviors.tsv", epoch_ms=True):
        for nid, clicked in imp.candidates:
            base = {"user_id": imp.user_id, "article_id": nid,
                    "impression_id": imp.impression_id, "ts": imp.ts_ms}
            yield (imp.ts_ms, IMPRESSION, topic, imp.user_id,
                   {"event_id": f"{imp.impression_id}:{nid}:impression", "type": "impression", **base})
            if clicked:
                yield (imp.ts_ms, CLICK, topic, imp.user_id,
                       {"event_id": f"{imp.impression_id}:{nid}:click", "type": "click", **base})


def main() -> None:
    cfg = config.load()
    ap = argparse.ArgumentParser()
    ap.add_argument("--speedup", type=float, default=cfg["replay"]["speedup"])
    ap.add_argument("--max-minutes", type=float, default=None,
                    help="stop after this many minutes of *event* time (smoke tests, ablation)")
    args = ap.parse_args()

    topics = cfg["kafka"]["topics"]
    subset = Path(cfg["data"]["subset_dir"])
    articles = article_items(subset, topics["articles_raw"], cfg["replay"]["publish_offset_s"] * 1000)
    timeline = heapq.merge(articles, event_items(subset, topics["events_raw"]), key=lambda it: it[:2])

    producer = Producer({"bootstrap.servers": cfg["kafka"]["bootstrap"], "linger.ms": 20,
                         "compression.type": "lz4", "enable.idempotence": True})
    t0_event: int | None = None
    wall0 = time.monotonic()
    sent = {ARTICLE: 0, IMPRESSION: 0, CLICK: 0}
    last_log = wall0

    for ts, kind, topic, key, payload in timeline:
        if t0_event is None:
            t0_event = ts
        elapsed_event_min = (ts - t0_event) / 60_000
        if args.max_minutes is not None and elapsed_event_min > args.max_minutes:
            break
        # Pace: event-time offset divided by the speed-up gives the wall-clock send time.
        delay = wall0 + (ts - t0_event) / 1000 / args.speedup - time.monotonic()
        if delay > 0:
            time.sleep(delay)
        payload["emitted_at"] = int(time.time() * 1000)
        producer.produce(topic, key=key, value=json.dumps(payload, separators=(",", ":")))
        producer.poll(0)
        sent[kind] += 1

        now = time.monotonic()
        if now - last_log >= 10:
            print(f"[replay] event-time +{elapsed_event_min:7.1f} min | articles {sent[ARTICLE]} "
                  f"impressions {sent[IMPRESSION]} clicks {sent[CLICK]}", flush=True)
            last_log = now

    producer.flush(30)
    print(f"[replay] done in {time.monotonic() - wall0:.0f}s wall | articles {sent[ARTICLE]} "
          f"impressions {sent[IMPRESSION]} clicks {sent[CLICK]}", flush=True)


if __name__ == "__main__":
    main()
