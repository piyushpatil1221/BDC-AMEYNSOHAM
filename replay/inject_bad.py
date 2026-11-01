"""Inject known-bad and duplicate records into the raw topics to exercise the cleaner.

Sends: 2 malformed JSON + 3 schema violations + 1 unknown-article event (all -> dlq),
and re-sends the first 5 articles and 5 events already on the raw topics (-> dropped as duplicates).
"""
import json

from confluent_kafka import Consumer, Producer

from replay import config


def first_n(bootstrap: str, topic: str, n: int) -> list[bytes]:
    c = Consumer({"bootstrap.servers": bootstrap, "group.id": f"inject-{topic}",
                  "auto.offset.reset": "earliest", "enable.auto.commit": False})
    c.subscribe([topic])
    out = []
    while len(out) < n:
        m = c.poll(10)
        if m is None:
            break
        if not m.error():
            out.append(m.value())
    c.close()
    return out


def main() -> None:
    cfg = config.load()
    t, bs = cfg["kafka"]["topics"], cfg["kafka"]["bootstrap"]
    p = Producer({"bootstrap.servers": bs})
    bad_articles = [
        b"{not json",
        json.dumps({"article_id": "N1", "category": "news"}).encode(),            # missing fields
        json.dumps({"article_id": "N2", "publish_ts": "yesterday", "category": "news", "subcategory": "x",
                    "title": "t", "abstract": "", "url": "", "emitted_at": 1}).encode(),  # wrong type
    ]
    bad_events = [
        b"[1, 2, 3]",
        json.dumps({"event_id": "x", "type": "share", "user_id": "U1", "article_id": "N1",
                    "impression_id": "1", "ts": 1, "emitted_at": 1}).encode(),   # bad enum
        json.dumps({"event_id": "orphan:1", "type": "click", "user_id": "U1", "article_id": "N999999999",
                    "impression_id": "1", "ts": 1, "emitted_at": 1}).encode(),   # unknown article
    ]
    for v in bad_articles:
        p.produce(t["articles_raw"], v)
    for v in bad_events:
        p.produce(t["events_raw"], v)
    dup_a = first_n(bs, t["articles_raw"], 5)
    dup_e = first_n(bs, t["events_raw"], 5)
    for v in dup_a:
        p.produce(t["articles_raw"], v)
    for v in dup_e:
        p.produce(t["events_raw"], v)
    p.flush(10)
    print(f"injected: {len(bad_articles) + len(bad_events)} invalid (expect dlq), "
          f"{len(dup_a) + len(dup_e)} duplicates (expect dropped)")


if __name__ == "__main__":
    main()
