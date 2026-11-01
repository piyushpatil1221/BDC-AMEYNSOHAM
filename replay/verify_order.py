"""Check the cold-start guarantee on what actually landed in Kafka.

For every article seen on the events topic: the article record must exist, its publish_ts
must precede its first event's ts, and it must have been emitted (wall clock) no later than
that first event. Defaults to the raw topics; pass --clean for the Kafka Streams outputs.
"""
import argparse
import json

from confluent_kafka import Consumer, TopicPartition

from replay import config


def drain(bootstrap: str, topic: str) -> list[dict]:
    c = Consumer({"bootstrap.servers": bootstrap, "group.id": f"verify-{topic}",
                  "enable.auto.commit": False, "auto.offset.reset": "earliest"})
    meta = c.list_topics(topic, timeout=10).topics[topic]
    parts = [TopicPartition(topic, p, 0) for p in meta.partitions]
    ends = {p.partition: c.get_watermark_offsets(p, timeout=10)[1] for p in parts}
    c.assign([TopicPartition(topic, p.partition, c.get_watermark_offsets(p, timeout=10)[0]) for p in parts])
    out, done = [], {p for p, end in ends.items() if end == 0}
    while len(done) < len(ends):
        msg = c.poll(5)
        if msg is None:
            break
        if msg.error():
            continue
        out.append(json.loads(msg.value()))
        if msg.offset() + 1 >= ends[msg.partition()]:
            done.add(msg.partition())
    c.close()
    return out


def main() -> None:
    cfg = config.load()
    ap = argparse.ArgumentParser()
    ap.add_argument("--clean", action="store_true")
    args = ap.parse_args()
    t = cfg["kafka"]["topics"]
    a_topic, e_topic = (t["articles_clean"], t["events_clean"]) if args.clean else (t["articles_raw"], t["events_raw"])

    articles = {a["article_id"]: a for a in drain(cfg["kafka"]["bootstrap"], a_topic)}
    events = drain(cfg["kafka"]["bootstrap"], e_topic)
    first: dict[str, dict] = {}
    for e in events:
        if e["article_id"] not in first or e["ts"] < first[e["article_id"]]["ts"]:
            first[e["article_id"]] = e

    missing = [aid for aid in first if aid not in articles]
    late_publish = [aid for aid, e in first.items() if aid in articles and articles[aid]["publish_ts"] >= e["ts"]]
    late_emit = [aid for aid, e in first.items() if aid in articles and articles[aid]["emitted_at"] > e["emitted_at"]]
    clicks = sum(e["type"] == "click" for e in events)
    lead = sorted(e["emitted_at"] - articles[a]["emitted_at"] for a, e in first.items() if a in articles)

    print(f"{a_topic}: {len(articles)} articles | {e_topic}: {len(events)} events ({clicks} clicks)")
    print(f"articles with events: {len(first)} | missing article record: {len(missing)} | "
          f"publish_ts not before first event: {len(late_publish)} | emitted after first event: {len(late_emit)}")
    if lead:
        print(f"wall-clock lead of article over its first event: median {lead[len(lead) // 2]} ms, min {lead[0]} ms")
    ok = not (missing or late_publish or late_emit)
    print("ORDERING OK" if ok else "ORDERING VIOLATIONS FOUND")
    raise SystemExit(0 if ok else 1)


if __name__ == "__main__":
    main()
