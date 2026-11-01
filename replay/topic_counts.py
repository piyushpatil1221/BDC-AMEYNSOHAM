"""Print the number of records in every pipeline topic (end offset - start offset)."""
from confluent_kafka import Consumer, TopicPartition

from replay import config


def main() -> None:
    cfg = config.load()
    c = Consumer({"bootstrap.servers": cfg["kafka"]["bootstrap"], "group.id": "topic-counts"})
    for topic in cfg["kafka"]["topics"].values():
        md = c.list_topics(topic, timeout=10).topics[topic]
        total = 0
        for p in md.partitions:
            lo, hi = c.get_watermark_offsets(TopicPartition(topic, p), timeout=10)
            total += hi - lo
        print(f"{topic:<20} {total}")
    c.close()


if __name__ == "__main__":
    main()
