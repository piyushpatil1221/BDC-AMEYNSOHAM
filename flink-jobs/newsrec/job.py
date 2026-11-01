"""PyFlink job: the whole stateful pipeline of report Section 5.1 in one job graph.

  articles.clean ─┐                       ┌─> [1] EmbedBatcher ──> Qdrant, articles.embedded
                  ├─ parse ─ watermarks ──┤         │ (embedded articles)
  events.clean ───┘     (event time)      │         v
                                          ├─> [cache] EmbeddingCacheJoin <── clicks
                                          │         │ (click + embedding)
                                          │         v
                                          │   [2] session windows (per user) ──> ScyllaDB session_vectors
                                          └─> [3] TopicDecayDrift (per category) ──> ScyllaDB, drift.events

Usage: flink run -py job.py [--eviction on|off] [--speedup 60]
"""
from __future__ import annotations

import argparse
import os

import yaml
from pyflink.common import Configuration, Duration, Types, WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.time import Time
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import (KafkaOffsetsInitializer, KafkaRecordSerializationSchema, KafkaSink,
                                                 KafkaSource)
from pyflink.datastream.state_backend import EmbeddedRocksDBStateBackend
from pyflink.datastream.window import CountTrigger, EventTimeSessionWindows

from newsrec.operators import (ARTICLES_TAG, DRIFT_JSON_TAG, EMBEDDED_JSON_TAG, PICKLED, EmbedBatcher,
                               EmbeddingCacheJoin, EventTimeAssigner, ParseJson, Route, SessionVectorAgg,
                               SessionWriter, TopicDecayDrift, bucket)


def main() -> None:
    cfg = yaml.safe_load(open(os.environ.get("PIPELINE_CONFIG", "/config/pipeline.yaml"), encoding="utf-8"))
    ap = argparse.ArgumentParser()
    ap.add_argument("--eviction", choices=["on", "off"], default="on" if cfg["eviction"]["enabled"] else "off")
    ap.add_argument("--speedup", type=float, default=cfg["replay"]["speedup"])
    ap.add_argument("--python-mode", choices=["process", "thread"], default=cfg["flink"]["python_execution_mode"])
    args = ap.parse_args()
    eviction = args.eviction == "on"
    f, topics, bootstrap = cfg["flink"], cfg["kafka"]["topics"], cfg["kafka"]["bootstrap"]

    # "thread" runs Python operators inside the TaskManager JVM (PEMJA): state access and record
    # hand-off are in-process calls instead of gRPC round trips to separate Python workers.
    conf = Configuration()
    conf.set_string("python.execution-mode", args.python_mode)
    env = StreamExecutionEnvironment.get_execution_environment(conf)
    env.set_parallelism(1)
    env.set_state_backend(EmbeddedRocksDBStateBackend(enable_incremental_checkpointing=True))
    env.enable_checkpointing(30_000)

    offsets = KafkaOffsetsInitializer.earliest() if f["start_from"] == "earliest" else KafkaOffsetsInitializer.latest()

    def source(topic: str):
        return (KafkaSource.builder().set_bootstrap_servers(bootstrap).set_topics(topic)
                .set_group_id("newsrec-flink").set_starting_offsets(offsets)
                .set_value_only_deserializer(SimpleStringSchema()).build())

    def sink(topic: str):
        return (KafkaSink.builder().set_bootstrap_servers(bootstrap)
                .set_record_serializer(KafkaRecordSerializationSchema.builder().set_topic(topic)
                                       .set_value_serialization_schema(SimpleStringSchema()).build())
                .build())

    # Ingestion: one Python chain parses both topics and assigns event-time watermarks.
    watermarks = (WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_seconds(f["watermark_out_of_orderness_s"]))
                  .with_timestamp_assigner(EventTimeAssigner()))
    articles_src = env.from_source(source(topics["articles_clean"]), WatermarkStrategy.no_watermarks(), "articles.clean")
    events_src = env.from_source(source(topics["events_clean"]), WatermarkStrategy.no_watermarks(), "events.clean")
    routed = (articles_src.union(events_src)
              .map(ParseJson(), output_type=PICKLED)
              .assign_timestamps_and_watermarks(watermarks)
              .process(Route(), output_type=PICKLED))
    events = routed
    articles = routed.get_side_output(ARTICLES_TAG)
    clicks = events.filter(lambda e: e["type"] == "click")

    # [1] Cold-start embedding: a single key so every new article joins the same micro-batch.
    embedded = articles.key_by(lambda a: 0, key_type=Types.INT()).process(EmbedBatcher(cfg), output_type=PICKLED)
    embedded.get_side_output(EMBEDDED_JSON_TAG).sink_to(sink(topics["articles_embedded"])).name("articles.embedded")

    # Embedding cache (bounded LRU in keyed state) joined with clicks.
    b = f["cache_buckets"]
    clicked_vectors = (embedded.connect(clicks)
                       .key_by(lambda a: bucket(a["article_id"], b), lambda c: bucket(c["article_id"], b),
                               key_type=Types.INT())
                       .process(EmbeddingCacheJoin(cfg, args.speedup, eviction), output_type=PICKLED))

    # [2] Event-time session windows per user; CountTrigger(1) emits the evolving vector on every click.
    (clicked_vectors.key_by(lambda c: c["user_id"], key_type=Types.STRING())
     .window(EventTimeSessionWindows.with_gap(Time.minutes(f["session_gap_min"])))
     .trigger(CountTrigger.of(1))
     .aggregate(SessionVectorAgg(f["session_half_life_min"] * 60.0),
                window_function=SessionWriter(cfg, args.speedup, eviction),
                # Fresh TypeInformation: PyFlink pickles the accumulator type into the window operator,
                # and an instance already used as an output_type caches an unpicklable JVM handle.
                accumulator_type=Types.PICKLED_BYTE_ARRAY(), output_type=Types.STRING())
     .print())  # side-effect operator; the print sink only anchors it in the job graph (prints nothing)

    # [3] Topic decay + drift detection.
    topic = (articles.connect(events)
             .key_by(lambda a: a["category"], lambda e: e["category"], key_type=Types.STRING())
             .process(TopicDecayDrift(cfg, args.speedup, eviction), output_type=Types.STRING()))
    topic.get_side_output(DRIFT_JSON_TAG).sink_to(sink(topics["drift_events"])).name("drift.events")
    topic.print()

    env.execute(f"newsrec-pipeline eviction={args.eviction} speedup={args.speedup:g}")


if __name__ == "__main__":
    main()
