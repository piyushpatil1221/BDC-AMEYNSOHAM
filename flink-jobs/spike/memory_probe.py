"""Phase 0 spike: does a PyFlink job shaped like ours fit in a 1400 MB TaskManager?

Topology mirrors the real job: three keyed Python stages (user -> topic -> article bucket),
each with RocksDB keyed state + TTL, and River ADWIN in the topic stage. A synthetic
unbounded source pushes events as fast as the job can take them, which is harsher
than the real replay.
"""
import random

from pyflink.common import Types, WatermarkStrategy
from pyflink.common.time import Time
from pyflink.datastream import KeyedProcessFunction, RuntimeContext, StreamExecutionEnvironment
from pyflink.datastream.connectors.number_seq import NumberSequenceSource
from pyflink.datastream.state import MapStateDescriptor, StateTtlConfig, ValueStateDescriptor
from pyflink.datastream.state_backend import EmbeddedRocksDBStateBackend

DIM = 384
N_USERS, N_TOPICS, N_BUCKETS = 5000, 18, 64


def ttl(minutes: int) -> StateTtlConfig:
    # Background cleanup is on by default, which for RocksDB means the compaction filter
    # (query time refreshed every 1000 entries). Calling cleanup_in_rocksdb_compact_filter()
    # explicitly crashes in PyFlink 1.20 workers: its default Duration needs the JVM gateway.
    return (StateTtlConfig.new_builder(Time.minutes(minutes))
            .set_update_type(StateTtlConfig.UpdateType.OnCreateAndWrite)
            .set_state_visibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
            .build())


class SessionStage(KeyedProcessFunction):
    """Keeps a 384-d running mean vector per user (stand-in for the session vector)."""

    def open(self, ctx: RuntimeContext) -> None:
        d = ValueStateDescriptor("vec", Types.PRIMITIVE_ARRAY(Types.FLOAT()))
        d.enable_time_to_live(ttl(20))
        self.vec = ctx.get_state(d)

    def process_element(self, value, ctx):
        v = self.vec.value() or [0.0] * DIM
        rnd = random.random()
        self.vec.update([0.9 * x + 0.1 * rnd for x in v])
        yield value


class TopicStage(KeyedProcessFunction):
    """EWMA CTR + ADWIN per topic (the drift stage)."""

    def open(self, ctx: RuntimeContext) -> None:
        from river import drift
        self.detectors = {}
        self._drift = drift
        d = ValueStateDescriptor("ewma", Types.DOUBLE())
        d.enable_time_to_live(ttl(24 * 60))
        self.ewma = ctx.get_state(d)

    def process_element(self, value, ctx):
        clicked = 1.0 if value[0] % 10 == 0 else 0.0
        self.ewma.update(0.98 * (self.ewma.value() or 0.0) + 0.02 * clicked)
        det = self.detectors.setdefault(ctx.get_current_key(), self._drift.ADWIN())
        det.update(clicked)
        yield value


class CacheStage(KeyedProcessFunction):
    """Bounded per-bucket MapState of article embeddings (the LRU cache stage)."""

    def open(self, ctx: RuntimeContext) -> None:
        d = MapStateDescriptor("emb", Types.STRING(), Types.PRIMITIVE_ARRAY(Types.FLOAT()))
        d.enable_time_to_live(ttl(6 * 60))
        self.cache = ctx.get_map_state(d)

    def process_element(self, value, ctx):
        self.cache.put(f"N{value[0] % 20000}", [0.1] * DIM)
        yield value


def main() -> None:
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    env.set_state_backend(EmbeddedRocksDBStateBackend(enable_incremental_checkpointing=True))
    env.enable_checkpointing(30_000)

    src = env.from_source(NumberSequenceSource(0, 10**12), WatermarkStrategy.no_watermarks(), "seq")
    t = Types.TUPLE([Types.LONG(), Types.LONG(), Types.LONG(), Types.LONG()])
    events = src.map(lambda i: (i, i % N_USERS, i % N_TOPICS, i % N_BUCKETS), output_type=t)
    (events.key_by(lambda e: e[1], Types.LONG()).process(SessionStage(), t)
           .key_by(lambda e: e[2], Types.LONG()).process(TopicStage(), t)
           .key_by(lambda e: e[3], Types.LONG()).process(CacheStage(), t)
           .filter(lambda e: e[0] % 1_000_000 == 0).print())
    env.execute("phase0-memory-probe")


if __name__ == "__main__":
    main()
