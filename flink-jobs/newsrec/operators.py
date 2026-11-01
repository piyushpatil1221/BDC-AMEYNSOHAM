"""PyFlink operators for the three stages of report Section 5.1 plus ingestion helpers.

Records travel between Python operators as pickled dicts. Every stateful operator declares its
own State TTL (memory governance, Section 5.1): session = short, embedding cache = medium,
topic decay = long. With eviction disabled (ablation) no TTL is set and the LRU bound is lifted.
"""
from __future__ import annotations

import json
import time
import zlib
from datetime import datetime, timezone
from typing import Any, Iterable

import numpy as np
from pyflink.common import Types
from pyflink.common.time import Time
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.datastream import OutputTag
from pyflink.datastream.functions import (AggregateFunction, KeyedCoProcessFunction, KeyedProcessFunction,
                                          MapFunction, ProcessFunction, ProcessWindowFunction, RuntimeContext)
from pyflink.datastream.state import ListStateDescriptor, MapStateDescriptor, StateTtlConfig, ValueStateDescriptor

from newsrec import clients
from newsrec_core import session as sess
from newsrec_core.decay import smoothed_ctr
from newsrec_core.drift import DecayPolicy, ctr_from_indicator_mean, relax, respond_to_drift
from newsrec_core.ids import point_id
from newsrec_core.lru import lru_victims

PICKLED = Types.PICKLED_BYTE_ARRAY()
ARTICLES_TAG = OutputTag("articles", PICKLED)
EMBEDDED_JSON_TAG = OutputTag("embedded-json", Types.STRING())
DRIFT_JSON_TAG = OutputTag("drift-json", Types.STRING())


def now_ms() -> int:
    return int(time.time() * 1000)


def to_dt(ms: int | None) -> datetime | None:
    return None if ms is None else datetime.fromtimestamp(ms / 1000, tz=timezone.utc)


def bucket(article_id: str, buckets: int) -> int:
    # crc32, not hash(): Python's hash is salted per process and keys must agree across workers.
    return zlib.crc32(article_id.encode()) % buckets


def ttl_config(event_minutes: float, speedup: float, enabled: bool, refresh_on_read: bool = False) -> StateTtlConfig | None:
    """Flink State TTL is processing-time only, so event-time minutes are scaled by the replay speed-up.

    Background cleanup is on by default; for RocksDB that means the compaction filter drops expired
    entries during compaction, so checkpoints only carry live state. (Calling
    cleanup_in_rocksdb_compact_filter() explicitly crashes PyFlink 1.20 workers: its default argument
    needs the JVM gateway, which Python workers don't have.)
    """
    if not enabled:
        return None
    wall_ms = max(1000, int(event_minutes * 60_000 / speedup))
    update = StateTtlConfig.UpdateType.OnReadAndWrite if refresh_on_read else StateTtlConfig.UpdateType.OnCreateAndWrite
    return (StateTtlConfig.new_builder(Time.milliseconds(wall_ms))
            .set_update_type(update)
            .set_state_visibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
            .build())


def with_ttl(desc, ttl: StateTtlConfig | None):
    if ttl is not None:
        desc.enable_time_to_live(ttl)
    return desc


# --------------------------------------------------------------------------------------------
# Ingestion
# --------------------------------------------------------------------------------------------
class ParseJson(MapFunction):
    def map(self, value: str) -> dict:
        d = json.loads(value)
        d["_kind"] = "article" if "publish_ts" in d else "event"
        return d


class EventTimeAssigner(TimestampAssigner):
    """Event time comes from the payload (2019 MIND timestamps), never from Kafka."""

    def extract_timestamp(self, value: dict, record_timestamp: int) -> int:
        return value["publish_ts"] if value["_kind"] == "article" else value["ts"]


class Route(ProcessFunction):
    """Events on the main output, articles on a side output."""

    def process_element(self, value: dict, ctx):
        if value["_kind"] == "article":
            yield ARTICLES_TAG, value
        else:
            yield value


# --------------------------------------------------------------------------------------------
# Stage 1: cold-start embedding (micro-batched) -> Qdrant
# --------------------------------------------------------------------------------------------
class EmbedBatcher(KeyedProcessFunction):
    """Buffers new articles in keyed ListState (so a partial batch survives a failure) and flushes
    when `batch_size` is reached or `max_wait_ms` after the first buffered article."""

    def __init__(self, cfg: dict) -> None:
        self.embed_url = cfg["embedding"]["url"] + "/embed"
        q = cfg["qdrant"]
        self.upsert_url = f"{q['url']}/collections/{q['collection']}/points?wait=true"
        self.batch_size = cfg["flink"]["embed_batch_size"]
        self.max_wait_ms = cfg["flink"]["embed_max_wait_ms"]

    def open(self, rc: RuntimeContext) -> None:
        self.buffer = rc.get_list_state(ListStateDescriptor("buffer", PICKLED))
        self.timer = rc.get_state(ValueStateDescriptor("timer", Types.LONG()))
        mg = rc.get_metrics_group()
        self.m_batches = mg.counter("embed_batches")
        self.m_articles = mg.counter("embedded_articles")
        self.last_batch_ms = 0
        mg.gauge("embed_last_batch_ms", lambda: int(self.last_batch_ms))

    def process_element(self, value: dict, ctx):
        self.buffer.add(value)
        items = list(self.buffer.get())
        if len(items) >= self.batch_size:
            yield from self._flush(items)
        elif self.timer.value() is None:
            t = ctx.timer_service().current_processing_time() + self.max_wait_ms
            ctx.timer_service().register_processing_time_timer(t)
            self.timer.update(t)

    def on_timer(self, timestamp: int, ctx):
        self.timer.clear()
        items = list(self.buffer.get())
        if items:
            yield from self._flush(items)

    def _flush(self, items: list[dict]) -> Iterable:
        self.buffer.clear()
        t0 = time.perf_counter()
        texts = [f"{a['title']}. {a['abstract']}".strip() for a in items]
        vectors = clients.http_json("POST", self.embed_url, {"texts": texts})["vectors"]
        points = [{"id": point_id(a["article_id"]), "vector": v,
                   "payload": {"article_id": a["article_id"], "category": a["category"],
                               "subcategory": a["subcategory"], "title": a["title"],
                               "publish_ts": a["publish_ts"], "emitted_at": a["emitted_at"]}}
                  for a, v in zip(items, vectors)]
        # One upsert per micro-batch: fewer, larger HNSW insertions (report Section 4.3).
        clients.http_json("PUT", self.upsert_url, {"points": points})
        embedded_at = now_ms()
        self.last_batch_ms = (time.perf_counter() - t0) * 1000
        self.m_batches.inc()
        self.m_articles.inc(len(items))
        for a, v in zip(items, vectors):
            out = {k: a[k] for k in ("article_id", "category", "subcategory", "title", "url", "publish_ts", "emitted_at")}
            out["embedded_at"] = embedded_at
            out["vector"] = v
            yield out
            yield EMBEDDED_JSON_TAG, json.dumps(out, separators=(",", ":"))


# --------------------------------------------------------------------------------------------
# Embedding cache: bounded LRU in keyed state, joined with clicks
# --------------------------------------------------------------------------------------------
class EmbeddingCacheJoin(KeyedCoProcessFunction):
    """Keyed by hash(article_id) % B. Input 1 = freshly embedded articles (cache fill),
    input 2 = clicks (cache lookup). Output = click + embedding for the session stage.

    Each bucket holds at most `capacity` embeddings (LRU, total bound B x capacity) with a
    medium TTL refreshed on read. A miss (evicted/expired) falls back to Qdrant, the durable
    source of truth (report Section 4.2, "tiered caching"). A click that arrives before its
    article has been embedded at all waits in `pending` until the embedding arrives.
    """

    def __init__(self, cfg: dict, speedup: float, eviction: bool) -> None:
        q = cfg["qdrant"]
        self.fetch_url = f"{q['url']}/collections/{q['collection']}/points"
        self.capacity = cfg["flink"]["cache_capacity_per_bucket"]
        self.ttl_min = cfg["eviction"]["ttl_embedding_cache_min"]
        self.speedup = speedup
        self.eviction = eviction

    def open(self, rc: RuntimeContext) -> None:
        ttl = ttl_config(self.ttl_min, self.speedup, self.eviction, refresh_on_read=True)
        self.vectors = rc.get_map_state(with_ttl(MapStateDescriptor("vectors", Types.STRING(), PICKLED), ttl))
        self.access = rc.get_map_state(with_ttl(MapStateDescriptor("access", Types.STRING(), Types.LONG()), ttl))
        self.size = rc.get_state(ValueStateDescriptor("size", Types.INT()))
        self.pending = rc.get_map_state(with_ttl(MapStateDescriptor("pending", Types.STRING(), PICKLED),
                                                 ttl_config(self.ttl_min, self.speedup, self.eviction)))
        mg = rc.get_metrics_group()
        self.m_hits = mg.counter("cache_hits")
        self.m_misses = mg.counter("cache_misses")
        self.m_evictions = mg.counter("cache_evictions")
        self.m_pending = mg.counter("clicks_waiting_for_embedding")

    def _put(self, aid: str, vector: Any) -> np.ndarray:
        vec = np.asarray(vector, dtype=np.float32)
        is_new = not self.access.contains(aid)
        self.vectors.put(aid, vec)
        self.access.put(aid, now_ms())
        size = (self.size.value() or 0) + is_new
        if self.eviction and size > self.capacity:
            # Amortised LRU: only scan when over capacity, then evict down to 90% so the next
            # scans are rare. TTL-expired entries are invisible to the scan, which also resyncs `size`.
            entries = dict(self.access.items())
            victims = lru_victims(entries, int(self.capacity * 0.9))
            for victim in victims:
                self.vectors.remove(victim)
                self.access.remove(victim)
            self.m_evictions.inc(len(victims))
            size = len(entries) - len(victims)
        self.size.update(size)
        return vec

    @staticmethod
    def _joined(click: dict, vec: np.ndarray) -> dict:
        return {"user_id": click["user_id"], "article_id": click["article_id"], "ts": click["ts"], "vector": vec}

    def process_element1(self, article: dict, ctx):
        aid = article["article_id"]
        vec = self._put(aid, article["vector"])
        if self.pending.contains(aid):
            for click in self.pending.get(aid):
                yield self._joined(click, vec)
            self.pending.remove(aid)

    def process_element2(self, click: dict, ctx):
        aid = click["article_id"]
        if self.vectors.contains(aid):
            vec = self.vectors.get(aid)
            self.access.put(aid, now_ms())
            self.m_hits.inc()
            yield self._joined(click, vec)
            return
        self.m_misses.inc()
        res = clients.http_json("POST", self.fetch_url, {"ids": [point_id(aid)], "with_vector": True,
                                                          "with_payload": False})
        points = res.get("result") or []
        if points:  # evicted or expired from the cache, but durable in Qdrant
            yield self._joined(click, self._put(aid, points[0]["vector"]))
            return
        # Not embedded yet (the click beat the micro-batch): hold it until the embedding arrives.
        waiting = self.pending.get(aid) if self.pending.contains(aid) else []
        self.pending.put(aid, waiting + [click])
        self.m_pending.inc()


# --------------------------------------------------------------------------------------------
# Stage 2: event-time session windows -> session interest vector
# --------------------------------------------------------------------------------------------
class SessionVectorAgg(AggregateFunction):
    def __init__(self, half_life_s: float) -> None:
        self.half_life_s = half_life_s

    def create_accumulator(self) -> sess.SessionAcc:
        return sess.SessionAcc()

    def add(self, value: dict, acc: sess.SessionAcc) -> sess.SessionAcc:
        return sess.add(acc, value["vector"], value["ts"], value["article_id"], self.half_life_s)

    def get_result(self, acc: sess.SessionAcc) -> sess.SessionAcc:
        return acc

    def merge(self, a: sess.SessionAcc, b: sess.SessionAcc) -> sess.SessionAcc:
        return sess.merge(a, b, self.half_life_s)


class SessionWriter(ProcessWindowFunction):
    """Fires on every click (CountTrigger(1)) so the session vector is live, and writes it to
    ScyllaDB for the serving API. Window state itself is purged by Flink once the session gap
    passes; the per-user cross-session `recent_clicked` list lives in global keyed state with the
    short session TTL."""

    def __init__(self, cfg: dict, speedup: float, eviction: bool) -> None:
        self.hosts = cfg["scylla"]["hosts"]
        self.ttl_min = cfg["eviction"]["ttl_session_min"]
        self.speedup = speedup
        self.eviction = eviction

    def open(self, rc: RuntimeContext) -> None:
        self.recent_desc = with_ttl(ValueStateDescriptor("recent_clicked", PICKLED),
                                    ttl_config(self.ttl_min, self.speedup, self.eviction))
        self.db = clients.scylla_session(self.hosts)
        self.insert = self.db.prepare(
            "INSERT INTO newsrec.session_vectors (user_id, vector, clicks, clicked, session_end, updated_at) "
            "VALUES (?, ?, ?, ?, ?, ?)")
        self.m_updates = rc.get_metrics_group().counter("session_updates")

    def process(self, key: str, context: ProcessWindowFunction.Context, elements: Iterable[sess.SessionAcc]):
        acc = next(iter(elements))
        vec = sess.result(acc)
        if vec is None:
            return []
        recent_state = context.global_state().get_state(self.recent_desc)
        recent = list(dict.fromkeys((recent_state.value() or []) + acc.clicked))[-sess.MAX_CLICKED:]
        recent_state.update(recent)
        self.db.execute_async(self.insert, (key, [float(x) for x in vec], acc.clicks, recent,
                                            to_dt(acc.last_ts), to_dt(now_ms())))
        self.m_updates.inc()
        return []


# --------------------------------------------------------------------------------------------
# Stage 3: per-topic decay + drift (River ADWIN) -> ScyllaDB roll-ups + drift events
# --------------------------------------------------------------------------------------------
class TopicDecayDrift(KeyedCoProcessFunction):
    """Keyed by category. Input 1 = articles (learn publish time), input 2 = impressions/clicks.

    Per event: update the article's counters, the topic's EWMA engagement and its ADWIN detector.
    On drift: adapt the topic half-life (newsrec_core.drift) and emit a drift event.
    Every `rollup_interval_min` of event time: merge the accumulated counters into Flink state,
    write topic + item scores to ScyllaDB and relax the half-life toward the default. Flink keeps
    only live counters (long TTL); ScyllaDB keeps the history.

    Throughput design: in PyFlink's process mode every keyed-state access is a gRPC round trip
    to the JVM, which capped this operator at ~50-115 events/s. Per-event updates therefore go to
    a per-category accumulator in worker memory and are written to keyed state once per roll-up.
    Trade-off: after a failure, increments since the last roll-up (<= 10 event-minutes) are lost;
    everything already rolled up is in Flink state and ScyllaDB. ADWIN detectors also live in
    memory and simply re-warm after a restart.
    """

    WARMUP_EVENTS = 100  # ignore ADWIN detections while the first window is still forming

    def __init__(self, cfg: dict, speedup: float, eviction: bool) -> None:
        d = cfg["decay"]
        h = 3600.0
        self.policy = DecayPolicy(d["default_half_life_h"] * h, d["min_half_life_h"] * h, d["max_half_life_h"] * h,
                                  d["spike_factor"], d["slow_burn_factor"], d["relax_rate"])
        self.alpha = d["ewma_alpha"]
        self.delta = d["adwin_delta"]
        self.prior_ctr, self.prior_weight = d["prior_ctr"], d["prior_weight"]
        self.rollup_ms = cfg["flink"]["rollup_interval_min"] * 60_000
        self.hosts = cfg["scylla"]["hosts"]
        self.ttl_min = cfg["eviction"]["ttl_topic_decay_min"]
        self.speedup = speedup
        self.eviction = eviction

    def open(self, rc: RuntimeContext) -> None:
        ttl = ttl_config(self.ttl_min, self.speedup, self.eviction)
        self.topic = rc.get_state(with_ttl(ValueStateDescriptor("topic", PICKLED), ttl))
        self.items = rc.get_map_state(with_ttl(MapStateDescriptor("items", Types.STRING(), PICKLED), ttl))
        self.mem: dict[str, dict] = {}  # category -> {"topic": dict, "deltas": {aid: [clicks, impressions]}, "armed": int}
        self.db = clients.scylla_session(self.hosts)
        self.put_topic = self.db.prepare(
            "INSERT INTO newsrec.topic_decay (category, half_life_s, ewma_ctr, drift_count, event_time, updated_at) "
            "VALUES (?, ?, ?, ?, ?, ?)")
        self.put_history = self.db.prepare(
            "INSERT INTO newsrec.topic_decay_history (category, event_time, half_life_s, ewma_ctr, drift) "
            "VALUES (?, ?, ?, ?, ?)")
        self.put_item = self.db.prepare(
            "INSERT INTO newsrec.item_scores (article_id, category, base_relevance, clicks, impressions, publish_ts, updated_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)")
        self.m_drifts = rc.get_metrics_group().counter("drift_detections")
        from river import drift  # heavy import, only in this operator's worker
        self._new_detector = lambda: drift.ADWIN(delta=self.delta)
        self.detectors: dict[str, Any] = {}

    def _mem(self, category: str) -> dict:
        """Per-category accumulator; seeded from keyed state once per worker lifetime."""
        m = self.mem.get(category)
        if m is None:
            t = self.topic.value() or {"ewma": None, "n": 0, "half_life_s": self.policy.default_s, "drift_count": 0,
                                       "drift_since_rollup": False, "dirty": False}
            m = self.mem[category] = {"topic": t, "deltas": {}, "armed": 0}
        return m

    def _detector(self, category: str):
        det = self.detectors.get(category)
        if det is None:
            det = self.detectors[category] = self._new_detector()
        return det

    def _arm_timer(self, ctx, m: dict, ts: int) -> None:
        nxt = (ts // self.rollup_ms + 1) * self.rollup_ms
        if m["armed"] < nxt:  # registering the same timestamp twice is a no-op in Flink anyway
            ctx.timer_service().register_event_time_timer(nxt)
            m["armed"] = nxt

    def process_element1(self, article: dict, ctx):
        aid = article["article_id"]
        if not self.items.contains(aid):
            self.items.put(aid, {"clicks": 0, "impressions": 0, "publish_ts": article["publish_ts"], "dirty": True})
        m = self._mem(ctx.get_current_key())
        m["topic"]["dirty"] = True
        self._arm_timer(ctx, m, ctx.timestamp())
        return []

    def process_element2(self, event: dict, ctx):
        aid, clicked = event["article_id"], event["type"] == "click"
        m = self._mem(ctx.get_current_key())
        delta = m["deltas"].setdefault(aid, [0, 0])
        delta[0 if clicked else 1] += 1

        t = m["topic"]
        adwin = self._detector(ctx.get_current_key())
        x = 1.0 if clicked else 0.0
        # Direction of a drift = ADWIN's window mean just before the cut (old regime) vs right after
        # it (new regime). A slow EWMA is a poor reference: a strong burst drags it up before ADWIN
        # has enough evidence to fire.
        baseline = adwin.estimation
        adwin.update(x)
        if adwin.drift_detected and t["n"] >= self.WARMUP_EVENTS:
            current = adwin.estimation
            old_hl = t["half_life_s"]
            t["half_life_s"], direction = respond_to_drift(old_hl, baseline, current, self.policy)
            t["drift_count"] += 1
            t["drift_since_rollup"] = True
            self.m_drifts.inc()
            yield DRIFT_JSON_TAG, json.dumps({
                "category": ctx.get_current_key(), "direction": direction,
                "event_ts": ctx.timestamp(), "wall_ts": now_ms(),
                "old_half_life_s": old_hl, "new_half_life_s": t["half_life_s"],
                "baseline_ctr": ctr_from_indicator_mean(baseline), "current_ctr": ctr_from_indicator_mean(current)})
        t["ewma"] = x if t["ewma"] is None else (1 - self.alpha) * t["ewma"] + self.alpha * x
        t["n"] += 1
        t["dirty"] = True
        self._arm_timer(ctx, m, ctx.timestamp())

    def on_timer(self, timestamp: int, ctx):
        category = ctx.get_current_key()
        m = self._mem(category)
        m["armed"] = 0
        t = m["topic"]
        if not t["dirty"] and not m["deltas"]:
            return []  # idle topic: stay disarmed until the next element
        if not t["drift_since_rollup"]:
            t["half_life_s"] = relax(t["half_life_s"], self.policy)
        ctr = ctr_from_indicator_mean(t["ewma"] or 0.0)
        ev, wall = to_dt(timestamp), to_dt(now_ms())
        self.db.execute_async(self.put_topic, (category, t["half_life_s"], ctr, t["drift_count"], ev, wall))
        self.db.execute_async(self.put_history, (category, ev, t["half_life_s"], ctr, t["drift_since_rollup"]))

        # Merge accumulated counters into keyed state (one read + one write per touched article).
        touched = set(m["deltas"])
        for aid, (dc, di) in m["deltas"].items():
            item = self.items.get(aid) if self.items.contains(aid) else {"clicks": 0, "impressions": 0, "publish_ts": None}
            item["clicks"] += dc
            item["impressions"] += di
            item["dirty"] = True
            self.items.put(aid, item)
        m["deltas"] = {}
        for aid, item in list(self.items.items()):
            if item.get("dirty") or aid in touched:
                base = smoothed_ctr(item["clicks"], item["impressions"], self.prior_ctr, self.prior_weight)
                self.db.execute_async(self.put_item, (aid, category, base, item["clicks"], item["impressions"],
                                                      to_dt(item["publish_ts"]), wall))
                item["dirty"] = False
                self.items.put(aid, item)
        t["dirty"] = False
        t["drift_since_rollup"] = False
        self.topic.update(t)
        self._arm_timer(ctx, m, timestamp)
        return []
