"""In-process state the serving API keeps warm, fed by Kafka and ScyllaDB in background threads.

* RecentArticles: bounded LRU of the latest embedded articles (vectors included). This is the
  fallback candidate set when Qdrant would blow the latency budget (report Section 5.2).
* Feed: the last few hundred articles with their cold-start timestamps, for the dashboard.
* WarmScores: item scores and topic half-lives copied from ScyllaDB every couple of seconds, so
  decayed popularity is computed locally instead of scanning ScyllaDB on every request.
* Event clock: the replay's current event time (max publish_ts seen), used to compute ages.
"""
from __future__ import annotations

import json
import logging
import threading
import time
import uuid
from collections import OrderedDict, deque
from typing import Any

import numpy as np
from confluent_kafka import Consumer

from app import metrics as m
from newsrec_core.decay import decayed_score
from newsrec_core.lru import LRUCache

log = logging.getLogger("serving")
FEED_SIZE = 300


class LiveState:
    def __init__(self, cfg: dict) -> None:
        self.cfg = cfg
        self.lock = threading.Lock()
        self.recent: LRUCache[str, dict] = LRUCache(cfg["serving"]["recent_cache_size"])
        self._matrix_dirty = True
        self._matrix_built = 0.0
        self.feed: OrderedDict[str, dict] = OrderedDict()
        self.clock_ms = 0
        self.drifts: deque[dict] = deque(maxlen=500)
        self.bursts: deque[dict] = deque(maxlen=50)
        self.drift_lags: dict[str, dict] = {}
        self.coldstart = {"total": 0, "within_target": 0, "delays_s": deque(maxlen=5000)}
        # warm scores (replaced wholesale by the refresher thread)
        self.items: dict[str, dict] = {}
        self.half_lives: dict[str, float] = {}
        self.topics: list[dict] = []
        self.warm_top: dict[str, float] = {}

    # ---- Kafka-fed updates ----------------------------------------------------------------
    def _feed_entry(self, d: dict) -> dict:
        e = self.feed.get(d["article_id"])
        if e is None:
            e = {k: d.get(k) for k in ("article_id", "title", "category", "subcategory", "publish_ts", "emitted_at")}
            e["embedded_at"] = None
            self.feed[d["article_id"]] = e
            while len(self.feed) > FEED_SIZE:
                self.feed.popitem(last=False)
        return e

    def on_article_clean(self, d: dict) -> None:
        with self.lock:
            self._feed_entry(d)
            self.clock_ms = max(self.clock_ms, d["publish_ts"])

    def on_article_embedded(self, d: dict) -> None:
        vec = np.asarray(d["vector"], dtype=np.float32)
        with self.lock:
            self.recent.put(d["article_id"], {"vector": vec, "publish_ts": d["publish_ts"], "category": d["category"],
                                              "title": d["title"]})
            self._matrix_dirty = True
            self._feed_entry(d)["embedded_at"] = d["embedded_at"]
            self.clock_ms = max(self.clock_ms, d["publish_ts"])
            delay_s = max(0.0, (d["embedded_at"] - d["emitted_at"]) / 1000)
            self.coldstart["total"] += 1
            self.coldstart["delays_s"].append(delay_s)
            ok = delay_s <= self.cfg["serving"]["coldstart_target_s"]
            self.coldstart["within_target"] += ok
        m.COLD_DELAY.observe(delay_s)
        m.COLD_TOTAL.inc()
        if ok:
            m.COLD_OK.inc()

    def on_burst(self, d: dict) -> None:
        with self.lock:
            self.bursts.append(d)

    def on_drift(self, d: dict) -> None:
        m.DRIFT_EVENTS.labels(d["category"], d["direction"]).inc()
        with self.lock:
            self.drifts.append(d)
            for b in self.bursts:
                key = str(b["burst_id"])
                if (key not in self.drift_lags and d["direction"] == "spike" and d["category"] == b["category"]
                        and d["event_ts"] >= b["start_event_ts"]):
                    lag = {"burst_id": b["burst_id"], "category": b["category"],
                           "event_lag_s": (d["event_ts"] - b["start_event_ts"]) / 1000,
                           "wall_lag_s": (d["wall_ts"] - b["start_wall_ts"]) / 1000,
                           "old_half_life_h": d["old_half_life_s"] / 3600, "new_half_life_h": d["new_half_life_s"] / 3600}
                    self.drift_lags[key] = lag
                    m.DRIFT_LAG.labels("event").set(lag["event_lag_s"])
                    m.DRIFT_LAG.labels("wall").set(lag["wall_lag_s"])

    # ---- ScyllaDB-fed warm scores ---------------------------------------------------------
    def set_scores(self, items: dict[str, dict], topics: list[dict]) -> None:
        default_hl = self.cfg["decay"]["default_half_life_h"] * 3600
        half_lives = {t["category"]: t["half_life_s"] for t in topics}
        clock = self.clock_ms
        scored = {}
        for aid, it in items.items():
            if it["publish_ts"] is None:
                continue
            age_s = (clock - it["publish_ts"]) / 1000
            scored[aid] = decayed_score(it["base_relevance"], age_s, half_lives.get(it["category"], default_hl))
        top = dict(sorted(scored.items(), key=lambda kv: -kv[1])[:150])
        for t in topics:
            m.HALF_LIFE.labels(t["category"]).set(t["half_life_s"] / 3600)
        with self.lock:
            self.items, self.half_lives, self.topics, self.warm_top = items, half_lives, topics, top

    # ---- read helpers ---------------------------------------------------------------------
    def recent_matrix(self, min_publish_ts: int) -> tuple[list[str], np.ndarray | None]:
        """Stacked vectors of the recent-articles cache, rebuilt at most once a second.

        The fallback runs exactly when the budget is already tight, so it must not pay for
        np.stack over 2000 vectors on every request.
        """
        now = time.monotonic()
        cached = getattr(self, "_matrix", None)
        if cached is None or (self._matrix_dirty and now - self._matrix_built > 1.0):
            with self.lock:
                rows = [(aid, e["vector"], e["publish_ts"]) for aid, e in self.recent.items()]
                self._matrix_dirty = False
            ids = [r[0] for r in rows]
            mat = np.stack([r[1] for r in rows]) if rows else None
            pub = np.array([r[2] for r in rows], dtype=np.int64)
            cached = self._matrix = (ids, mat, pub)
            self._matrix_built = now
        ids, mat, pub = cached
        if mat is None:
            return [], None
        keep = np.nonzero(pub >= min_publish_ts)[0]
        return [ids[i] for i in keep], mat[keep]

    def article_meta(self, aid: str) -> dict:
        with self.lock:
            e = self.recent.get(aid) or {}
            f = self.feed.get(aid) or {}
            it = self.items.get(aid) or {}
        return {"title": e.get("title") or f.get("title"), "category": e.get("category") or f.get("category") or it.get("category"),
                "publish_ts": e.get("publish_ts") or f.get("publish_ts") or it.get("publish_ts"),
                "clicks": it.get("clicks", 0)}


def start_kafka_consumer(state: LiveState, cfg: dict) -> threading.Thread:
    t = cfg["kafka"]["topics"]
    handlers = {t["articles_clean"]: state.on_article_clean, t["articles_embedded"]: state.on_article_embedded,
                t["drift_events"]: state.on_drift, t["bursts"]: state.on_burst}

    def run() -> None:
        while True:
            try:
                c = Consumer({"bootstrap.servers": cfg["kafka"]["bootstrap"], "group.id": f"serving-{uuid.uuid4()}",
                              "auto.offset.reset": "earliest", "enable.auto.commit": False})
                c.subscribe(list(handlers))
                while True:
                    msg = c.poll(0.5)
                    if msg is None or msg.error():
                        continue
                    try:
                        handlers[msg.topic()](json.loads(msg.value()))
                    except Exception:
                        log.exception("bad message on %s", msg.topic())
            except Exception:
                log.exception("kafka consumer crashed; restarting")
                time.sleep(2)

    th = threading.Thread(target=run, name="kafka-consumer", daemon=True)
    th.start()
    return th


def start_score_refresher(state: LiveState, db: Any, cfg: dict) -> threading.Thread:
    period = cfg["serving"]["score_refresh_s"]

    def run() -> None:
        while True:
            try:
                items = {r.article_id: {"category": r.category, "base_relevance": r.base_relevance, "clicks": r.clicks,
                                        "impressions": r.impressions,
                                        "publish_ts": int(r.publish_ts.timestamp() * 1000) if r.publish_ts else None}
                         for r in db.execute("SELECT article_id, category, base_relevance, clicks, impressions, publish_ts "
                                             "FROM newsrec.item_scores")}
                topics = [{"category": r.category, "half_life_s": r.half_life_s, "ewma_ctr": r.ewma_ctr,
                           "drift_count": r.drift_count,
                           "event_time": int(r.event_time.timestamp() * 1000) if r.event_time else None}
                          for r in db.execute("SELECT category, half_life_s, ewma_ctr, drift_count, event_time "
                                              "FROM newsrec.topic_decay")]
                state.set_scores(items, topics)
            except Exception:
                log.exception("score refresh failed")
            time.sleep(period)

    th = threading.Thread(target=run, name="score-refresher", daemon=True)
    th.start()
    return th
