"""Serving API: GET /recommend/{user_id}?k=10 under a 50 ms budget (report Sections 5.1, 5.2).

1. Read the user's latest session vector from ScyllaDB (written by Flink's session windows).
2. ANN search in Qdrant over fresh articles, with whatever budget is left. If Qdrant would
   overrun, fall back to brute-force similarity over the in-memory recent-articles cache and
   count the fallback (explicit, monitored degradation).
3. Blend similarity with decayed warm scores (newsrec_core.blend) and drop already-clicked items.
"""
from __future__ import annotations

import asyncio
import logging
import os
import time
from collections import deque
from contextlib import asynccontextmanager
from typing import Any

import httpx
import numpy as np
import yaml
from cassandra.cluster import Cluster
from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import Response
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from app import metrics as m
from app.state import LiveState, start_kafka_consumer, start_score_refresher
from newsrec_core.blend import blend

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger("serving")

CFG = yaml.safe_load(open(os.environ.get("PIPELINE_CONFIG", "/config/pipeline.yaml"), encoding="utf-8"))
S = CFG["serving"]
BUDGET_S = S["budget_ms"] / 1000
QDRANT_SEARCH = f"{CFG['qdrant']['url']}/collections/{CFG['qdrant']['collection']}/points/search"

state = LiveState(CFG)
runtime: dict[str, Any] = {"qdrant_delay_s": 0.0}
latencies: deque[tuple[float, str]] = deque(maxlen=5000)  # (seconds, path) for /stats percentiles
ages: deque[float] = deque(maxlen=5000)


@asynccontextmanager
async def lifespan(_: FastAPI):
    for attempt in range(60):
        try:
            db = Cluster(CFG["scylla"]["hosts"], protocol_version=4).connect()
            break
        except Exception as e:
            log.warning("ScyllaDB not ready: %s", e)
            await asyncio.sleep(2)
    else:
        raise RuntimeError("ScyllaDB unavailable")
    runtime["db"] = db
    runtime["get_session"] = db.prepare(
        "SELECT vector, clicks, clicked, session_end FROM newsrec.session_vectors WHERE user_id = ?")
    runtime["http"] = httpx.AsyncClient(limits=httpx.Limits(max_keepalive_connections=20))
    try:  # open the keep-alive connection now rather than on the first user's request
        await qdrant_ann(np.zeros(CFG["qdrant"]["dim"], dtype=np.float32), 0)
    except Exception:
        log.warning("Qdrant warm-up failed (collection may be empty)")
    start_kafka_consumer(state, CFG)
    # Separate cluster connection for the bulk score refresh: its large result sets are decoded on
    # their own driver I/O thread instead of delaying the per-request session lookups.
    start_score_refresher(state, Cluster(CFG["scylla"]["hosts"], protocol_version=4).connect(), CFG)
    yield
    await runtime["http"].aclose()


app = FastAPI(title="newsrec-serving", lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])


async def scylla_get(stmt, params) -> list:
    """Bridge the driver's callback future to asyncio without blocking the event loop."""
    loop = asyncio.get_running_loop()
    fut = loop.create_future()
    rf = runtime["db"].execute_async(stmt, params)
    rf.add_callbacks(lambda rows: loop.call_soon_threadsafe(fut.set_result, list(rows)),
                     lambda exc: loop.call_soon_threadsafe(fut.set_exception, exc))
    return await fut


async def qdrant_ann(query: np.ndarray, min_publish_ts: int) -> dict[str, float]:
    if runtime["qdrant_delay_s"]:
        await asyncio.sleep(runtime["qdrant_delay_s"])  # fault injection for the fallback demo
    r = await runtime["http"].post(QDRANT_SEARCH, json={
        "vector": query.tolist(), "limit": S["ann_limit"], "with_payload": ["article_id"],
        "filter": {"must": [{"key": "publish_ts", "range": {"gte": min_publish_ts}}]},
        "params": {"hnsw_ef": 64}})
    r.raise_for_status()
    return {p["payload"]["article_id"]: p["score"] for p in r.json()["result"]}


def local_ann(query: np.ndarray, min_publish_ts: int) -> dict[str, float]:
    ids, mat = state.recent_matrix(min_publish_ts)
    if mat is None:
        return {}
    sims = mat @ query
    top = np.argsort(-sims)[:S["ann_limit"]]
    return {ids[i]: float(sims[i]) for i in top}


@app.get("/recommend/{user_id}")
async def recommend(user_id: str, k: int = Query(10, ge=1, le=50)) -> dict:
    t0 = time.perf_counter()
    timings: dict[str, float] = {}
    try:
        rows = await asyncio.wait_for(scylla_get(runtime["get_session"], (user_id,)), timeout=0.02)
    except Exception:
        rows = []
    timings["session_ms"] = (time.perf_counter() - t0) * 1000
    row = rows[0] if rows else None
    query = np.asarray(row.vector, dtype=np.float32) if row is not None and row.vector else None
    exclude = set(row.clicked or []) if row is not None else set()

    clock = state.clock_ms
    min_ts = clock - int(S["fresh_window_h"] * 3600 * 1000)
    ann: dict[str, float] = {}
    path = "popular"
    if query is not None:
        t_ann = time.perf_counter()
        remaining = BUDGET_S - (t_ann - t0) - 0.015  # reserve 15 ms for the fallback + blending
        try:
            ann = await asyncio.wait_for(qdrant_ann(query, min_ts), timeout=max(0.001, remaining))
            path = "ann"
        except Exception:
            ann = local_ann(query, min_ts)
            path = "fallback"
            m.FALLBACKS.inc()
        timings["ann_ms"] = (time.perf_counter() - t_ann) * 1000

    t_blend = time.perf_counter()
    warm = dict(state.warm_top)
    extra = {}
    if query is not None:
        with state.lock:
            for aid in warm:
                e = state.recent.get(aid)
                if e is not None:
                    extra[aid] = float(e["vector"] @ query)
    ranked = blend(ann, warm, alpha=S["alpha_session"] if query is not None else 0.0, k=k,
                   extra_similarity=extra, exclude=exclude)

    items = []
    req_ages = []
    for s in ranked:
        meta = state.article_meta(s.article_id)
        age_h = (clock - meta["publish_ts"]) / 3_600_000 if meta["publish_ts"] else None
        if age_h is not None:
            req_ages.append(age_h)
            m.REC_AGE.observe(age_h)
        items.append({"article_id": s.article_id, "title": meta["title"], "category": meta["category"],
                      "score": round(s.score, 4), "similarity": round(s.similarity, 4), "warm": round(s.warm, 4),
                      "age_h": None if age_h is None else round(age_h, 2), "clicks": meta["clicks"]})
    if req_ages:
        med = float(np.median(req_ages))
        m.FRESHNESS.set(med)
        ages.append(med)

    elapsed = time.perf_counter() - t0
    timings["blend_ms"] = (time.perf_counter() - t_blend) * 1000
    m.REC_LATENCY.labels(path).observe(elapsed)
    m.REC_REQUESTS.labels(path).inc()
    latencies.append((elapsed, path))
    return {"user_id": user_id, "path": path, "latency_ms": round(elapsed * 1000, 2),
            "timings": {k: round(v, 2) for k, v in timings.items()},
            "session": None if row is None else {"clicks": row.clicks, "clicked": list(row.clicked or [])[-10:],
                                                 "session_end": row.session_end.isoformat() if row.session_end else None},
            "event_clock": clock, "items": items}


@app.get("/stats")
def stats() -> dict:
    lat = np.array([l for l, _ in latencies]) * 1000 if latencies else np.array([])
    paths = [p for _, p in latencies]
    cs = state.coldstart
    delays = np.array(cs["delays_s"]) if cs["delays_s"] else np.array([])
    pct = (lambda a, q: round(float(np.percentile(a, q)), 2) if a.size else None)
    return {
        "latency_ms": {"p50": pct(lat, 50), "p95": pct(lat, 95), "p99": pct(lat, 99), "budget": S["budget_ms"],
                       "requests": len(paths)},
        "fallback_rate": round(paths.count("fallback") / len(paths), 4) if paths else 0.0,
        "paths": {p: paths.count(p) for p in set(paths)},
        "coldstart": {"articles": cs["total"], "within_target": cs["within_target"],
                      "coverage": round(cs["within_target"] / cs["total"], 4) if cs["total"] else None,
                      "target_s": S["coldstart_target_s"], "p50_s": pct(delays, 50), "p95_s": pct(delays, 95)},
        "freshness_median_age_h": round(float(np.median(ages)), 2) if ages else None,
        "drift_lags": list(state.drift_lags.values()),
        "drift_events": len(state.drifts),
        "event_clock": state.clock_ms,
        "recent_cache": len(state.recent),
        "warm_items": len(state.items),
        "qdrant_delay_ms": runtime["qdrant_delay_s"] * 1000,
    }


@app.get("/articles/recent")
def recent_articles(limit: int = Query(50, ge=1, le=300)) -> list[dict]:
    with state.lock:
        feed = list(state.feed.values())[-limit:][::-1]
        items = state.items
    out = []
    for e in feed:
        clicks = items.get(e["article_id"], {}).get("clicks", 0)
        status = "cold" if e["embedded_at"] is None else ("warm" if clicks > 0 else "fresh")
        delay = None if e["embedded_at"] is None else round((e["embedded_at"] - e["emitted_at"]) / 1000, 2)
        out.append({**e, "status": status, "clicks": clicks, "coldstart_delay_s": delay})
    return out


@app.get("/users/active")
async def active_users(limit: int = Query(30, ge=1, le=200)) -> list[dict]:
    rows = await scylla_get("SELECT user_id, clicks, session_end FROM newsrec.session_vectors LIMIT 1000", ())
    rows.sort(key=lambda r: r.session_end.timestamp() if r.session_end else 0.0, reverse=True)
    return [{"user_id": r.user_id, "clicks": r.clicks, "session_end": r.session_end.isoformat() if r.session_end else None}
            for r in rows[:limit]]


@app.get("/topics")
async def topics(history: int = Query(200, ge=1, le=1000)) -> list[dict]:
    with state.lock:
        current = list(state.topics)
    out = []
    for t in sorted(current, key=lambda t: t["category"]):
        rows = await scylla_get("SELECT event_time, half_life_s, ewma_ctr, drift FROM newsrec.topic_decay_history "
                                "WHERE category = %s LIMIT %s", (t["category"], history))
        out.append({**t, "history": [{"event_time": int(r.event_time.timestamp() * 1000), "half_life_h": r.half_life_s / 3600,
                                      "ctr": r.ewma_ctr, "drift": r.drift} for r in reversed(rows)]})
    return out


@app.get("/drift")
def drift() -> dict:
    with state.lock:
        return {"events": list(state.drifts)[-100:], "bursts": list(state.bursts), "lags": list(state.drift_lags.values())}


@app.post("/admin/qdrant-delay")
def set_qdrant_delay(ms: float = Query(0, ge=0, le=5000)) -> dict:
    """Test hook: add artificial latency to every Qdrant call to exercise the fallback path."""
    runtime["qdrant_delay_s"] = ms / 1000
    return {"qdrant_delay_ms": ms}


@app.get("/health")
def health() -> dict:
    return {"status": "ok", "event_clock": state.clock_ms}


@app.get("/metrics")
def metrics() -> Response:
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)
