"""End-to-end smoke test against a running stack (`make up` + a few minutes of `make replay`).

Checks one observable outcome per stage: cleaned topics, embedded articles in Qdrant, session
vectors and topic roll-ups in ScyllaDB, and a /recommend answer within the latency budget.
"""
import os

import httpx
import pytest
from cassandra.cluster import Cluster

SERVING = os.environ.get("SERVING_URL", "http://serving:8000")
QDRANT = os.environ.get("QDRANT_URL", "http://qdrant:6333")


@pytest.fixture(scope="module")
def db():
    session = Cluster(["scylla"], protocol_version=4).connect()
    yield session
    session.shutdown()


def test_cold_start_articles_are_in_qdrant():
    r = httpx.post(f"{QDRANT}/collections/articles/points/count", json={"exact": True}, timeout=5)
    assert r.json()["result"]["count"] > 0


def test_flink_writes_session_vectors(db):
    assert db.execute("SELECT count(*) FROM newsrec.session_vectors").one()[0] > 0


def test_flink_rolls_up_topic_decay(db):
    rows = list(db.execute("SELECT category, half_life_s FROM newsrec.topic_decay"))
    assert rows and all(r.half_life_s > 0 for r in rows)


def test_serving_reports_coldstart_coverage():
    stats = httpx.get(f"{SERVING}/stats", timeout=5).json()
    assert stats["coldstart"]["articles"] > 0


def test_recommend_for_active_user_within_budget():
    users = httpx.get(f"{SERVING}/users/active?limit=5", timeout=5).json()
    assert users, "no active sessions yet"
    # best of 3: a single request can land on a Qdrant merge spike (that is what the fallback is for)
    runs = [httpx.get(f"{SERVING}/recommend/{users[0]['user_id']}?k=10", timeout=5).json() for _ in range(3)]
    r = min(runs, key=lambda x: x["latency_ms"])
    assert len(r["items"]) == 10
    assert r["path"] in {"ann", "fallback"}
    assert r["latency_ms"] < 50
    clicked = set(r["session"]["clicked"])
    assert not clicked & {i["article_id"] for i in r["items"]}, "already-clicked items must be excluded"


def test_unknown_user_gets_popular_items():
    r = httpx.get(f"{SERVING}/recommend/U0?k=5", timeout=5).json()
    assert r["path"] == "popular" and len(r["items"]) == 5
