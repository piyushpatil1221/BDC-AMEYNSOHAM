"""One-shot bootstrap: Kafka topics, Qdrant collection, ScyllaDB schema. Idempotent; exits when done."""
import time
from typing import Callable, TypeVar

import yaml
from cassandra.cluster import Cluster
from confluent_kafka.admin import AdminClient, NewTopic
from qdrant_client import QdrantClient
from qdrant_client.models import Distance, HnswConfigDiff, OptimizersConfigDiff, PayloadSchemaType, VectorParams

T = TypeVar("T")
CFG = yaml.safe_load(open("/config/pipeline.yaml", encoding="utf-8"))
RETENTION_MS = str(60 * 60 * 1000)  # 1 h: Kafka is a buffer here, not a store


def retry(what: str, fn: Callable[[], T], attempts: int = 60, wait: float = 3) -> T:
    for i in range(attempts):
        try:
            return fn()
        except Exception as e:  # services may still be starting
            print(f"[init] waiting for {what} ({i + 1}/{attempts}): {type(e).__name__}", flush=True)
            time.sleep(wait)
    raise SystemExit(f"[init] {what} never became ready")


def kafka() -> None:
    admin = AdminClient({"bootstrap.servers": CFG["kafka"]["bootstrap"]})
    existing = retry("kafka", lambda: admin.list_topics(timeout=5)).topics
    new = [NewTopic(t, num_partitions=1, replication_factor=1, config={"retention.ms": RETENTION_MS})
           for t in CFG["kafka"]["topics"].values() if t not in existing]
    for topic, fut in (admin.create_topics(new).items() if new else []):
        fut.result()
        print(f"[init] topic {topic} created")
    print(f"[init] kafka topics: {sorted(CFG['kafka']['topics'].values())}")


def qdrant() -> None:
    q = CFG["qdrant"]
    client = QdrantClient(url=q["url"], timeout=10)
    retry("qdrant", client.get_collections)
    if not client.collection_exists(q["collection"]):
        client.create_collection(
            q["collection"],
            # on_disk vectors keep the 300 MB container budget; HNSW m=16 is the standard recall/size trade-off.
            vectors_config=VectorParams(size=q["dim"], distance=Distance.COSINE, on_disk=True),
            hnsw_config=HnswConfigDiff(m=q["hnsw_m"], ef_construct=q["hnsw_ef_construct"], on_disk=False),
            # Few segments: less background merging competing with searches on a small machine.
            optimizers_config=OptimizersConfigDiff(default_segment_number=2),
            on_disk_payload=True,
        )
        client.create_payload_index(q["collection"], "publish_ts", PayloadSchemaType.INTEGER)
        client.create_payload_index(q["collection"], "category", PayloadSchemaType.KEYWORD)
        print(f"[init] qdrant collection {q['collection']} created")
    print(f"[init] qdrant collections: {[c.name for c in client.get_collections().collections]}")


def scylla() -> None:
    session = retry("scylla", lambda: Cluster(CFG["scylla"]["hosts"]).connect(), attempts=100)
    with open("/schema/schema.cql", encoding="utf-8") as f:
        # Drop comment lines first: comments may contain ';', which would split a statement.
        cql = "\n".join(l for l in f if not l.strip().startswith("--"))
    for stmt in (s.strip() for s in cql.split(";")):
        if stmt:
            session.execute(stmt)
    tables = session.execute("SELECT table_name FROM system_schema.tables WHERE keyspace_name=%s",
                             [CFG["scylla"]["keyspace"]])
    print(f"[init] scylla tables: {sorted(r.table_name for r in tables)}")


if __name__ == "__main__":
    kafka()
    qdrant()
    scylla()
    print("[init] done")
