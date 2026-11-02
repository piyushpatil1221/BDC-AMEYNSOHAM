# Real-Time Session-Based News Recommender on Apache Flink

Big Data Computing case study (VESIT, AI & DS) by **Amey Vichave (51)** and **Soham Tejwani (61)**.
It implements, on one laptop, the design in [docs/case-study.pdf](docs/case-study.pdf):

- **CA1: news recommendation from streaming articles.** New articles are embedded the moment they're
  published, so they're recommendable before anyone has clicked them (item cold start).
- **CA2: optimising stateful stream processing and memory eviction in Apache Flink.** Keyed state, event-time
  session windows, three separate State TTLs, a bounded LRU cache in keyed state, and RocksDB incremental
  checkpoints, plus an ablation experiment showing what eviction buys.

For a full walkthrough written for a reader new to the project, see [docs/PROJECT_GUIDE.md](docs/PROJECT_GUIDE.md).

## Architecture

```mermaid
flowchart LR
  subgraph Replay["replay/ (MIND-small, event-time order, 60x)"]
    P[producer.py] --> AR[(articles.raw)]
    P --> ER[(events.raw)]
    B[burst.py] --> AR & ER
  end
  subgraph KS["kafka-streams/ (Java)"]
    V[validate JSON Schema<br/>dedup<br/>enrich category]
  end
  AR & ER --> V
  V --> AC[(articles.clean)] & EC[(events.clean)]
  V --> DLQ[(dlq)]
  subgraph Flink["flink-jobs/ (PyFlink, RocksDB, incremental checkpoints)"]
    I[parse + watermarks] --> EB[1 EmbedBatcher<br/>micro-batches]
    EB --> CJ[embedding cache<br/>bounded LRU + TTL]
    I -- clicks --> CJ
    CJ --> SW[2 session windows<br/>20 min gap]
    I --> TD[3 topic decay<br/>EWMA + River ADWIN]
  end
  AC & EC --> I
  EB <--> EMB[embedding-service<br/>INT8 MiniLM, ONNX Runtime]
  EB --> Q[(Qdrant HNSW)]
  EB --> AE[(articles.embedded)]
  SW --> S[(ScyllaDB)]
  TD --> S
  TD --> DE[(drift.events)]
  subgraph Serve["serving-api/ (FastAPI)"]
    R["GET /recommend/{user}<br/>50 ms budget"]
  end
  S & Q & AE & DE --> R
  R --> UI[dashboard/ Next.js]
  R & Flink --> PR[Prometheus + Grafana]
```

The flow of an article: it's published to Kafka, cleaned by Kafka Streams, micro-batched in Flink, embedded by
the INT8 encoder, inserted into Qdrant (now **retrievable**), and published on `articles.embedded`. A click on it
then flows through the embedding cache to the user's **session window**, which writes a fresh session vector to
ScyllaDB. The same events also feed the **topic decay** operator: per-category engagement, ADWIN drift detection,
an adapted half-life, and roll-ups to ScyllaDB. `/recommend` blends ANN similarity from Qdrant with decayed
engagement scores.

## Requirements

- Docker Desktop with WSL2, limited to 4.5 GB (`C:\Users\<you>\.wslconfig`: `[wsl2]` `memory=4608MB` `swap=4GB`).
- Node 18+ for the dashboard (it runs outside Docker).
- About 15 GB of free disk for images and data.
- `make`, or `mingw32-make` on Windows. Every target is a plain `docker compose` command, so you can copy them out of the [Makefile](Makefile).

## Run the demo

```bash
make data          # download MIND-small (commit-pinned HF mirror) + build the 2-day / 5,000-user subset
make up            # core stack + topics/collection/schema + submit the Flink job
make up-all        # same, plus Prometheus (localhost:9090) and Grafana (localhost:3001)
make replay        # replay the subset at 60x (2 days of news in ~48 min); Ctrl-C any time
make burst         # in a second terminal while the replay runs: "breaking news" flood in sports
make loadgen       # steady /recommend traffic so latency metrics have data
make smoke         # end-to-end smoke test
make test          # unit tests
```

Dashboard (production build, as recommended for demos):

```bash
cd dashboard && npm install && npm run build && npm start     # http://localhost:3000
```

| URL | What |
|---|---|
| http://localhost:3000 | Dashboard: live feed, user recommendations, topic drift, latency |
| http://localhost:8000/docs | Serving API (Swagger): `/recommend/{user_id}`, `/stats`, `/topics`, `/drift` |
| http://localhost:8081 | Flink web UI: job graph, checkpoints, backpressure |
| http://localhost:3001/d/newsrec | Grafana dashboard (monitoring profile) |
| http://localhost:6333/dashboard | Qdrant UI |

**Fallback demo.** Run `curl -X POST "localhost:8000/admin/qdrant-delay?ms=80"`. Requests switch to the `fallback`
path (in-memory recent-articles cache) and stay inside the budget. Set `ms=0` to restore.

**Burst demo.** Run `make burst` while the replay is going. Within a few event-minutes `drift.events` reports a
`spike` for `sports`, the half-life drops from 6 h to 3 h (Topics page), and the overview shows the detection lag.

## Memory budget (core profile, every container has `mem_limit`)

| Container | Limit | Notes |
|---|---|---|
| Kafka (KRaft) | 512 MB | `-Xmx384m`, 1 h retention |
| Kafka Streams | 256 MB | `-Xmx192m`, in-memory stores |
| Flink JobManager | 640 MB | |
| Flink TaskManager | 1400 MB | 2 slots, RocksDB with managed memory, PyFlink workers |
| Embedding service | 450 MB | 1 worker, `intra_op_num_threads=2` |
| Qdrant | 300 MB | vectors `on_disk`, HNSW m=16 |
| ScyllaDB | 700 MB | `--smp 1 --memory 500M` (600M does not fit a 700 MB cgroup) |
| Serving API | 200 MB | |
| **Total** | **≈ 4.46 GB** | measured usage ≈ 2.9–3.2 GB during a 60x replay (`make stats`) |

## Report section → code (viva map)

| Report section | What it says | Where it is implemented |
|---|---|---|
| 3.1, 4.3, 5.2 Cold start | Content-only embeddings for brand-new articles; quantized encoder; micro-batching | [embedding-service/scripts/quantize.py](embedding-service/scripts/quantize.py), [embedding-service/app/encoder.py](embedding-service/app/encoder.py), [embedding-service/app/main.py](embedding-service/app/main.py) (`MicroBatcher`), [flink-jobs/newsrec/operators.py](flink-jobs/newsrec/operators.py) (`EmbedBatcher`) |
| 4.3 Index churn | Batch inserts into HNSW | `EmbedBatcher._flush` (one upsert per micro-batch), [infra/init/init.py](infra/init/init.py) (HNSW m=16, on-disk vectors) |
| 4.1 Keyed state, session windows, watermarks | Per-user state; inactivity-gap sessions; out-of-order clicks | [flink-jobs/newsrec/job.py](flink-jobs/newsrec/job.py), `SessionVectorAgg` / `SessionWriter`, [newsrec_core/session.py](newsrec_core/session.py) |
| 4.2 Memory eviction | TTL with compaction cleanup, session purging, tiered LRU cache | `ttl_config`, `EmbeddingCacheJoin` ([operators.py](flink-jobs/newsrec/operators.py)), [newsrec_core/lru.py](newsrec_core/lru.py), `eviction:` in [config/pipeline.yaml](config/pipeline.yaml) |
| 3.2, 4.4, 5.3 Decay & drift | exp decay with per-topic half-life; ADWIN; spike/slow-burn response; roll-up to Cassandra | `TopicDecayDrift`, [newsrec_core/decay.py](newsrec_core/decay.py), [newsrec_core/drift.py](newsrec_core/drift.py), [infra/scylla/schema.cql](infra/scylla/schema.cql) |
| 5.1 Ingestion | Kafka + Kafka Streams validation, dedup, enrichment | [kafka-streams/](kafka-streams/src/main/java/newsrec/cleaner/), [replay/producer.py](replay/producer.py) |
| 5.1 Serving | Session vector × ANN + decayed warm scores | [serving-api/app/main.py](serving-api/app/main.py), [newsrec_core/blend.py](newsrec_core/blend.py) |
| 4.5 Latency budget | 50 ms with a monitored fallback | `recommend()` in [serving-api/app/main.py](serving-api/app/main.py) (per-stage `timings`, 15 ms reserve), [serving-api/app/state.py](serving-api/app/state.py) |
| 5.4 Incremental checkpointing | Short recovery, changed segments only | `EmbeddedRocksDBStateBackend(enable_incremental_checkpointing=True)` in [job.py](flink-jobs/newsrec/job.py), `FLINK_PROPERTIES` in [docker-compose.yml](docker-compose.yml) |
| 5.5 Evaluation metrics | Coverage, p95/p99, drift lag, state size, freshness | [serving-api/app/metrics.py](serving-api/app/metrics.py), [infra/grafana/build_dashboard.py](infra/grafana/build_dashboard.py), [eval/offline.py](eval/offline.py), [eval/ablation.py](eval/ablation.py) |
| 6 Evaluation harness | Offline replay through the same logic | [eval/offline.py](eval/offline.py) (chronological replay scoring) |

## Results

See [reports/](reports/). Numbers below come from runs on the 8 GB development laptop.

### Eviction ablation (CA2): `make ablation`

The same 24 h of event time is replayed at 120× from fresh state, once with TTL + LRU eviction and once without
([reports/ablation.md](reports/ablation.md), log in [reports/ablation.log](reports/ablation.log)).

| Run | Final keyed state | Peak keyed state | Mean / max checkpoint | Peak TM memory | Watchdog (90% of 1400 MiB) |
|---|---|---|---|---|---|
| TTL + LRU eviction **ON** | **1.47 MB** | 3.23 MB | 11.1 s / 36.8 s | 892 MiB | not reached |
| Eviction **OFF** | 6.38 MB | 6.40 MB | 8.2 s / 29.6 s | 888 MiB | not reached |

![State size](reports/ablation_state_size.png)

- **State size is the clear result.** Without eviction, keyed state grows steadily with the catalogue and the user
  base: about 0.27 MB per event-hour, ending **4.3× larger**. With eviction it rises until the medium TTL (6
  event-hours) starts expiring cache entries, peaks at about 9 h, then *falls* and stays bounded as TTL and LRU
  remove cold entries faster than new ones arrive.
- **Checkpoint duration did not improve at this scale.** At a few MB, a checkpoint's duration is dominated by
  barrier alignment under backpressure and PyFlink bundle flushes, not by bytes uploaded. The ON run is slightly
  slower on average, which is consistent with the extra delete and compaction work eviction causes. The size gap
  (and with it upload time and recovery time) widens linearly with days replayed.
- **TaskManager memory is the same in both runs**, at about 890 MiB. RocksDB keeps state off-heap within its fixed
  managed-memory budget, and the JVM plus PyFlink workers dominate the footprint. So the watchdog was never reached
  in 24 h. The OFF run's growth is unbounded, but on a 5,000-user subset it would take far longer than one day to
  matter. At production scale (millions of users and articles) the same slope is what exhausts memory and disk.
- Charts: [state size](reports/ablation_state_size.png), [checkpoint duration](reports/ablation_checkpoint_duration.png),
  [TM memory](reports/ablation_memory.png).

### Offline quality vs MIND labels: `make eval`

Day-2 impressions of the subset are scored in time order, using only information available before each impression
([reports/eval.md](reports/eval.md)):

| Model | AUC | MRR | nDCG@5 | nDCG@10 |
|---|---|---|---|---|
| Random | 0.503 | 0.256 | 0.267 | 0.321 |
| Popularity (clicks so far) | 0.621 | 0.348 | 0.366 | 0.413 |
| Decayed CTR (warm score) | 0.654 | 0.379 | 0.398 | 0.442 |
| Session blend (serving formula) | 0.655 | 0.379 | 0.399 | 0.442 |
| History blend (cross-session bound) | **0.705** | **0.403** | **0.427** | **0.477** |

On the 204 impressions (4.4%) where the user had a live session, the session blend beats decayed CTR: AUC 0.611 vs
0.591, nDCG@10 0.363 vs 0.353.

### Live pipeline (Section 5.5)

| Metric | Result |
|---|---|
| Cold-start coverage (retrievable ≤ 10 s) | 68% at 120× (median 8.6 s); recent articles 3–8 s at 60× once warmed up |
| Recommend latency | p50 ≈ 21 ms at 20 req/s; 87% ANN path, 12% fallback while Qdrant is under heavy writes |
| Drift-detection lag | sports burst detected as a **spike** after **188 s event time** (37 s wall); half-life 6 h → 3 h |
| Data quality | 6/6 invalid records → DLQ with reasons; 10/10 duplicates dropped |
| Embedding model | INT8 58.8 MB (FP32 90.4 MB), 4.2 ms/article, 0.992 cosine vs FP32 |
| Tests | 38 unit tests + 6-check end-to-end smoke test passing |

## Known limitations vs the production design

- **CPU instead of Triton or a GPU.** Triton is the production choice in the report; here ONNX Runtime runs on CPU
  with FastAPI micro-batching. Dynamic INT8 quantisation (MatMul only) gave 0.992 cosine agreement with FP32 and
  1.1–1.6× speed-up on this CPU, not the 2–4× the report cites for server CPUs and GPUs.
- **Single broker, single node, parallelism 1.** There's no replication, so the system shows correctness and the
  memory discipline, not horizontal scale.
- **PyFlink process mode is the throughput ceiling.** Every keyed-state access is a gRPC round trip to the JVM.
  To keep up with 60–120× replay, the topic operator accumulates counters in worker memory and writes them to keyed
  state once per roll-up. After a failure it can lose up to one roll-up interval (10 event-minutes) of counter
  increments. ADWIN detectors also restart empty. PyFlink's thread mode (in-JVM) would remove the round trips, but
  it hung silently with this job in testing.
- **Processing-time TTL.** Flink State TTL is processing-time only, so the event-time TTLs in the config are divided
  by the replay speed-up.
- **Synthetic publish times.** MIND has no publication timestamps; publish time = first impression − 30 s.
- **Single-node Qdrant on a 300 MB budget** occasionally stalls during segment optimisation. The serving fallback
  path absorbs this and its rate is reported.
- **No cross-session profile in serving.** Report §6 lists it as future work. `history_blend` in the offline
  evaluation estimates its value.
