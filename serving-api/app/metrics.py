"""Prometheus metrics for report Section 5.5 (exposed on /metrics; also summarised on /stats)."""
from prometheus_client import Counter, Gauge, Histogram

REC_LATENCY = Histogram("newsrec_recommend_latency_seconds", "End-to-end /recommend latency", ["path"],
                        buckets=(0.005, 0.01, 0.02, 0.03, 0.04, 0.05, 0.075, 0.1, 0.25, 0.5))
REC_REQUESTS = Counter("newsrec_recommend_requests_total", "Recommendation requests by serving path", ["path"])
FALLBACKS = Counter("newsrec_fallback_total", "Requests served from the in-memory cache because Qdrant was too slow")

COLD_DELAY = Histogram("newsrec_coldstart_delay_seconds", "Publication (replay emit) -> retrievable in Qdrant",
                       buckets=(0.25, 0.5, 1, 2, 5, 10, 20, 60))
COLD_TOTAL = Counter("newsrec_coldstart_articles_total", "Embedded articles observed")
COLD_OK = Counter("newsrec_coldstart_within_target_total", "Embedded articles retrievable within the target")

REC_AGE = Histogram("newsrec_recommendation_age_hours", "Age of recommended articles (event time)",
                    buckets=(0.5, 1, 2, 4, 8, 12, 24, 48))
FRESHNESS = Gauge("newsrec_freshness_median_age_hours", "Median article age in the latest top-k")

DRIFT_EVENTS = Counter("newsrec_drift_events_total", "Drift detections seen on drift.events", ["category", "direction"])
DRIFT_LAG = Gauge("newsrec_drift_detection_lag_seconds", "Burst start -> first spike detection", ["clock"])
HALF_LIFE = Gauge("newsrec_topic_half_life_hours", "Current decay half-life per topic", ["category"])
