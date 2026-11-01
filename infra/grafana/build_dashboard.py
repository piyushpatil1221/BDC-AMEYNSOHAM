"""Generate infra/grafana/dashboards/newsrec.json (report Section 5.5 metrics). Run: python infra/grafana/build_dashboard.py"""
import json
from pathlib import Path

DS = {"type": "prometheus", "uid": "prom"}
panels = []


def panel(title, exprs, kind="timeseries", unit=None, w=12, h=8, thresholds=None, desc=""):
    pid = len(panels) + 1
    col = (sum(p["gridPos"]["w"] for p in panels) % 24)
    y = sum(p["gridPos"]["h"] for p in panels if p["gridPos"]["x"] == 0) if col == 0 else panels[-1]["gridPos"]["y"]
    if col + w > 24:
        col, y = 0, panels[-1]["gridPos"]["y"] + panels[-1]["gridPos"]["h"]
    defaults = {"unit": unit} if unit else {}
    if thresholds:
        defaults["thresholds"] = {"mode": "absolute", "steps": thresholds}
    if kind == "timeseries":
        defaults["custom"] = {"lineWidth": 2, "fillOpacity": 0, "showPoints": "never"}
    panels.append({
        "id": pid, "type": kind, "title": title, "description": desc, "datasource": DS,
        "gridPos": {"x": col, "y": y, "w": w, "h": h},
        "fieldConfig": {"defaults": defaults, "overrides": []},
        "options": {"legend": {"displayMode": "list", "placement": "bottom"}, "tooltip": {"mode": "multi"}}
        if kind == "timeseries" else {"reduceOptions": {"calcs": ["lastNotNull"]}, "colorMode": "value"},
        "targets": [{"refId": chr(65 + i), "expr": e, "legendFormat": lf, "datasource": DS} for i, (e, lf) in enumerate(exprs)],
    })


GREEN_RED = [{"color": "green", "value": None}, {"color": "red", "value": 0.05}]

panel("Cold-start coverage (retrievable within 10 s)",
      [("sum(newsrec_coldstart_within_target_total) / sum(newsrec_coldstart_articles_total)", "coverage")],
      kind="stat", unit="percentunit", w=6, h=5, desc="Share of new articles searchable in Qdrant within 10 s of publication")
panel("Recommend p95 latency", [("histogram_quantile(0.95, sum(rate(newsrec_recommend_latency_seconds_bucket[1m])) by (le))", "p95")],
      kind="stat", unit="s", w=6, h=5, thresholds=GREEN_RED)
panel("Fallback rate", [("sum(rate(newsrec_fallback_total[1m])) / sum(rate(newsrec_recommend_requests_total[1m]))", "fallback")],
      kind="stat", unit="percentunit", w=6, h=5)
panel("Drift-detection lag (burst start -> first spike)", [("newsrec_drift_detection_lag_seconds", "{{clock}} time")],
      kind="stat", unit="s", w=6, h=5)

panel("Recommendation latency vs 50 ms budget",
      [("histogram_quantile(0.95, sum(rate(newsrec_recommend_latency_seconds_bucket[1m])) by (le))", "p95"),
       ("histogram_quantile(0.99, sum(rate(newsrec_recommend_latency_seconds_bucket[1m])) by (le))", "p99"),
       ("vector(0.05)", "budget")], unit="s")
panel("Requests by serving path", [("sum(rate(newsrec_recommend_requests_total[1m])) by (path)", "{{path}}")], unit="reqps")

panel("Cold-start delay (publication -> retrievable)",
      [("histogram_quantile(0.5, sum(rate(newsrec_coldstart_delay_seconds_bucket[2m])) by (le))", "p50"),
       ("histogram_quantile(0.95, sum(rate(newsrec_coldstart_delay_seconds_bucket[2m])) by (le))", "p95")], unit="s")
panel("Recommendation freshness (median age of top-k, event time)", [("newsrec_freshness_median_age_hours", "median age")], unit="h")

panel("Topic half-life (adapted by drift)", [("newsrec_topic_half_life_hours", "{{category}}")], unit="h")
panel("Drift detections", [("sum(increase(newsrec_drift_events_total[5m])) by (category, direction)", "{{category}} {{direction}}")])

panel("Flink checkpoint size", [("flink_jobmanager_job_lastCheckpointFullSize", "full state"),
                                ("flink_jobmanager_job_lastCheckpointSize", "incremental")], unit="bytes")
panel("Flink checkpoint duration", [("flink_jobmanager_job_lastCheckpointDuration", "duration")], unit="ms")

panel("RocksDB live state by state name",
      [('sum by (__name__) ({__name__=~"flink_taskmanager_job_task_operator_python_state_.*_rocksdb_estimate_live_data_size"})',
        "{{__name__}}")], unit="bytes", desc="Embedding cache (vectors), topic items, sessions... bounded by TTL/LRU")
panel("TaskManager memory", [("flink_taskmanager_Status_JVM_Memory_Heap_Used", "JVM heap"),
                             ("flink_taskmanager_Status_Flink_Memory_Managed_Used", "managed (RocksDB + Python)")], unit="bytes")

panel("Embedding cache", [("sum(rate(flink_taskmanager_job_task_operator_cache_hits[1m]))", "hits/s"),
                          ("sum(rate(flink_taskmanager_job_task_operator_cache_misses[1m]))", "misses/s"),
                          ("sum(rate(flink_taskmanager_job_task_operator_cache_evictions[1m]))", "LRU evictions/s")])
panel("Embedding service", [("histogram_quantile(0.95, sum(rate(embed_encode_seconds_bucket[1m])) by (le))", "encode p95"),
                            ("sum(rate(embed_batch_size_sum[1m])) / sum(rate(embed_batch_size_count[1m]))", "avg batch size")])

dash = {"uid": "newsrec", "title": "NewsRec - real-time pipeline", "timezone": "browser", "refresh": "5s",
        "time": {"from": "now-30m", "to": "now"}, "schemaVersion": 39, "panels": panels, "tags": ["newsrec"]}
out = Path(__file__).parent / "dashboards" / "newsrec.json"
out.parent.mkdir(exist_ok=True)
out.write_text(json.dumps(dash, indent=2))
print(f"wrote {out} ({len(panels)} panels)")
