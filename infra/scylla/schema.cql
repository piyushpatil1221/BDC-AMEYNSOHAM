-- ScyllaDB (Cassandra-compatible) schema. Flink writes rolled-up state here so it never holds history.
CREATE KEYSPACE IF NOT EXISTS newsrec
  WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

-- Latest session interest vector per user (written by the session stage, read by the serving API).
CREATE TABLE IF NOT EXISTS newsrec.session_vectors (
  user_id     text PRIMARY KEY,
  vector      list<float>,
  clicks      int,
  clicked     list<text>,  -- recent clicked article ids (excluded from recommendations)
  session_end timestamp,   -- event time of the last click in the session
  updated_at  timestamp    -- wall clock
) WITH default_time_to_live = 7200;

-- Current decay parameters per topic (category), rolled up periodically by the drift stage.
CREATE TABLE IF NOT EXISTS newsrec.topic_decay (
  category    text PRIMARY KEY,
  half_life_s double,
  ewma_ctr    double,
  drift_count int,
  event_time  timestamp,
  updated_at  timestamp
);

-- Half-life history for the dashboard chart (one row per roll-up per topic).
CREATE TABLE IF NOT EXISTS newsrec.topic_decay_history (
  category    text,
  event_time  timestamp,
  half_life_s double,
  ewma_ctr    double,
  drift       boolean,
  PRIMARY KEY (category, event_time)
) WITH CLUSTERING ORDER BY (event_time DESC) AND default_time_to_live = 86400;

-- Warm item scores: base relevance from engagement; decay is applied at read time with the topic half-life.
CREATE TABLE IF NOT EXISTS newsrec.item_scores (
  article_id     text PRIMARY KEY,
  category       text,
  base_relevance double,
  clicks         int,
  impressions    int,
  publish_ts     timestamp,
  updated_at     timestamp
) WITH default_time_to_live = 172800;
