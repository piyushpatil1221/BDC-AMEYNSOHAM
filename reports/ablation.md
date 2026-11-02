# Eviction ablation

Same replay (24 h of event time at 120x), fresh state per run.

| Run | Final state (MB) | Max state (MB) | Mean checkpoint (ms) | Max checkpoint (ms) | Peak TM memory (MiB) | Watchdog cutoff |
|---|---|---|---|---|---|---|
| TTL + LRU eviction ON | 1.467 | 3.232 | 11055 | 36837 | 891.9 | not reached |
| Eviction OFF | 6.383 | 6.396 | 8183 | 29635 | 888.0 | not reached |
