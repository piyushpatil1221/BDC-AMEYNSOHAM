# Offline evaluation (MIND-small replay subset)

Test impressions: 4624 (day 2, labels from MIND). Impressions where the user had a live session vector: 204 (4.4%).

## All test impressions

| Model | AUC | MRR | nDCG@5 | nDCG@10 |
|---|---|---|---|---|
| random | 0.5032 | 0.2562 | 0.2667 | 0.3211 |
| popularity | 0.6207 | 0.3482 | 0.3663 | 0.4133 |
| decayed_ctr | 0.6540 | 0.3785 | 0.3981 | 0.4419 |
| session_blend | 0.6548 | 0.3791 | 0.3986 | 0.4424 |
| history_blend | 0.7050 | 0.4025 | 0.4273 | 0.4769 |

## Only impressions with a live session (204)

| Model | AUC | MRR | nDCG@5 | nDCG@10 |
|---|---|---|---|---|
| random | 0.5275 | 0.2551 | 0.2584 | 0.3047 |
| popularity | 0.5548 | 0.2591 | 0.2671 | 0.3310 |
| decayed_ctr | 0.5908 | 0.2786 | 0.3013 | 0.3534 |
| session_blend | 0.6106 | 0.2911 | 0.3146 | 0.3627 |
| history_blend | 0.6106 | 0.2911 | 0.3146 | 0.3627 |
