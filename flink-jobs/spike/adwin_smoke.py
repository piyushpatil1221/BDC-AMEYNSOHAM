"""Phase 0 spike: prove River's ADWIN imports and detects a CTR shift inside the Flink image.

Simulates a topic whose click-through rate jumps from 5% to 40% (a "breaking news" spike)
and reports how many events ADWIN needs to flag the change.
"""
import random

import numpy
import pandas
import pyflink
from river import drift

CHANGE_AT = 3000
random.seed(42)

adwin = drift.ADWIN(delta=0.002)
detected_at: list[int] = []
for i in range(6000):
    ctr = 0.05 if i < CHANGE_AT else 0.40
    clicked = 1 if random.random() < ctr else 0
    adwin.update(clicked)
    if adwin.drift_detected:
        detected_at.append(i)

print(f"versions: pyflink={pyflink.__version__ if hasattr(pyflink, '__version__') else 'n/a'} "
      f"numpy={numpy.__version__} pandas={pandas.__version__}")
print(f"true change at event {CHANGE_AT}; ADWIN detections at {detected_at}")
first_after = next((d for d in detected_at if d >= CHANGE_AT), None)
assert first_after is not None, "ADWIN missed the change"
assert all(d >= CHANGE_AT for d in detected_at), "false positive before the change"
print(f"OK: detection lag = {first_after - CHANGE_AT} events")
