"""Compare FP32 vs INT8 encoders: latency per batch and embedding fidelity (cosine agreement).

Usage: python bench.py <fp32.onnx> <int8.onnx> <tokenizer.json>
"""
import statistics
import sys
import time

import numpy as np

sys.path.insert(0, "/build")
from app.encoder import Encoder  # noqa: E402

TEXTS = [
    "Stocks rally as central bank signals pause in rate hikes",
    "Local team clinches championship in overtime thriller",
    "New study links sleep quality to heart health in older adults",
    "Hurricane strengthens to category 4 as it approaches the coast",
    "Tech giant unveils foldable phone with week-long battery life",
    "Celebrity couple announces engagement on social media",
    "Senate passes infrastructure bill after marathon session",
    "Recipe: five-ingredient weeknight pasta the whole family will love",
] * 2  # 16 texts = one realistic micro-batch


def bench(enc: Encoder, runs: int = 30) -> tuple[np.ndarray, float, float]:
    enc.encode(TEXTS)  # warm-up
    times = []
    for _ in range(runs):
        t0 = time.perf_counter()
        out = enc.encode(TEXTS)
        times.append((time.perf_counter() - t0) * 1000)
    single = []
    for _ in range(runs):
        t0 = time.perf_counter()
        enc.encode(TEXTS[:1])
        single.append((time.perf_counter() - t0) * 1000)
    return out, statistics.median(times), statistics.median(single)


fp32_path, int8_path, tok = sys.argv[1:4]
fp32_out, fp32_batch, fp32_one = bench(Encoder(fp32_path, tok))
int8_out, int8_batch, int8_one = bench(Encoder(int8_path, tok))
cos = (fp32_out * int8_out).sum(axis=1)  # both unit-normalised

print(f"FP32: batch16 {fp32_batch:.1f} ms | single {fp32_one:.1f} ms")
print(f"INT8: batch16 {int8_batch:.1f} ms | single {int8_one:.1f} ms")
print(f"speed-up batch16: {fp32_batch / int8_batch:.2f}x | single: {fp32_one / int8_one:.2f}x")
print(f"FP32-vs-INT8 cosine: mean {cos.mean():.4f}, min {cos.min():.4f}")
