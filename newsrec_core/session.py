"""Session interest vector: a decay-weighted mean of clicked-article embeddings.

Each click contributes its embedding with weight 1 at its own event time; older clicks in the
session fade with `half_life_s`, so the vector follows the user's *current* interest. The
accumulator is what Flink's session window keeps per user (AggregateFunction state).
"""
from __future__ import annotations

from dataclasses import dataclass, field

import numpy as np

from newsrec_core.decay import decay_factor

MAX_CLICKED = 50  # recent clicked ids kept, used to avoid recommending what the user already read


@dataclass
class SessionAcc:
    vec: np.ndarray | None = None   # weighted sum of embeddings
    weight: float = 0.0             # sum of weights (same decay as vec)
    last_ts: int = 0                # event time (ms) the sums are aligned to
    clicks: int = 0
    clicked: list[str] = field(default_factory=list)


def _aligned(acc: SessionAcc, ts: int, half_life_s: float) -> tuple[np.ndarray | None, float]:
    """The accumulator's sums decayed forward to event time `ts` (no-op if ts <= last_ts)."""
    if acc.vec is None:
        return None, 0.0
    f = decay_factor((ts - acc.last_ts) / 1000, half_life_s)
    return acc.vec * f, acc.weight * f


def add(acc: SessionAcc, vector: np.ndarray, ts: int, article_id: str, half_life_s: float) -> SessionAcc:
    v = np.asarray(vector, dtype=np.float32)
    if acc.vec is None:
        new_vec, new_w, new_ts = v.copy(), 1.0, ts
    elif ts >= acc.last_ts:
        old_vec, old_w = _aligned(acc, ts, half_life_s)
        new_vec, new_w, new_ts = old_vec + v, old_w + 1.0, ts
    else:  # late click: decay the newcomer to the accumulator's time instead
        f = decay_factor((acc.last_ts - ts) / 1000, half_life_s)
        new_vec, new_w, new_ts = acc.vec + v * f, acc.weight + f, acc.last_ts
    clicked = ([c for c in acc.clicked if c != article_id] + [article_id])[-MAX_CLICKED:]
    return SessionAcc(new_vec, new_w, new_ts, acc.clicks + 1, clicked)


def merge(a: SessionAcc, b: SessionAcc, half_life_s: float) -> SessionAcc:
    """Combine two partial sessions (session windows merge when a click bridges their gap)."""
    if a.vec is None:
        return b
    if b.vec is None:
        return a
    ts = max(a.last_ts, b.last_ts)
    av, aw = _aligned(a, ts, half_life_s)
    bv, bw = _aligned(b, ts, half_life_s)
    first, second = (a, b) if a.last_ts <= b.last_ts else (b, a)
    clicked = list(dict.fromkeys(first.clicked + second.clicked))[-MAX_CLICKED:]
    return SessionAcc(av + bv, aw + bw, ts, a.clicks + b.clicks, clicked)


def result(acc: SessionAcc) -> np.ndarray | None:
    """Unit-length session vector (so a dot product with article embeddings is cosine similarity)."""
    if acc.vec is None or acc.weight <= 0:
        return None
    mean = acc.vec / acc.weight
    norm = float(np.linalg.norm(mean))
    return None if norm == 0 else (mean / norm).astype(np.float32)
