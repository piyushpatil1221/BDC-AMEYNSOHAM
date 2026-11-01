"""Blend cold/fresh (content similarity) and warm (decayed engagement) candidates (report Section 5.1).

final = alpha * similarity + (1 - alpha) * warm_score / max_warm_score

* similarity: cosine between the session vector and the article embedding, from the ANN search
  (or from a locally known vector for warm-only candidates); negatives are clipped to 0.
* warm score: smoothed CTR * exp(-ln2 * age / topic_half_life), normalised to [0, 1].
* alpha is 0 for users without a session vector, which reduces to decayed popularity.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping


@dataclass(frozen=True)
class Scored:
    article_id: str
    score: float
    similarity: float
    warm: float


def blend(ann: Mapping[str, float], warm: Mapping[str, float], *, alpha: float, k: int,
          extra_similarity: Mapping[str, float] | None = None,
          exclude: set[str] | frozenset[str] = frozenset()) -> list[Scored]:
    if not 0.0 <= alpha <= 1.0:
        raise ValueError("alpha must be in [0, 1]")
    extra_similarity = extra_similarity or {}
    top_warm = max(warm.values(), default=0.0)
    out = []
    for aid in (set(ann) | set(warm)) - set(exclude):
        sim = max(0.0, ann.get(aid, extra_similarity.get(aid, 0.0)))
        w = warm.get(aid, 0.0) / top_warm if top_warm > 0 else 0.0
        out.append(Scored(aid, alpha * sim + (1 - alpha) * w, sim, w))
    out.sort(key=lambda s: (-s.score, s.article_id))
    return out[:k]
