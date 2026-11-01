"""Time-decay maths (report Section 3.2).

score = base_relevance * exp(-ln2 * age / half_life): after one half-life an item keeps 50% of its
relevance, after two 25%, and so on. The half-life is per topic and is adapted by the drift rule.
"""
import math

LN2 = math.log(2)


def decay_factor(age_s: float, half_life_s: float) -> float:
    """Fraction of relevance left after `age_s` seconds. Future-dated items (age < 0) are not boosted."""
    if half_life_s <= 0:
        raise ValueError("half_life_s must be positive")
    if age_s <= 0:
        return 1.0
    return math.exp(-LN2 * age_s / half_life_s)


def decayed_score(base: float, age_s: float, half_life_s: float) -> float:
    return base * decay_factor(age_s, half_life_s)


def smoothed_ctr(clicks: int, impressions: int, prior_ctr: float, prior_weight: float) -> float:
    """Bayesian-smoothed CTR: behaves like `prior_weight` virtual impressions at `prior_ctr`.

    Stops a brand-new article with 1 click out of 1 impression from outranking everything.
    """
    return (clicks + prior_ctr * prior_weight) / (impressions + prior_weight)
