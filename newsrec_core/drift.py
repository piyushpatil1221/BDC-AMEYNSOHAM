"""How a topic's half-life responds to concept drift (report Section 5.3).

ADWIN watches a per-event engagement indicator (1 = click, 0 = impression). When it reports a
change it drops the stale part of its window; we compare the window mean just before the cut
(old regime, `baseline`) with the mean right after it (new regime, `current`):
  * engagement went UP   -> "spike":     shorten the half-life so fresh items in the topic rise fast
  * engagement went DOWN -> "slow_burn": lengthen it so still-relevant older items stay visible
Between drifts the half-life relaxes back toward the default, so one burst doesn't stick forever.
"""
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class DecayPolicy:
    default_s: float
    min_s: float
    max_s: float
    spike_factor: float = 0.5
    slow_burn_factor: float = 1.5
    relax_rate: float = 0.05

    def clamp(self, half_life_s: float) -> float:
        return min(self.max_s, max(self.min_s, half_life_s))


def respond_to_drift(half_life_s: float, baseline: float, current: float,
                     policy: DecayPolicy) -> tuple[float, str]:
    """Return (new_half_life_s, direction)."""
    if current > baseline:
        return policy.clamp(half_life_s * policy.spike_factor), "spike"
    return policy.clamp(half_life_s * policy.slow_burn_factor), "slow_burn"


def relax(half_life_s: float, policy: DecayPolicy) -> float:
    """One roll-up step of mean reversion toward the default half-life."""
    return policy.clamp(half_life_s + (policy.default_s - half_life_s) * policy.relax_rate)


def ctr_from_indicator_mean(mean: float) -> float:
    """The indicator stream has one 0 per impression and one extra 1 per click, so its mean is
    m = c / (i + c). Solving for CTR = c / i gives m / (1 - m)."""
    if mean >= 1.0:
        return float("inf")
    return max(0.0, mean) / (1.0 - mean)
