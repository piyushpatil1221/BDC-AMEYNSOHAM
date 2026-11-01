import pytest

from newsrec_core.drift import DecayPolicy, ctr_from_indicator_mean, relax, respond_to_drift

H = 3600.0
POLICY = DecayPolicy(default_s=6 * H, min_s=1 * H, max_s=24 * H, spike_factor=0.5, slow_burn_factor=1.5, relax_rate=0.1)


def test_spike_shortens_half_life():
    hl, direction = respond_to_drift(6 * H, baseline=0.04, current=0.30, policy=POLICY)
    assert direction == "spike" and hl == pytest.approx(3 * H)


def test_slow_burn_lengthens_half_life():
    hl, direction = respond_to_drift(6 * H, baseline=0.30, current=0.04, policy=POLICY)
    assert direction == "slow_burn" and hl == pytest.approx(9 * H)


def test_half_life_is_clamped():
    assert respond_to_drift(1.5 * H, 0.0, 1.0, POLICY)[0] == pytest.approx(1 * H)    # floor
    assert respond_to_drift(20 * H, 1.0, 0.0, POLICY)[0] == pytest.approx(24 * H)    # ceiling


def test_repeated_spikes_never_go_below_minimum():
    hl = 6 * H
    for _ in range(10):
        hl, _ = respond_to_drift(hl, 0.0, 1.0, POLICY)
    assert hl == pytest.approx(POLICY.min_s)


def test_relax_moves_toward_default_from_both_sides():
    assert 2 * H < relax(2 * H, POLICY) < 6 * H
    assert 6 * H < relax(12 * H, POLICY) < 12 * H
    assert relax(6 * H, POLICY) == pytest.approx(6 * H)


def test_indicator_mean_to_ctr():
    # 100 impressions + 5 clicks -> indicator mean 5/105 -> CTR 5%
    assert ctr_from_indicator_mean(5 / 105) == pytest.approx(0.05)
    assert ctr_from_indicator_mean(0.0) == 0.0
