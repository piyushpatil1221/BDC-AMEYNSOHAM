import math

import pytest

from newsrec_core.decay import decay_factor, decayed_score, smoothed_ctr


def test_one_half_life_halves_the_score():
    assert decayed_score(0.8, age_s=3600, half_life_s=3600) == pytest.approx(0.4)


def test_two_half_lives_quarter_the_score():
    assert decay_factor(7200, 3600) == pytest.approx(0.25)


def test_zero_or_negative_age_is_not_boosted():
    assert decay_factor(0, 3600) == 1.0
    assert decay_factor(-500, 3600) == 1.0


def test_shorter_half_life_decays_faster():
    assert decay_factor(3600, 1800) < decay_factor(3600, 7200)


def test_matches_closed_form():
    assert decay_factor(1234, 5678) == pytest.approx(math.exp(-math.log(2) * 1234 / 5678))


def test_invalid_half_life_rejected():
    with pytest.raises(ValueError):
        decay_factor(10, 0)


def test_smoothed_ctr_starts_at_prior_and_converges():
    assert smoothed_ctr(0, 0, prior_ctr=0.05, prior_weight=20) == pytest.approx(0.05)
    # 1/1 does not jump to 100%
    assert smoothed_ctr(1, 1, 0.05, 20) < 0.1
    # with lots of evidence it approaches the observed rate
    assert smoothed_ctr(300, 1000, 0.05, 20) == pytest.approx(0.3, abs=0.01)
