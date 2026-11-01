import numpy as np
import pytest

from newsrec_core.session import SessionAcc, add, merge, result

HL = 600.0  # 10 min half-life
X = np.array([1.0, 0.0], dtype=np.float32)
Y = np.array([0.0, 1.0], dtype=np.float32)


def test_single_click_is_that_embedding():
    acc = add(SessionAcc(), X, ts=0, article_id="a", half_life_s=HL)
    assert np.allclose(result(acc), X)


def test_recent_click_dominates_older_one():
    acc = add(SessionAcc(), X, ts=0, article_id="a", half_life_s=HL)
    acc = add(acc, Y, ts=int(2 * HL * 1000), article_id="b", half_life_s=HL)  # X decayed to 25%
    v = result(acc)
    assert v[1] > v[0]
    assert v[0] / v[1] == pytest.approx(0.25, rel=1e-5)


def test_result_is_unit_length():
    acc = add(add(SessionAcc(), X, 0, "a", HL), Y, 1000, "b", HL)
    assert np.linalg.norm(result(acc)) == pytest.approx(1.0)


def test_late_click_is_down_weighted_not_reordered():
    acc = add(SessionAcc(), Y, ts=int(HL * 1000), article_id="b", half_life_s=HL)
    acc = add(acc, X, ts=0, article_id="a", half_life_s=HL)  # arrives late, one half-life older
    assert acc.last_ts == int(HL * 1000)
    v = result(acc)
    assert v[0] / v[1] == pytest.approx(0.5, rel=1e-5)


def test_merge_equals_sequential_adds():
    seq = add(add(SessionAcc(), X, 0, "a", HL), Y, 60_000, "b", HL)
    merged = merge(add(SessionAcc(), X, 0, "a", HL), add(SessionAcc(), Y, 60_000, "b", HL), HL)
    assert np.allclose(result(seq), result(merged))
    assert merged.clicks == 2 and merged.clicked == ["a", "b"]


def test_clicked_list_dedups_and_keeps_order():
    acc = SessionAcc()
    for i, aid in enumerate(["a", "b", "a"]):
        acc = add(acc, X, i, aid, HL)
    assert acc.clicked == ["b", "a"] and acc.clicks == 3


def test_empty_session_has_no_vector():
    assert result(SessionAcc()) is None
