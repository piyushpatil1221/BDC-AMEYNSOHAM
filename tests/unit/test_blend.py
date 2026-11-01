import pytest

from newsrec_core.blend import blend


def test_pure_popularity_when_alpha_zero():
    out = blend({"a": 0.9}, {"b": 0.10, "c": 0.05}, alpha=0.0, k=3)
    assert [s.article_id for s in out] == ["b", "c", "a"]
    assert out[0].score == pytest.approx(1.0)  # warm scores normalised by the max


def test_pure_similarity_when_alpha_one():
    out = blend({"a": 0.2, "b": 0.8}, {"a": 1.0}, alpha=1.0, k=2)
    assert [s.article_id for s in out] == ["b", "a"]


def test_blended_score_formula():
    out = blend({"a": 0.5}, {"a": 0.2, "b": 0.4}, alpha=0.6, k=2)
    a = next(s for s in out if s.article_id == "a")
    assert a.score == pytest.approx(0.6 * 0.5 + 0.4 * (0.2 / 0.4))


def test_union_dedups_and_respects_k():
    out = blend({"a": 0.9, "b": 0.8}, {"b": 1.0, "c": 0.5}, alpha=0.5, k=2)
    ids = [s.article_id for s in out]
    assert len(ids) == 2 and len(set(ids)) == 2


def test_excluded_items_never_returned():
    out = blend({"a": 0.9, "b": 0.1}, {"a": 1.0}, alpha=0.5, k=5, exclude={"a"})
    assert all(s.article_id != "a" for s in out)


def test_extra_similarity_used_for_warm_only_candidates():
    out = blend({}, {"w": 0.5}, alpha=1.0, k=1, extra_similarity={"w": 0.7})
    assert out[0].similarity == pytest.approx(0.7)


def test_negative_similarity_clipped():
    out = blend({"a": -0.4}, {}, alpha=1.0, k=1)
    assert out[0].score == 0.0


def test_empty_inputs():
    assert blend({}, {}, alpha=0.5, k=10) == []


def test_alpha_validated():
    with pytest.raises(ValueError):
        blend({}, {}, alpha=1.5, k=1)
