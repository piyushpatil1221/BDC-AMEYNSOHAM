import pytest

from newsrec_core.lru import LRUCache, lru_victims


def test_no_victims_under_capacity():
    assert lru_victims({"a": 1, "b": 2}, capacity=2) == []


def test_victims_are_least_recently_accessed():
    access = {"a": 10, "b": 5, "c": 30, "d": 1}
    assert lru_victims(access, capacity=2) == ["d", "b"]


def test_victim_ties_break_deterministically():
    assert lru_victims({"y": 1, "x": 1, "z": 9}, capacity=1) == ["x", "y"]


def test_zero_capacity_evicts_everything():
    assert sorted(lru_victims({"a": 1, "b": 2}, capacity=0)) == ["a", "b"]


def test_cache_bounded_and_evicts_oldest():
    c: LRUCache[str, int] = LRUCache(2)
    c.put("a", 1)
    c.put("b", 2)
    c.put("c", 3)
    assert len(c) == 2 and "a" not in c and c.evictions == 1


def test_get_refreshes_recency():
    c: LRUCache[str, int] = LRUCache(2)
    c.put("a", 1)
    c.put("b", 2)
    assert c.get("a") == 1      # a is now most recent
    c.put("c", 3)               # so b is evicted
    assert "a" in c and "b" not in c


def test_put_existing_key_updates_without_eviction():
    c: LRUCache[str, int] = LRUCache(2)
    c.put("a", 1)
    c.put("b", 2)
    c.put("a", 10)
    assert c.get("a") == 10 and len(c) == 2 and c.evictions == 0


def test_items_order_is_lru_to_mru():
    c: LRUCache[str, int] = LRUCache(3)
    for k in "abc":
        c.put(k, 0)
    c.get("a")
    assert [k for k, _ in c.items()] == ["b", "c", "a"]


def test_invalid_capacity():
    with pytest.raises(ValueError):
        LRUCache(0)
