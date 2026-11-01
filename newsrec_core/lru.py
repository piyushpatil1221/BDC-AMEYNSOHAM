"""Least-recently-used eviction.

`lru_victims` is the policy used against Flink MapState (which has no ordering), and `LRUCache`
is the in-process cache used by the serving API for its recent-articles fallback.
"""
from __future__ import annotations

from collections import OrderedDict
from typing import Generic, Hashable, Iterator, Mapping, TypeVar

K = TypeVar("K", bound=Hashable)
V = TypeVar("V")


def lru_victims(last_access: Mapping[K, int], capacity: int) -> list[K]:
    """Keys to evict so that at most `capacity` remain: the least recently accessed first.

    Ties on access time break on the key so the result is deterministic.
    """
    if capacity < 0:
        raise ValueError("capacity must be >= 0")
    overflow = len(last_access) - capacity
    if overflow <= 0:
        return []
    return [k for k, _ in sorted(last_access.items(), key=lambda kv: (kv[1], str(kv[0])))[:overflow]]


class LRUCache(Generic[K, V]):
    def __init__(self, capacity: int) -> None:
        if capacity <= 0:
            raise ValueError("capacity must be positive")
        self.capacity = capacity
        self.evictions = 0
        self._data: OrderedDict[K, V] = OrderedDict()

    def get(self, key: K) -> V | None:
        if key not in self._data:
            return None
        self._data.move_to_end(key)
        return self._data[key]

    def put(self, key: K, value: V) -> None:
        self._data[key] = value
        self._data.move_to_end(key)
        while len(self._data) > self.capacity:
            self._data.popitem(last=False)
            self.evictions += 1

    def __contains__(self, key: object) -> bool:
        return key in self._data

    def __len__(self) -> int:
        return len(self._data)

    def items(self) -> Iterator[tuple[K, V]]:
        """Most recently used last. Does not change recency."""
        return iter(list(self._data.items()))
