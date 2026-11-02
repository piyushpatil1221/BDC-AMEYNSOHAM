"""Tiny HTTP and ScyllaDB helpers used inside PyFlink operators (stdlib HTTP keeps the worker image small)."""
from __future__ import annotations

import json
import logging
import time
import urllib.request
from typing import Any

log = logging.getLogger("newsrec")


def http_json(method: str, url: str, payload: Any, timeout: float = 45, retries: int = 8) -> Any:
    """JSON request with exponential back-off (0.5 s ... 64 s). Raises after `retries` attempts so
    Flink restarts the task from the last checkpoint instead of silently dropping data; the long
    budget rides out a dependency that is merely slow (e.g. the host paging) without a restart."""
    body = json.dumps(payload).encode()
    for attempt in range(retries):
        try:
            req = urllib.request.Request(url, data=body, method=method, headers={"Content-Type": "application/json"})
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return json.loads(resp.read() or b"null")
        except Exception as e:
            if attempt == retries - 1:
                raise
            log.warning("HTTP %s %s failed (%s), retrying", method, url, e)
            time.sleep(0.5 * 2 ** attempt)


def scylla_session(hosts: list[str]):
    from cassandra.cluster import Cluster  # imported lazily: only operators that write need it

    for attempt in range(30):
        try:
            return Cluster(hosts, protocol_version=4).connect()
        except Exception as e:
            log.warning("ScyllaDB not ready (%s), retrying", e)
            time.sleep(2)
    raise RuntimeError("could not connect to ScyllaDB")
