"""Loads config/pipeline.yaml (mounted at /config in every container)."""
import os
from functools import lru_cache
from typing import Any

import yaml


@lru_cache(maxsize=1)
def load() -> dict[str, Any]:
    with open(os.environ.get("PIPELINE_CONFIG", "/config/pipeline.yaml"), encoding="utf-8") as f:
        return yaml.safe_load(f)
