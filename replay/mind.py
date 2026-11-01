"""Parsing helpers for the MIND TSV formats.

behaviors.tsv: impression_id, user_id, time, history (space-separated news ids),
               impressions (space-separated "Nxxxx-<label>", label 1 = clicked)
news.tsv:      news_id, category, subcategory, title, abstract, url, title_entities, abstract_entities
"""
from __future__ import annotations

import csv
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator

csv.field_size_limit(1 << 24)
MIND_TIME = "%m/%d/%Y %I:%M:%S %p"


@dataclass(frozen=True)
class Impression:
    impression_id: str
    user_id: str
    ts_ms: int
    history: tuple[str, ...]
    candidates: tuple[tuple[str, int], ...]  # (news_id, clicked)


def parse_time(s: str) -> int:
    """MIND timestamps carry no zone; treat them as UTC so the conversion is deterministic."""
    return int(datetime.strptime(s, MIND_TIME).replace(tzinfo=timezone.utc).timestamp() * 1000)


def read_behaviors(path: Path, epoch_ms: bool = False) -> Iterator[Impression]:
    with path.open(encoding="utf-8", newline="") as f:
        for imp_id, user, t, hist, imps in csv.reader(f, delimiter="\t", quoting=csv.QUOTE_NONE):
            cands = []
            for c in imps.split():
                nid, label = c.rsplit("-", 1)
                cands.append((nid, int(label)))
            yield Impression(imp_id, user, int(t) if epoch_ms else parse_time(t),
                             tuple(hist.split()), tuple(cands))


def write_behaviors(path: Path, rows: list[Impression]) -> None:
    """Same column layout as MIND, but time is stored as epoch milliseconds."""
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.writer(f, delimiter="\t", quoting=csv.QUOTE_NONE, escapechar="\\", lineterminator="\n")
        for r in rows:
            w.writerow([r.impression_id, r.user_id, r.ts_ms, " ".join(r.history),
                        " ".join(f"{n}-{c}" for n, c in r.candidates)])


def read_news(path: Path) -> dict[str, dict[str, str]]:
    news = {}
    with path.open(encoding="utf-8", newline="") as f:
        for row in csv.reader(f, delimiter="\t", quoting=csv.QUOTE_NONE):
            nid, cat, sub, title, abstract, url = row[:6]
            news[nid] = {"category": cat, "subcategory": sub, "title": title,
                         "abstract": abstract, "url": url}
    return news
