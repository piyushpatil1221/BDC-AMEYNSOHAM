"""Offline recommendation quality on the replay subset (report Section 5.5).

Walks the MIND subset in event-time order exactly like the stream does. For each test impression
(day 2), every model scores the candidate list using only information from *before* that
impression; then the impression's labels are fed back (clicks update sessions and counters).
Metrics follow the official MIND evaluation: AUC, MRR, nDCG@5, nDCG@10, averaged over impressions.

Models
  random        - uniform scores (sanity floor)
  popularity    - clicks so far (the classic MIND popularity baseline)
  decayed_ctr   - our warm score: smoothed CTR x exp(-ln2 * age / half_life)
  session_blend - our serving formula: 0.6 * cos(session vector, article) + 0.4 * warm (normalised);
                  users without a live session fall back to decayed_ctr, as in the serving API
  history_blend - same blend, but users without a session use a profile vector built from their
                  MIND click history (a "cross-session personalisation" upper bound, report Section 6)

Usage: python -m eval.offline   (writes reports/eval.md and reports/eval.json)
"""
from __future__ import annotations

import json
import os
import random
import sys
import time
from collections import defaultdict
from pathlib import Path

import numpy as np
import yaml

sys.path.insert(0, "/srv")  # embedding-service image: app.encoder
from app.encoder import Encoder  # noqa: E402

from newsrec_core import session as sess  # noqa: E402
from newsrec_core.decay import decayed_score, smoothed_ctr  # noqa: E402
from replay.mind import read_behaviors, read_news  # noqa: E402

DAY_MS = 86_400_000


# ---- MIND metrics (same definitions as the official evaluation script) ----------------------
def auc(y: np.ndarray, s: np.ndarray) -> float:
    order = np.argsort(s)
    ranks = np.empty(len(s))
    ranks[order] = np.arange(1, len(s) + 1)
    pos = y == 1
    n_pos, n_neg = pos.sum(), (~pos).sum()
    return float((ranks[pos].sum() - n_pos * (n_pos + 1) / 2) / (n_pos * n_neg))


def mrr(y: np.ndarray, s: np.ndarray) -> float:
    yy = y[np.argsort(-s, kind="stable")]
    return float((yy / np.arange(1, len(yy) + 1)).sum() / yy.sum())


def ndcg(y: np.ndarray, s: np.ndarray, k: int) -> float:
    def dcg(order):
        g = y[order][:k]
        return float(((2 ** g - 1) / np.log2(np.arange(len(g)) + 2)).sum())
    return dcg(np.argsort(-s, kind="stable")) / dcg(np.argsort(-y, kind="stable"))


def main() -> None:
    cfg = yaml.safe_load(open(os.environ.get("PIPELINE_CONFIG", "/config/pipeline.yaml"), encoding="utf-8"))
    d, f, alpha = cfg["decay"], cfg["flink"], cfg["serving"]["alpha_session"]
    half_life_s = d["default_half_life_h"] * 3600
    session_hl_s = f["session_half_life_min"] * 60
    gap_ms = f["session_gap_min"] * 60_000

    subset = Path(cfg["data"]["subset_dir"])
    rows = list(read_behaviors(subset / "behaviors.tsv", epoch_ms=True))
    news = read_news(subset / "news_full.tsv")
    t0 = rows[0].ts_ms
    test_from = t0 + DAY_MS

    # Embed every article any model may need (candidates + users' histories), cached on disk.
    needed = sorted({n for r in rows for n, _ in r.candidates} | {n for r in rows for n in r.history} & set(news))
    cache = Path("/data/eval_embeddings.npz")
    if cache.exists() and set(np.load(cache)["ids"]) >= set(needed):
        z = np.load(cache)
        emb = dict(zip(z["ids"], z["vecs"]))
    else:
        enc = Encoder(os.environ["MODEL_PATH"], os.environ["TOKENIZER_PATH"], threads=4)
        t = time.time()
        vecs = []
        for i in range(0, len(needed), 64):
            batch = needed[i:i + 64]
            vecs.append(enc.encode([f"{news[n]['title']}. {news[n]['abstract']}".strip() for n in batch]))
        vecs = np.concatenate(vecs)
        np.savez(cache, ids=np.array(needed), vecs=vecs)
        emb = dict(zip(needed, vecs))
        print(f"embedded {len(needed)} articles in {time.time() - t:.0f}s", flush=True)

    # Streaming state, updated only *after* each impression is scored.
    clicks = defaultdict(int)
    shows = defaultdict(int)
    first_seen: dict[str, int] = {}
    sessions: dict[str, sess.SessionAcc] = {}
    last_click: dict[str, int] = {}
    history_vec: dict[str, np.ndarray | None] = {}
    rng = random.Random(0)
    models = ["random", "popularity", "decayed_ctr", "session_blend", "history_blend"]
    scores_by_model = {m: defaultdict(list) for m in models}
    session_only = {m: defaultdict(list) for m in models}  # impressions where the user had a live session
    n_with_session = n_test = 0

    def profile(user: str, hist: tuple[str, ...]) -> np.ndarray | None:
        if user not in history_vec:
            vs = [emb[h] for h in hist[-50:] if h in emb]
            if vs:
                v = np.mean(vs, axis=0)
                history_vec[user] = v / np.linalg.norm(v)
            else:
                history_vec[user] = None
        return history_vec[user]

    for r in rows:
        cands = [n for n, _ in r.candidates]
        for n in cands:
            first_seen.setdefault(n, r.ts_ms)
        if r.ts_ms >= test_from:
            y = np.array([c for _, c in r.candidates], dtype=np.float64)
            if 0 < y.sum() < len(y):
                n_test += 1
                warm = np.array([decayed_score(smoothed_ctr(clicks[n], shows[n], d["prior_ctr"], d["prior_weight"]),
                                               (r.ts_ms - first_seen[n]) / 1000, half_life_s) for n in cands])
                warm_n = warm / warm.max() if warm.max() > 0 else warm
                cand_vecs = np.stack([emb[n] for n in cands])
                acc = sessions.get(r.user_id)
                live = acc is not None and r.ts_ms - last_click.get(r.user_id, -10**18) <= gap_ms
                sv = sess.result(acc) if live else None
                n_with_session += sv is not None
                hv = sv if sv is not None else profile(r.user_id, r.history)
                model_scores = {
                    "random": np.array([rng.random() for _ in cands]),
                    "popularity": np.array([clicks[n] for n in cands], dtype=np.float64) + 1e-9 * warm,
                    "decayed_ctr": warm,
                    "session_blend": warm if sv is None else alpha * np.clip(cand_vecs @ sv, 0, None) + (1 - alpha) * warm_n,
                    "history_blend": warm if hv is None else alpha * np.clip(cand_vecs @ hv, 0, None) + (1 - alpha) * warm_n,
                }
                for m, s in model_scores.items():
                    for target in [scores_by_model[m]] + ([session_only[m]] if sv is not None else []):
                        target["auc"].append(auc(y, s))
                        target["mrr"].append(mrr(y, s))
                        target["ndcg@5"].append(ndcg(y, s, 5))
                        target["ndcg@10"].append(ndcg(y, s, 10))
        # feed back the labels
        for n, c in r.candidates:
            shows[n] += 1
            if c:
                clicks[n] += 1
                if r.ts_ms - last_click.get(r.user_id, -10**18) > gap_ms:
                    sessions[r.user_id] = sess.SessionAcc()  # gap exceeded: new session
                sessions[r.user_id] = sess.add(sessions.setdefault(r.user_id, sess.SessionAcc()), emb[n], r.ts_ms, n,
                                               session_hl_s)
                last_click[r.user_id] = r.ts_ms

    results = {m: {k: float(np.mean(v)) for k, v in scores_by_model[m].items()} for m in models}
    results_session = {m: {k: float(np.mean(v)) for k, v in session_only[m].items()} for m in models}
    meta = {"test_impressions": n_test, "with_live_session": n_with_session,
            "train_window": "day 1 of the subset", "test_window": "day 2 of the subset"}
    out = Path("/w/reports")
    out.mkdir(exist_ok=True)
    (out / "eval.json").write_text(json.dumps({"meta": meta, "results": results,
                                               "results_live_session_only": results_session}, indent=2))

    def table(res: dict) -> list[str]:
        rows_ = ["| Model | AUC | MRR | nDCG@5 | nDCG@10 |", "|---|---|---|---|---|"]
        for m in models:
            r_ = res[m]
            rows_.append(f"| {m} | {r_['auc']:.4f} | {r_['mrr']:.4f} | {r_['ndcg@5']:.4f} | {r_['ndcg@10']:.4f} |")
        return rows_

    lines = (["# Offline evaluation (MIND-small replay subset)", "",
              f"Test impressions: {n_test} (day 2, labels from MIND). Impressions where the user had a live "
              f"session vector: {n_with_session} ({n_with_session / max(n_test, 1):.1%}).", "",
              "## All test impressions", ""] + table(results) +
             ["", f"## Only impressions with a live session ({n_with_session})", ""] + table(results_session))
    (out / "eval.md").write_text("\n".join(lines) + "\n")
    print("\n".join(lines))


if __name__ == "__main__":
    main()
