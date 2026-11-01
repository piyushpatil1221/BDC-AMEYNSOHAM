"""Sentence encoder: all-MiniLM-L6-v2 on ONNX Runtime (CPU).

Reproduces what sentence-transformers does for this model, without PyTorch:
tokenize -> transformer -> mean-pool token embeddings over the attention mask -> L2-normalise.
The output is a 384-d unit vector, so cosine similarity == dot product (what Qdrant uses).
"""
from __future__ import annotations

from pathlib import Path

import numpy as np
import onnxruntime as ort
from tokenizers import Tokenizer

MAX_TOKENS = 128  # title + abstract rarely exceed this; longer text is truncated
DIM = 384


class Encoder:
    def __init__(self, model_path: str | Path, tokenizer_path: str | Path, threads: int = 2) -> None:
        opts = ort.SessionOptions()
        opts.intra_op_num_threads = threads
        opts.inter_op_num_threads = 1
        opts.graph_optimization_level = ort.GraphOptimizationLevel.ORT_ENABLE_ALL
        self.session = ort.InferenceSession(str(model_path), opts, providers=["CPUExecutionProvider"])
        self.input_names = {i.name for i in self.session.get_inputs()}

        self.tokenizer = Tokenizer.from_file(str(tokenizer_path))
        self.tokenizer.enable_truncation(max_length=MAX_TOKENS)
        self.tokenizer.enable_padding(pad_id=0, pad_token="[PAD]")  # pad to longest in batch

    def encode(self, texts: list[str]) -> np.ndarray:
        """Encode a batch of texts into an (n, 384) float32 array of unit vectors."""
        if not texts:
            return np.zeros((0, DIM), dtype=np.float32)
        enc = self.tokenizer.encode_batch(texts)
        input_ids = np.array([e.ids for e in enc], dtype=np.int64)
        attention = np.array([e.attention_mask for e in enc], dtype=np.int64)
        feeds = {"input_ids": input_ids, "attention_mask": attention}
        if "token_type_ids" in self.input_names:
            feeds["token_type_ids"] = np.zeros_like(input_ids)

        token_emb = self.session.run(None, feeds)[0]  # (n, seq, 384)
        mask = attention[..., None].astype(np.float32)
        pooled = (token_emb * mask).sum(axis=1) / np.clip(mask.sum(axis=1), 1e-9, None)
        norms = np.linalg.norm(pooled, axis=1, keepdims=True)
        return (pooled / np.clip(norms, 1e-12, None)).astype(np.float32)
