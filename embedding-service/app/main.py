"""Embedding service: INT8 MiniLM on ONNX Runtime behind FastAPI, with request micro-batching.

Concurrent /embed requests are queued and merged into one encoder call of up to `max_batch` texts,
waiting at most `max_wait_ms` to fill it (the CPU analogue of Triton's dynamic batching,
report Section 5.2). The encoder runs in a worker thread so the event loop keeps accepting requests.
"""
from __future__ import annotations

import asyncio
import logging
import os
import time
from contextlib import asynccontextmanager

import numpy as np
import yaml
from fastapi import FastAPI, HTTPException
from fastapi.responses import Response
from prometheus_client import CONTENT_TYPE_LATEST, Histogram, generate_latest
from pydantic import BaseModel, Field

from app.encoder import Encoder

CFG = yaml.safe_load(open(os.environ.get("PIPELINE_CONFIG", "/config/pipeline.yaml"), encoding="utf-8"))["embedding"]

BATCH_SIZE = Histogram("embed_batch_size", "Texts per encoder call", buckets=(1, 2, 4, 8, 16, 32, 64))
ENCODE_SECONDS = Histogram("embed_encode_seconds", "Encoder call latency",
                           buckets=(0.005, 0.01, 0.02, 0.05, 0.1, 0.2, 0.5, 1))
REQUEST_SECONDS = Histogram("embed_request_seconds", "End-to-end /embed latency incl. queueing",
                            buckets=(0.005, 0.01, 0.02, 0.05, 0.1, 0.2, 0.5, 1))


class MicroBatcher:
    def __init__(self, encoder: Encoder, max_batch: int, max_wait_ms: int) -> None:
        self.encoder = encoder
        self.max_batch = max_batch
        self.max_wait = max_wait_ms / 1000
        self.queue: asyncio.Queue[tuple[list[str], asyncio.Future]] = asyncio.Queue()

    async def submit(self, texts: list[str]) -> np.ndarray:
        fut = asyncio.get_running_loop().create_future()
        await self.queue.put((texts, fut))
        return await fut

    async def run(self) -> None:
        # The loop must never die: if it did, every later request would time out with 503.
        while True:
            try:
                await self._one_batch()
            except asyncio.CancelledError:
                raise
            except Exception:
                logging.exception("micro-batch failed; continuing")

    async def _one_batch(self) -> None:
        loop = asyncio.get_running_loop()
        batch = [await self.queue.get()]
        n = len(batch[0][0])
        deadline = loop.time() + self.max_wait
        while n < self.max_batch:
            remaining = deadline - loop.time()
            if remaining <= 0:
                break
            try:
                item = await asyncio.wait_for(self.queue.get(), remaining)
            except asyncio.TimeoutError:
                break
            batch.append(item)
            n += len(item[0])
        texts = [t for texts, _ in batch for t in texts]
        BATCH_SIZE.observe(len(texts))
        t0 = time.perf_counter()
        try:
            vecs = await loop.run_in_executor(None, self.encoder.encode, texts)
        except Exception as e:  # fail every waiter in this batch, keep serving
            for _, fut in batch:
                if not fut.done():
                    fut.set_exception(e)
            return
        ENCODE_SECONDS.observe(time.perf_counter() - t0)
        i = 0
        for texts_i, fut in batch:
            # A waiter whose request already timed out has a cancelled future: skip it.
            if not fut.done():
                fut.set_result(vecs[i:i + len(texts_i)])
            i += len(texts_i)


batcher: MicroBatcher | None = None


@asynccontextmanager
async def lifespan(_: FastAPI):
    global batcher
    encoder = Encoder(os.environ["MODEL_PATH"], os.environ["TOKENIZER_PATH"], int(os.environ.get("ORT_THREADS", "2")))
    encoder.encode(["warm-up"])
    batcher = MicroBatcher(encoder, CFG["max_batch"], CFG["max_wait_ms"])
    task = asyncio.create_task(batcher.run())
    yield
    task.cancel()


app = FastAPI(title="embedding-service", lifespan=lifespan)


class EmbedRequest(BaseModel):
    texts: list[str] = Field(min_length=1, max_length=64)


class EmbedResponse(BaseModel):
    vectors: list[list[float]]
    dim: int
    ms: float


@app.post("/embed", response_model=EmbedResponse)
async def embed(req: EmbedRequest) -> EmbedResponse:
    assert batcher is not None
    t0 = time.perf_counter()
    try:
        # Generous: on an 8 GB laptop under memory pressure an encode can stall for seconds.
        vecs = await asyncio.wait_for(batcher.submit(req.texts), timeout=30)
    except asyncio.TimeoutError:
        raise HTTPException(503, "encoder busy")
    ms = (time.perf_counter() - t0) * 1000
    REQUEST_SECONDS.observe(ms / 1000)
    return EmbedResponse(vectors=vecs.round(6).tolist(), dim=vecs.shape[1], ms=round(ms, 2))


@app.get("/health")
def health() -> dict:
    return {"status": "ok"}


@app.get("/metrics")
def metrics() -> Response:
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)
