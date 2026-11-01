# Stage 1: fetch the official FP32 ONNX export of all-MiniLM-L6-v2 and quantize it to INT8.
FROM python:3.11-slim-bookworm AS model
ARG HF_REV=1110a243fdf4706b3f48f1d95db1a4f5529b4d41
ARG HF=https://huggingface.co/sentence-transformers/all-MiniLM-L6-v2/resolve/${HF_REV}
RUN pip install --no-cache-dir onnxruntime==1.20.1 onnx==1.17.0
WORKDIR /build
ADD ${HF}/onnx/model.onnx model/model_fp32.onnx
ADD ${HF}/tokenizer.json model/tokenizer.json
COPY scripts/quantize.py scripts/quantize.py
RUN python scripts/quantize.py model/model_fp32.onnx model/model_int8.onnx

# Stage 2: slim runtime with only the INT8 model.
FROM python:3.11-slim-bookworm
WORKDIR /srv
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY --from=model /build/model/model_int8.onnx /build/model/tokenizer.json /models/
COPY app app
ENV MODEL_PATH=/models/model_int8.onnx TOKENIZER_PATH=/models/tokenizer.json ORT_THREADS=2
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "1"]
