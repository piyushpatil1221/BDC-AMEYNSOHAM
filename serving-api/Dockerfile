# Build context: repository root (the API reuses newsrec_core).
FROM python:3.11-slim-bookworm
WORKDIR /srv
COPY serving-api/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY newsrec_core newsrec_core
COPY serving-api/app app
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "1", "--no-access-log"]
