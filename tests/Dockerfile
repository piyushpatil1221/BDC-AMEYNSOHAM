FROM python:3.11-slim-bookworm
RUN pip install --no-cache-dir numpy==1.26.4 pytest==8.3.4 httpx==0.28.1 cassandra-driver==3.29.2
WORKDIR /w
ENV PYTHONPATH=/w
ENTRYPOINT ["pytest", "-q"]
