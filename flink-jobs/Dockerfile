# Flink 1.20.5 runtime + Python 3.11 for PyFlink user code. Build context: repository root.
# The official image is Ubuntu 22.04, whose stock python3 is 3.10, so Python 3.11
# comes from the deadsnakes PPA and lives in an isolated venv at /opt/venv.
FROM flink:1.20.5-java11

RUN apt-get update \
 && apt-get install -y --no-install-recommends software-properties-common gpg-agent \
 && add-apt-repository -y ppa:deadsnakes/ppa \
 && apt-get update \
 && apt-get install -y --no-install-recommends python3.11 python3.11-venv \
 && apt-get purge -y software-properties-common gpg-agent \
 && apt-get autoremove -y \
 && rm -rf /var/lib/apt/lists/*

COPY flink-jobs/requirements.txt /tmp/requirements.txt
RUN python3.11 -m venv /opt/venv \
 && /opt/venv/bin/pip install --no-cache-dir --upgrade pip==24.3.1 \
 && /opt/venv/bin/pip install --no-cache-dir --prefer-binary -r /tmp/requirements.txt

# PyFlink "thread" execution mode embeds CPython in the TaskManager JVM through PEMJA, which
# needs the shared libpython (no gRPC round trip per state access, no extra worker processes).
RUN apt-get update \
 && apt-get install -y --no-install-recommends software-properties-common gpg-agent \
 && add-apt-repository -y ppa:deadsnakes/ppa \
 && apt-get update \
 && apt-get install -y --no-install-recommends libpython3.11 \
 && ln -sf /usr/lib/x86_64-linux-gnu/libpython3.11.so.1.0 /usr/lib/x86_64-linux-gnu/libpython3.11.so \
 && apt-get purge -y software-properties-common gpg-agent \
 && apt-get autoremove -y \
 && rm -rf /var/lib/apt/lists/*

# `flink run -py` and the Python workers both look for `python` on PATH; job code lives in usrlib.
ENV PATH="/opt/venv/bin:${PATH}" PYTHONPATH=/opt/flink/usrlib

# Kafka connector for the DataStream KafkaSource/KafkaSink.
ADD --chown=flink:flink https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.3.0-1.20/flink-sql-connector-kafka-3.3.0-1.20.jar /opt/flink/lib/

# Checkpoint directory: a named volume mounted here inherits this ownership on first use.
RUN mkdir -p /opt/flink/checkpoints && chown flink:flink /opt/flink/checkpoints

COPY flink-jobs/spike /opt/flink/usrlib/spike
COPY newsrec_core /opt/flink/usrlib/newsrec_core
COPY flink-jobs/newsrec /opt/flink/usrlib/newsrec
