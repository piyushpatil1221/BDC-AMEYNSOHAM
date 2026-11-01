# Flink 1.20.5 runtime + Python 3.11 for PyFlink user code.
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

COPY requirements.txt /tmp/requirements.txt
RUN python3.11 -m venv /opt/venv \
 && /opt/venv/bin/pip install --no-cache-dir --upgrade pip==24.3.1 \
 && /opt/venv/bin/pip install --no-cache-dir --prefer-binary -r /tmp/requirements.txt

# `flink run -py` and the Python workers both look for `python` on PATH.
ENV PATH="/opt/venv/bin:${PATH}"

# Checkpoint directory: a named volume mounted here inherits this ownership on first use.
RUN mkdir -p /opt/flink/checkpoints && chown flink:flink /opt/flink/checkpoints

COPY . /opt/flink/usrlib/
