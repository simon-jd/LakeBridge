FROM python:3.11.11-slim-bookworm
RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-17-jre-headless procps ca-certificates \
    && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 \
    PYTHONUNBUFFERED=1 \
    SPARK_LOCAL_IP=127.0.0.1 \
    LAKEBRIDGE_JARS=/opt/lakebridge/.runtime/jars
WORKDIR /opt/lakebridge
COPY pyproject.toml ./
COPY requirements.lock ./
RUN pip install --no-cache-dir --require-hashes -r requirements.lock
COPY src ./src
COPY scripts ./scripts
RUN pip install --no-cache-dir --no-deps . && python scripts/bootstrap.py
WORKDIR /workspace
ENTRYPOINT ["lakebridge"]
CMD ["--root", "/workspace/artifacts/compose", "status"]
