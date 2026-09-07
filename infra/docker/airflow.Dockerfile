FROM apache/airflow:2.10.5-python3.11
USER root
RUN apt-get update && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
    && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 \
    LAKEBRIDGE_JARS=/opt/lakebridge/.runtime/jars \
    SPARK_LOCAL_IP=127.0.0.1
COPY --chown=airflow:root pyproject.toml /opt/lakebridge/
COPY --chown=airflow:root src /opt/lakebridge/src
COPY --chown=airflow:root scripts /opt/lakebridge/scripts
USER airflow
RUN pip install --no-cache-dir "apache-airflow==2.10.5" /opt/lakebridge \
    && cd /opt/lakebridge && python scripts/bootstrap.py

