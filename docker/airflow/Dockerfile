FROM ride-hailing-spark:3.5.1 AS spark-base

FROM apache/airflow:2.9.3-python3.11

USER root

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        openjdk-17-jre-headless \
        procps \
        curl \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

COPY --from=spark-base /opt/spark /opt/spark

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV SPARK_HOME=/opt/spark
ENV PATH="${SPARK_HOME}/bin:${PATH}"

USER airflow

RUN pip install --no-cache-dir \
    psycopg2-binary \
    python-dotenv \
    boto3 \
    kafka-python
