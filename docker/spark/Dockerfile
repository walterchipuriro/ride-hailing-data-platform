FROM apache/spark:3.5.1

USER root

RUN mkdir -p /tmp/.ivy2/cache /tmp/.ivy2/jars /opt/spark/custom-jars

# Create a tiny Spark job so spark-submit actually resolves and downloads packages.
RUN echo 'print("Downloading Spark dependencies during image build...")' > /tmp/download_deps.py

# Download required dependencies once during image build.
# These are needed for:
# - Kafka source: .format("kafka")
# - MinIO/S3A writing: s3a:// paths
RUN /opt/spark/bin/spark-submit \
    --conf spark.jars.ivy=/tmp/.ivy2 \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.apache.hadoop:hadoop-aws:3.3.4 \
    /tmp/download_deps.py

# Copy resolved JARs into Spark's runtime classpath.
RUN cp /tmp/.ivy2/jars/*.jar /opt/spark/jars/

# Optional: keep a copy for inspection/debugging.
RUN cp /tmp/.ivy2/jars/*.jar /opt/spark/custom-jars/

RUN chown -R spark:spark /opt/spark/jars /opt/spark/custom-jars /tmp/.ivy2

USER spark

