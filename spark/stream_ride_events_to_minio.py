import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    current_timestamp,
    from_json,
    lit,
    lower,
    to_date,
    to_timestamp,
    when,
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
)


KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS") or "localhost:9092"
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC") or "ride_events"
KAFKA_CONSUMER_GROUP = os.getenv("KAFKA_CONSUMER_GROUP") or "spark-ride-events-minio"

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT") or "localhost:9000"
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY") or "minioadmin"
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY") or "minioadmin"
MINIO_BUCKET = os.getenv("MINIO_BUCKET") or "ride-hailing-data"

RAW_OUTPUT_PATH = f"s3a://{MINIO_BUCKET}/raw/ride_events/"
CLEAN_OUTPUT_PATH = f"s3a://{MINIO_BUCKET}/clean/ride_events/"
REJECTED_OUTPUT_PATH = f"s3a://{MINIO_BUCKET}/rejected/ride_events/"

FOREACH_BATCH_CHECKPOINT_PATH = f"s3a://{MINIO_BUCKET}/checkpoints/ride_events/foreach_batch/"

ALLOWED_EVENT_TYPES = [
    "ride_requested",
    "driver_assigned",
    "trip_started",
    "trip_completed",
    "payment_completed",
]

ALLOWED_PAYMENT_METHODS = [
    "cash",
    "card",
    "ecocash",
]

ALLOWED_PAYMENT_STATUSES = [
    "pending",
    "paid",
    "failed",
]


ride_event_schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("ride_id", StringType(), True),
    StructField("driver_id", StringType(), True),
    StructField("rider_id", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("event_version", StringType(), True),
    StructField("event_timestamp", StringType(), True),
    StructField("pickup_location", StringType(), True),
    StructField("dropoff_location", StringType(), True),
    StructField("fare_usd", DoubleType(), True),
    StructField("payment_method", StringType(), True),
    StructField("payment_status", StringType(), True),
])


def create_spark_session():
    spark = (
        SparkSession.builder
        .appName("RideEventsKafkaToMinIO")

        # MinIO / S3A configuration
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
        )

        # Local dev stability
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
        .getOrCreate()
    )

    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.endpoint", MINIO_ENDPOINT)
    hadoop_conf.set("fs.s3a.access.key", MINIO_ACCESS_KEY)
    hadoop_conf.set("fs.s3a.secret.key", MINIO_SECRET_KEY)
    hadoop_conf.set("fs.s3a.path.style.access", "true")
    hadoop_conf.set("fs.s3a.connection.ssl.enabled", "false")
    hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    hadoop_conf.set(
        "fs.s3a.aws.credentials.provider",
        "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
    )

    spark.sparkContext.setLogLevel("WARN")
    return spark


def process_micro_batch(batch_df, batch_id):
    """
    This function runs once for every Spark micro-batch.

    One Kafka batch enters here, then we split it into:
    1. raw records
    2. clean valid records
    3. rejected invalid records
    """

    if batch_df.isEmpty():
        print(f"Batch {batch_id}: no records to process.")
        return

    batch_df.persist()

    total_count = batch_df.count()
    valid_count = batch_df.filter(col("is_valid") == True).count()
    rejected_count = batch_df.filter(col("is_valid") == False).count()

    print("=" * 100)
    print(f"Processing Spark micro-batch: {batch_id}")
    print(f"Total records: {total_count}")
    print(f"Valid records: {valid_count}")
    print(f"Rejected records: {rejected_count}")
    print("=" * 100)

    raw_batch_df = (
        batch_df
        .select(
            col("kafka_key"),
            col("raw_json"),
            col("topic"),
            col("partition"),
            col("offset"),
            col("kafka_timestamp"),
            col("ingested_at"),
            col("ingestion_date"),
        )
    )

    valid_clean_batch_df = (
        batch_df
        .filter(col("is_valid") == True)
        .select(
            col("event_id"),
            col("ride_id"),
            col("driver_id"),
            col("rider_id"),
            col("event_type"),
            col("event_version"),
            col("event_timestamp"),
            col("pickup_location"),
            col("dropoff_location"),
            col("fare_usd"),
            col("payment_method"),
            col("payment_status"),
            col("kafka_key"),
            col("topic"),
            col("partition"),
            col("offset"),
            col("kafka_timestamp"),
            col("ingested_at"),
            col("ingestion_date"),
        )
    )

    rejected_batch_df = (
        batch_df
        .filter(col("is_valid") == False)
        .select(
            col("validation_error"),
            col("raw_json"),
            col("event_id"),
            col("ride_id"),
            col("driver_id"),
            col("rider_id"),
            col("event_type"),
            col("event_version"),
            col("event_timestamp"),
            col("pickup_location"),
            col("dropoff_location"),
            col("fare_usd"),
            col("payment_method"),
            col("payment_status"),
            col("kafka_key"),
            col("topic"),
            col("partition"),
            col("offset"),
            col("kafka_timestamp"),
            col("ingested_at"),
            col("ingestion_date"),
        )
    )

    print(f"Batch {batch_id}: sample valid clean records")
    valid_clean_batch_df.show(20, truncate=False)

    print(f"Batch {batch_id}: sample rejected records")
    rejected_batch_df.show(20, truncate=False)

    raw_batch_df.write \
        .mode("append") \
        .partitionBy("ingestion_date") \
        .json(RAW_OUTPUT_PATH)

    valid_clean_batch_df.write \
        .mode("append") \
        .partitionBy("ingestion_date") \
        .parquet(CLEAN_OUTPUT_PATH)

    rejected_batch_df.write \
        .mode("append") \
        .partitionBy("ingestion_date") \
        .json(REJECTED_OUTPUT_PATH)

    print(f"Batch {batch_id}: write complete.")
    print(f"Raw output: {RAW_OUTPUT_PATH}")
    print(f"Clean output: {CLEAN_OUTPUT_PATH}")
    print(f"Rejected output: {REJECTED_OUTPUT_PATH}")

    batch_df.unpersist()


def main():
    print(f"KAFKA_BOOTSTRAP_SERVERS={KAFKA_BOOTSTRAP_SERVERS}")
    print(f"KAFKA_TOPIC={KAFKA_TOPIC}")
    print(f"KAFKA_CONSUMER_GROUP={KAFKA_CONSUMER_GROUP}")
    print(f"MINIO_ENDPOINT={MINIO_ENDPOINT}")
    print(f"MINIO_BUCKET={MINIO_BUCKET}")

    spark = create_spark_session()

    kafka_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("kafka.group.id", KAFKA_CONSUMER_GROUP)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )

    raw_df = (
        kafka_df
        .select(
            col("key").cast("string").alias("kafka_key"),
            col("value").cast("string").alias("raw_json"),
            col("topic"),
            col("partition"),
            col("offset"),
            col("timestamp").alias("kafka_timestamp"),
        )
        .withColumn("ingested_at", current_timestamp())
        .withColumn("ingestion_date", to_date(col("ingested_at")))
    )

    parsed_df = (
        raw_df
        .select(
            from_json(col("raw_json"), ride_event_schema).alias("event"),
            col("raw_json"),
            col("kafka_key"),
            col("topic"),
            col("partition"),
            col("offset"),
            col("kafka_timestamp"),
            col("ingested_at"),
            col("ingestion_date"),
        )
    )

    clean_df = (
        parsed_df
        .select(
            col("event.event_id").alias("event_id"),
            col("event.ride_id").alias("ride_id"),
            col("event.driver_id").alias("driver_id"),
            col("event.rider_id").alias("rider_id"),
            lower(col("event.event_type")).alias("event_type"),
            col("event.event_version").alias("event_version"),
            to_timestamp(
                col("event.event_timestamp"),
                "yyyy-MM-dd'T'HH:mm:ss"
            ).alias("event_timestamp"),
            col("event.pickup_location").alias("pickup_location"),
            col("event.dropoff_location").alias("dropoff_location"),
            col("event.fare_usd").alias("fare_usd"),
            lower(col("event.payment_method")).alias("payment_method"),
            lower(col("event.payment_status")).alias("payment_status"),
            col("raw_json"),
            col("kafka_key"),
            col("topic"),
            col("partition"),
            col("offset"),
            col("kafka_timestamp"),
            col("ingested_at"),
            col("ingestion_date"),
        )
    )

    validated_df = (
        clean_df
        .withColumn(
            "validation_error",
            when(col("event_id").isNull(), lit("missing_event_id"))
            .when(col("ride_id").isNull(), lit("missing_ride_id"))
            .when(col("driver_id").isNull(), lit("missing_driver_id"))
            .when(col("rider_id").isNull(), lit("missing_rider_id"))
            .when(col("event_type").isNull(), lit("missing_event_type"))
            .when(~col("event_type").isin(ALLOWED_EVENT_TYPES), lit("invalid_event_type"))
            .when(col("event_timestamp").isNull(), lit("missing_or_invalid_event_timestamp"))
            .when(col("pickup_location").isNull(), lit("missing_pickup_location"))
            .when(col("dropoff_location").isNull(), lit("missing_dropoff_location"))
            .when(col("payment_method").isNull(), lit("missing_payment_method"))
            .when(~col("payment_method").isin(ALLOWED_PAYMENT_METHODS), lit("invalid_payment_method"))
            .when(col("payment_status").isNull(), lit("missing_payment_status"))
            .when(~col("payment_status").isin(ALLOWED_PAYMENT_STATUSES), lit("invalid_payment_status"))
            .when(
                (col("event_type").isin("trip_completed", "payment_completed"))
                & col("fare_usd").isNull(),
                lit("fare_required_for_completed_events")
            )
            .when(
                (col("event_type").isin("trip_completed", "payment_completed"))
                & (col("fare_usd") <= 0),
                lit("fare_must_be_positive_for_completed_events")
            )
            .when(
                (col("event_type") == "payment_completed")
                & (col("payment_status") != "paid"),
                lit("payment_completed_must_have_paid_status")
            )
            .otherwise(lit(None))
        )
        .withColumn("is_valid", col("validation_error").isNull())
    )

    streaming_query = (
        validated_df.writeStream
        .queryName("ride_events_kafka_to_minio_foreach_batch")
        .foreachBatch(process_micro_batch)
        .option("checkpointLocation", FOREACH_BATCH_CHECKPOINT_PATH)
        .outputMode("append")
        .trigger(processingTime="15 seconds")
        .start()
    )

    print("Spark foreachBatch streaming job started.")
    print(f"Kafka topic: {KAFKA_TOPIC}")
    print(f"Kafka consumer group: {KAFKA_CONSUMER_GROUP}")
    print(f"Checkpoint: {FOREACH_BATCH_CHECKPOINT_PATH}")
    print(f"Raw output: {RAW_OUTPUT_PATH}")
    print(f"Clean output: {CLEAN_OUTPUT_PATH}")
    print(f"Rejected output: {REJECTED_OUTPUT_PATH}")

    streaming_query.awaitTermination()


if __name__ == "__main__":
    main()