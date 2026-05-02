from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    current_timestamp,
    from_json,
    lower,
    to_date,
    to_timestamp,
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
)


KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "ride_events"

MINIO_ENDPOINT = "http://localhost:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
MINIO_BUCKET = "ride-hailing-data"

RAW_OUTPUT_PATH = f"s3a://{MINIO_BUCKET}/raw/ride_events/"
CLEAN_OUTPUT_PATH = f"s3a://{MINIO_BUCKET}/clean/ride_events/"
REJECTED_OUTPUT_PATH = f"s3a://{MINIO_BUCKET}/rejected/ride_events/"

RAW_CHECKPOINT_PATH = f"s3a://{MINIO_BUCKET}/checkpoints/ride_events/raw/"
CLEAN_CHECKPOINT_PATH = f"s3a://{MINIO_BUCKET}/checkpoints/ride_events/clean/"
REJECTED_CHECKPOINT_PATH = f"s3a://{MINIO_BUCKET}/checkpoints/ride_events/rejected/"


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
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    return spark


def main():
    spark = create_spark_session()

    kafka_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
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
            "is_valid",
            col("event_id").isNotNull()
            & col("ride_id").isNotNull()
            & col("event_type").isNotNull()
            & col("event_timestamp").isNotNull()
            & col("pickup_location").isNotNull()
            & col("dropoff_location").isNotNull()
        )
    )

    valid_clean_df = (
        validated_df
        .filter(col("is_valid") == True)
        .drop("is_valid")
    )

    rejected_df = (
        validated_df
        .filter(col("is_valid") == False)
        .select(
            col("event_id"),
            col("ride_id"),
            col("event_type"),
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

    raw_query = (
        raw_df.writeStream
        .format("json")
        .option("path", RAW_OUTPUT_PATH)
        .option("checkpointLocation", RAW_CHECKPOINT_PATH)
        .partitionBy("ingestion_date")
        .outputMode("append")
        .trigger(processingTime="15 seconds")
        .start()
    )

    clean_query = (
        valid_clean_df.writeStream
        .format("parquet")
        .option("path", CLEAN_OUTPUT_PATH)
        .option("checkpointLocation", CLEAN_CHECKPOINT_PATH)
        .partitionBy("ingestion_date")
        .outputMode("append")
        .trigger(processingTime="15 seconds")
        .start()
    )

    rejected_query = (
        rejected_df.writeStream
        .format("json")
        .option("path", REJECTED_OUTPUT_PATH)
        .option("checkpointLocation", REJECTED_CHECKPOINT_PATH)
        .partitionBy("ingestion_date")
        .outputMode("append")
        .trigger(processingTime="15 seconds")
        .start()
    )

    print("Spark streaming job started.")
    print(f"Kafka topic: {KAFKA_TOPIC}")
    print(f"Raw output: {RAW_OUTPUT_PATH}")
    print(f"Clean output: {CLEAN_OUTPUT_PATH}")
    print(f"Rejected output: {REJECTED_OUTPUT_PATH}")

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
