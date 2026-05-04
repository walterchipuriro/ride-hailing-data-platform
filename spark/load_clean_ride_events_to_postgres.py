import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col


MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT") or "localhost:9000"
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY") or "minioadmin"
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY") or "minioadmin"
MINIO_BUCKET = os.getenv("MINIO_BUCKET") or "ride-hailing-data"

POSTGRES_HOST = os.getenv("POSTGRES_HOST") or "localhost"
POSTGRES_PORT = os.getenv("POSTGRES_PORT") or "5432"
POSTGRES_DB = os.getenv("POSTGRES_DB") or "ride_hailing_dw"
POSTGRES_USER = os.getenv("POSTGRES_USER") or "postgres"
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD") or "postgres"

CLEAN_RIDE_EVENTS_PATH = f"s3a://{MINIO_BUCKET}/clean/ride_events/"

POSTGRES_JDBC_URL = (
    f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
)

POSTGRES_TARGET_TABLE = "staging.ride_events"


def create_spark_session():
    spark = (
        SparkSession.builder
        .appName("LoadCleanRideEventsToPostgres")

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


def read_existing_event_ids(spark):
    query = """
        (
            SELECT event_id
            FROM staging.ride_events
            WHERE event_id IS NOT NULL
        ) existing_events
    """

    return (
        spark.read
        .format("jdbc")
        .option("url", POSTGRES_JDBC_URL)
        .option("dbtable", query)
        .option("user", POSTGRES_USER)
        .option("password", POSTGRES_PASSWORD)
        .option("driver", "org.postgresql.Driver")
        .load()
    )


def main():
    print("Starting MinIO clean ride events to PostgreSQL load")
    print(f"Source path: {CLEAN_RIDE_EVENTS_PATH}")
    print(f"Postgres JDBC URL: {POSTGRES_JDBC_URL}")
    print(f"Target table: {POSTGRES_TARGET_TABLE}")

    spark = create_spark_session()

    clean_df = spark.read.parquet(CLEAN_RIDE_EVENTS_PATH)

    print("Schema from MinIO clean ride events:")
    clean_df.printSchema()

    total_rows = clean_df.count()
    print(f"Rows found in clean lake path: {total_rows}")

    if total_rows == 0:
        print("No rows found. Nothing to load.")
        spark.stop()
        return

    staging_df = (
        clean_df
        .select(
            col("event_id"),
            col("ride_id"),
            col("driver_id"),
            col("rider_id"),
            col("event_type"),
            col("event_timestamp"),
            col("pickup_location"),
            col("dropoff_location"),
            col("fare_usd"),
            col("payment_method"),
            col("payment_status"),
            col("event_version"),
        )
        .filter(col("event_id").isNotNull())
        .dropDuplicates(["event_id"])
    )

    deduplicated_rows = staging_df.count()
    print(f"Rows after deduplication by event_id: {deduplicated_rows}")

    existing_event_ids_df = read_existing_event_ids(spark)
    existing_count = existing_event_ids_df.count()
    print(f"Existing rows already in staging.ride_events: {existing_count}")

    new_events_df = (
        staging_df.alias("new")
        .join(
            existing_event_ids_df.alias("existing"),
            col("new.event_id") == col("existing.event_id"),
            "left_anti"
        )
    )

    new_rows = new_events_df.count()
    print(f"New rows to insert into staging.ride_events: {new_rows}")

    if new_rows == 0:
        print("No new rows to insert. Staging table is already up to date.")
        spark.stop()
        return

    print("Sample new records to load:")
    new_events_df.show(20, truncate=False)

    (
        new_events_df.write
        .format("jdbc")
        .option("url", POSTGRES_JDBC_URL)
        .option("dbtable", POSTGRES_TARGET_TABLE)
        .option("user", POSTGRES_USER)
        .option("password", POSTGRES_PASSWORD)
        .option("driver", "org.postgresql.Driver")
        .mode("append")
        .save()
    )

    print(f"Loaded {new_rows} new rows into {POSTGRES_TARGET_TABLE}")

    spark.stop()


if __name__ == "__main__":
    main()