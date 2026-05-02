from pyspark.sql import SparkSession
from pyspark.sql.functions import col


KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "ride_events"


def create_spark_session():
    spark = (
        SparkSession.builder
        .appName("TestKafkaRideEventsStream")
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

    events_df = kafka_df.select(
        col("key").cast("string").alias("ride_id_key"),
        col("value").cast("string").alias("event_json"),
        col("topic"),
        col("partition"),
        col("offset"),
        col("timestamp").alias("kafka_timestamp"),
    )

    query = (
        events_df.writeStream
        .format("console")
        .option("truncate", "false")
        .outputMode("append")
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()
