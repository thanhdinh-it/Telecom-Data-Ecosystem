"""
PySpark Structured Streaming — Fraud Detection
Kafka → parse JSON → Sliding Window 1min/30s → fraud filter → PostgreSQL + DLQ.

Usage:
    spark-submit --master spark://spark-master:7077 \\
        --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0,org.postgresql:postgresql:42.6.0 \\
        spark_jobs/job_fraud_detection.py
"""
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
POSTGRES_HOST = os.environ.get("POSTGRES_HOST", "postgres")
POSTGRES_PORT = os.environ.get("POSTGRES_PORT", "5432")
POSTGRES_DB = os.environ.get("POSTGRES_DB", "telecom_db")
POSTGRES_USER = os.environ.get("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "changeme")

FRAUD_THRESHOLD = 50        # calls/minute → flag as fraud
WINDOW_DURATION = "1 minute"
SLIDE_DURATION = "30 seconds"
WATERMARK_DELAY = "2 minutes"
TRIGGER_INTERVAL = "30 seconds"

CHECKPOINT_DIR = "/tmp/spark-checkpoints/fraud-detection"
TOPIC_EVENTS = "telecom_events"
TOPIC_DLQ = "telecom_events_dlq"

POSTGRES_URL = f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
POSTGRES_PROPS = {
    "user": POSTGRES_USER,
    "password": POSTGRES_PASSWORD,
    "driver": "org.postgresql.Driver",
}

EVENT_SCHEMA = StructType([
    StructField("event_id", StringType(), True),
    StructField("caller_id", StringType(), True),
    StructField("callee_id", StringType(), True),
    StructField("event_timestamp", StringType(), True),
    StructField("duration_seconds", IntegerType(), True),
    StructField("call_type", StringType(), True),
    StructField("tower_id", StringType(), True),
])


def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder.appName("FraudDetection_Streaming")
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0,"
            "org.postgresql:postgresql:42.6.0",
        )
        .getOrCreate()
    )


def build_streaming_pipeline(spark: SparkSession):
    """
    Kafka raw → parse JSON → filter valid → sliding window aggregate → fraud flag.
    Returns: (fraud_alerts_df, malformed_df)
    """
    raw_stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", TOPIC_EVENTS)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    parsed = raw_stream.select(
        F.from_json(F.col("value").cast("string"), EVENT_SCHEMA).alias("data"),
        F.col("value").cast("string").alias("raw_value"),
    )

    valid_events = (
        parsed.filter(F.col("data.caller_id").isNotNull())
        .select("data.*")
        .withColumn("event_timestamp", F.to_timestamp("event_timestamp"))
        .filter(F.col("event_timestamp").isNotNull())
    )

    # Invalid JSON → Dead-Letter Queue
    malformed_events = parsed.filter(
        F.col("data.caller_id").isNull()
    ).select(F.col("raw_value").alias("value"))

    windowed_counts = (
        valid_events.withWatermark("event_timestamp", WATERMARK_DELAY)
        .groupBy(
            F.window("event_timestamp", WINDOW_DURATION, SLIDE_DURATION),
            F.col("caller_id"),
        )
        .agg(F.count("*").alias("call_count"))
    )

    fraud_alerts = windowed_counts.filter(
        F.col("call_count") > FRAUD_THRESHOLD
    ).select(
        F.col("caller_id"),
        F.col("window.start").alias("window_start"),
        F.col("window.end").alias("window_end"),
        F.col("call_count"),
        F.lit(True).alias("is_fraud"),
        F.current_timestamp().alias("detected_at"),
    )

    return fraud_alerts, malformed_events


def write_fraud_to_postgres(batch_df, batch_id: int) -> None:
    """foreachBatch: ghi fraud alerts vào PostgreSQL qua JDBC."""
    count = batch_df.count()
    if count > 0:
        batch_df.write.jdbc(
            POSTGRES_URL, "fraud_alerts", mode="append", properties=POSTGRES_PROPS
        )
        print(f"[BATCH {batch_id}] Wrote {count} fraud alerts to PostgreSQL")
    else:
        print(f"[BATCH {batch_id}] No fraud alerts to write")


def write_dlq_to_kafka(batch_df, batch_id: int) -> None:
    """foreachBatch: ghi malformed events ra Dead-Letter Queue topic."""
    count = batch_df.count()
    if count > 0:
        batch_df.write.format("kafka").option(
            "kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS
        ).option("topic", TOPIC_DLQ).save()
        print(f"[BATCH {batch_id}] Sent {count} malformed events to DLQ: {TOPIC_DLQ}")


if __name__ == "__main__":
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    print(f"[START] Fraud Detection Streaming")
    print(f"  Kafka: {KAFKA_BOOTSTRAP_SERVERS} | Topic: {TOPIC_EVENTS}")
    print(f"  Fraud threshold: {FRAUD_THRESHOLD} calls / {WINDOW_DURATION}")
    print(f"  Watermark: {WATERMARK_DELAY} | Trigger: {TRIGGER_INTERVAL}")

    fraud_alerts, malformed_events = build_streaming_pipeline(spark)

    query_fraud = (
        fraud_alerts.writeStream.foreachBatch(write_fraud_to_postgres)
        .outputMode("update")
        .option("checkpointLocation", f"{CHECKPOINT_DIR}/fraud-pg")
        .trigger(processingTime=TRIGGER_INTERVAL)
        .start()
    )

    query_dlq = (
        malformed_events.writeStream.foreachBatch(write_dlq_to_kafka)
        .outputMode("append")
        .option("checkpointLocation", f"{CHECKPOINT_DIR}/dlq")
        .trigger(processingTime=TRIGGER_INTERVAL)
        .start()
    )

    print("[RUNNING] Streaming queries started. Awaiting termination...")
    spark.streams.awaitAnyTermination()
