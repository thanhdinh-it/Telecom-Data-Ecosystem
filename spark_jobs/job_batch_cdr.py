"""
PySpark Batch Job — CDR Processing
Đọc raw CSV từ HDFS → clean → aggregate daily metrics → ghi Parquet.

Usage:
    spark-submit --master spark://spark-master:7077 --deploy-mode client \\
        spark_jobs/job_batch_cdr.py 2024-06-17
"""
import sys
from typing import Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder.appName("CDR_Batch_Processing")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
        .getOrCreate()
    )


def read_raw_cdr(spark: SparkSession, date_partition: str) -> DataFrame:
    """Đọc CDR từ HDFS raw_zone theo partition ngày (YYYY-MM-DD)."""
    hdfs_url = "hdfs://namenode:9000"
    path = f"{hdfs_url}/data/raw_zone/cdr/{date_partition}/"
    df = spark.read.csv(path, header=True, inferSchema=True)
    print(f"[READ] Raw CDR from {path}: {df.count():,} rows")
    return df


def clean_cdr(df: DataFrame) -> DataFrame:
    """
    Làm sạch CDR — xử lý các lỗi được tạo có chủ đích:
      1. Loại bỏ null caller_id
      2. Parse timestamp từ 2 format (ISO và dd/MM/yyyy)
      3. Loại bỏ duration <= 0
      4. Dedup theo record_id
    """
    total = df.count()

    df_no_null = df.filter(F.col("caller_id").isNotNull())
    after_null = df_no_null.count()

    df_ts = df_no_null.withColumn(
        "call_timestamp_clean",
        F.coalesce(
            F.to_timestamp("call_timestamp", "yyyy-MM-dd'T'HH:mm:ss"),
            F.to_timestamp("call_timestamp", "dd/MM/yyyy HH:mm:ss"),
        ),
    ).filter(F.col("call_timestamp_clean").isNotNull())
    after_ts = df_ts.count()

    df_valid_dur = df_ts.filter(F.col("duration_seconds") > 0)
    after_dur = df_valid_dur.count()

    df_dedup = df_valid_dur.dropDuplicates(["record_id"])
    after_dedup = df_dedup.count()

    print(
        f"[CLEAN] "
        f"raw={total:,} → "
        f"after_null={after_null:,} (-{total - after_null:,}) → "
        f"after_ts={after_ts:,} (-{after_null - after_ts:,}) → "
        f"after_dur={after_dur:,} (-{after_ts - after_dur:,}) → "
        f"after_dedup={after_dedup:,} (-{after_dur - after_dedup:,})"
    )

    return df_dedup


def aggregate_daily_usage(df_clean: DataFrame) -> DataFrame:
    """Tổng hợp usage theo (caller_id, call_date) với các metrics tổng hợp."""
    df_with_date = df_clean.withColumn(
        "call_date", F.to_date("call_timestamp_clean")
    )

    daily_agg = df_with_date.groupBy("caller_id", "call_date").agg(
        F.count("*").alias("total_calls"),
        F.sum("duration_seconds").alias("total_duration_seconds"),
        F.avg("duration_seconds").alias("avg_duration_seconds"),
        F.countDistinct("callee_id").alias("unique_callee_count"),
        F.sum(F.when(F.col("call_type") == "voice", 1).otherwise(0)).alias("voice_calls"),
        F.sum(F.when(F.col("call_type") == "video", 1).otherwise(0)).alias("video_calls"),
        F.countDistinct("tower_id").alias("towers_used"),
    ).withColumn(
        "avg_duration_minutes",
        F.round(F.col("avg_duration_seconds") / 60, 2),
    )

    print(f"[AGGREGATE] Output: {daily_agg.count():,} rows (subscriber × date)")
    return daily_agg


def write_to_processed_zone(df: DataFrame, date_partition: str) -> None:
    """
    Ghi kết quả về HDFS processed_zone dạng Parquet.
    Parquet: nén tốt hơn CSV, columnar reads, schema preservation.
    """
    hdfs_url = "hdfs://namenode:9000"
    output_path = f"{hdfs_url}/data/processed_zone/daily_cdr_agg/{date_partition}/"
    df.coalesce(1).write.mode("overwrite").parquet(output_path)
    print(f"[WRITE] Written to: {output_path}")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: job_batch_cdr.py <date_partition>  (e.g. 2024-06-17)")
        sys.exit(1)

    date_partition = sys.argv[1]
    print(f"[START] Processing CDR for partition: {date_partition}")

    spark = create_spark_session()

    try:
        raw_df = read_raw_cdr(spark, date_partition)
        clean_df = clean_cdr(raw_df)
        agg_df = aggregate_daily_usage(clean_df)
        write_to_processed_zone(agg_df, date_partition)
        print("[DONE] CDR batch job completed successfully.")
    finally:
        spark.stop()
