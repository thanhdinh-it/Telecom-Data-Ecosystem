"""
Unit Tests — PySpark Batch CDR Job
Test cho clean_cdr() và aggregate_daily_usage().

Run: pytest tests/test_batch_cdr.py -v
"""
import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    """SparkSession local, dùng chung cho tất cả tests trong file."""
    return (
        SparkSession.builder.master("local[2]")
        .appName("test_batch_cdr")
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )


CDR_COLUMNS = [
    "record_id", "caller_id", "callee_id",
    "call_timestamp", "duration_seconds", "call_type", "tower_id",
]


def make_cdr_df(spark: SparkSession, rows: list) -> "DataFrame":
    """Tạo CDR DataFrame từ list of tuples với schema chuẩn."""
    return spark.createDataFrame(rows, CDR_COLUMNS)


def test_clean_cdr_removes_null_caller_id(spark: SparkSession) -> None:
    """Dòng có caller_id null phải bị loại bỏ."""
    from spark_jobs.job_batch_cdr import clean_cdr

    rows = [
        ("uuid-1", "VT000001", "VT000002", "2024-06-17T10:00:00", 300, "voice", "TOWER_0001"),
        ("uuid-2", None,       "VT000003", "2024-06-17T11:00:00", 200, "voice", "TOWER_0002"),
    ]
    df = make_cdr_df(spark, rows)
    result = clean_cdr(df)

    assert result.count() == 1
    assert result.filter(F.col("caller_id").isNull()).count() == 0


def test_clean_cdr_removes_negative_duration(spark: SparkSession) -> None:
    """Cuộc gọi có duration_seconds âm hoặc bằng 0 phải bị loại bỏ."""
    from spark_jobs.job_batch_cdr import clean_cdr

    rows = [
        ("uuid-1", "VT000001", "VT000002", "2024-06-17T10:00:00",  300, "voice", "TOWER_0001"),
        ("uuid-2", "VT000003", "VT000004", "2024-06-17T11:00:00", -100, "voice", "TOWER_0002"),
        ("uuid-3", "VT000005", "VT000006", "2024-06-17T12:00:00",    0, "voice", "TOWER_0003"),
    ]
    df = make_cdr_df(spark, rows)
    result = clean_cdr(df)

    assert result.count() == 1
    assert result.filter(F.col("duration_seconds") <= 0).count() == 0


def test_clean_cdr_handles_mixed_timestamp_format(spark: SparkSession) -> None:
    """Cả ISO format và dd/MM/yyyy đều phải được parse, format sai bị loại."""
    from spark_jobs.job_batch_cdr import clean_cdr

    rows = [
        ("uuid-1", "VT000001", "VT000002", "2024-06-17T10:00:00",  300, "voice", "TOWER_0001"),
        ("uuid-2", "VT000003", "VT000004", "17/06/2024 11:00:00",  200, "voice", "TOWER_0002"),
        ("uuid-3", "VT000005", "VT000006", "not-a-date",           100, "voice", "TOWER_0003"),
    ]
    df = make_cdr_df(spark, rows)
    result = clean_cdr(df)

    assert result.count() == 2


def test_clean_cdr_removes_duplicates(spark: SparkSession) -> None:
    """Duplicate record_id chỉ giữ lại 1 bản."""
    from spark_jobs.job_batch_cdr import clean_cdr

    rows = [
        ("uuid-dup", "VT000001", "VT000002", "2024-06-17T10:00:00", 300, "voice", "TOWER_0001"),
        ("uuid-dup", "VT000001", "VT000002", "2024-06-17T10:00:00", 300, "voice", "TOWER_0001"),
        ("uuid-001", "VT000003", "VT000004", "2024-06-17T11:00:00", 200, "voice", "TOWER_0002"),
    ]
    df = make_cdr_df(spark, rows)
    result = clean_cdr(df)

    assert result.count() == 2


def test_clean_cdr_keeps_all_valid_rows(spark: SparkSession) -> None:
    """Dữ liệu hoàn toàn sạch phải không bị loại dòng nào."""
    from spark_jobs.job_batch_cdr import clean_cdr

    rows = [
        ("uuid-1", "VT000001", "VT000002", "2024-06-17T10:00:00", 300, "voice", "TOWER_0001"),
        ("uuid-2", "VT000003", "VT000004", "2024-06-17T11:00:00", 200, "video", "TOWER_0002"),
        ("uuid-3", "VT000005", "VT000006", "2024-06-17T12:00:00", 100, "data",  "TOWER_0003"),
    ]
    df = make_cdr_df(spark, rows)
    result = clean_cdr(df)

    assert result.count() == 3


AGG_COLUMNS = ["caller_id", "callee_id", "call_timestamp_clean", "duration_seconds", "call_type", "tower_id"]


def test_aggregate_daily_usage_correct_metrics(spark: SparkSession) -> None:
    """Aggregation phải tính đúng total_calls và total_duration_seconds."""
    from spark_jobs.job_batch_cdr import aggregate_daily_usage

    rows = [
        ("VT000001", "VT000002", "2024-06-17T10:00:00", 300, "voice", "TOWER_0001"),
        ("VT000001", "VT000003", "2024-06-17T14:00:00", 600, "video", "TOWER_0002"),
    ]
    df = spark.createDataFrame(rows, AGG_COLUMNS).withColumn(
        "call_timestamp_clean", F.to_timestamp("call_timestamp_clean")
    )

    result = aggregate_daily_usage(df)
    row = result.collect()[0]

    assert row["total_calls"] == 2
    assert row["total_duration_seconds"] == 900
    assert row["unique_callee_count"] == 2


def test_aggregate_daily_usage_splits_by_day(spark: SparkSession) -> None:
    """Cùng caller nhưng 2 ngày khác nhau → 2 dòng aggregate riêng biệt."""
    from spark_jobs.job_batch_cdr import aggregate_daily_usage

    rows = [
        ("VT000001", "VT000002", "2024-06-17T10:00:00", 300, "voice", "TOWER_0001"),
        ("VT000001", "VT000002", "2024-06-18T10:00:00", 300, "voice", "TOWER_0001"),
    ]
    df = spark.createDataFrame(rows, AGG_COLUMNS).withColumn(
        "call_timestamp_clean", F.to_timestamp("call_timestamp_clean")
    )

    result = aggregate_daily_usage(df)

    assert result.count() == 2


def test_aggregate_daily_usage_voice_video_split(spark: SparkSession) -> None:
    """voice_calls và video_calls phải được đếm riêng."""
    from spark_jobs.job_batch_cdr import aggregate_daily_usage

    rows = [
        ("VT000001", "VT000002", "2024-06-17T10:00:00", 100, "voice", "TOWER_0001"),
        ("VT000001", "VT000003", "2024-06-17T11:00:00", 200, "voice", "TOWER_0002"),
        ("VT000001", "VT000004", "2024-06-17T12:00:00", 300, "video", "TOWER_0003"),
    ]
    df = spark.createDataFrame(rows, AGG_COLUMNS).withColumn(
        "call_timestamp_clean", F.to_timestamp("call_timestamp_clean")
    )

    result = aggregate_daily_usage(df)
    row = result.collect()[0]

    assert row["voice_calls"] == 2
    assert row["video_calls"] == 1
    assert row["total_calls"] == 3
