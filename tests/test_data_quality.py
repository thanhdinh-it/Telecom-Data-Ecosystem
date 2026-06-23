"""
Unit Tests — Data Quality Checker
Test cho DataQualityChecker class.

Run: pytest tests/test_data_quality.py -v
"""
import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    return (
        SparkSession.builder.master("local[2]")
        .appName("test_data_quality")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )


def test_check_not_null_passes_when_no_nulls(spark: SparkSession) -> None:
    """check_not_null phải pass khi không có null."""
    from data_quality.dq_checks import DataQualityChecker

    df = spark.createDataFrame([("A",), ("B",), ("C",)], ["col1"])
    checker = DataQualityChecker(df, "test_table")
    checker.check_not_null("col1")

    assert checker.results[0]["passed"] is True
    assert checker.results[0]["null_count"] == 0


def test_check_not_null_fails_when_nulls_exceed_threshold(spark: SparkSession) -> None:
    """check_not_null phải fail khi null_pct > threshold."""
    from data_quality.dq_checks import DataQualityChecker

    df = spark.createDataFrame([("A",), (None,), (None,)], ["col1"])
    checker = DataQualityChecker(df, "test_table")
    checker.check_not_null("col1", threshold_pct=0.0)

    assert checker.results[0]["passed"] is False


def test_check_not_null_passes_within_threshold(spark: SparkSession) -> None:
    """check_not_null phải pass khi null_pct <= threshold."""
    from data_quality.dq_checks import DataQualityChecker

    rows = [("A",)] * 9 + [(None,)]  # 10% null — threshold 10% → pass
    df = spark.createDataFrame(rows, ["col1"])
    checker = DataQualityChecker(df, "test_table")
    checker.check_not_null("col1", threshold_pct=10.0)

    assert checker.results[0]["passed"] is True


def test_check_no_duplicates_passes_unique(spark: SparkSession) -> None:
    """check_no_duplicates phải pass khi không có duplicate."""
    from data_quality.dq_checks import DataQualityChecker

    df = spark.createDataFrame([("A", 1), ("B", 2), ("C", 3)], ["id", "val"])
    checker = DataQualityChecker(df, "test_table")
    checker.check_no_duplicates(["id"])

    assert checker.results[0]["passed"] is True
    assert checker.results[0]["duplicate_count"] == 0


def test_check_no_duplicates_fails_with_duplicates(spark: SparkSession) -> None:
    """check_no_duplicates phải fail khi có duplicate."""
    from data_quality.dq_checks import DataQualityChecker

    df = spark.createDataFrame([("A", 1), ("A", 2), ("B", 3)], ["id", "val"])
    checker = DataQualityChecker(df, "test_table")
    checker.check_no_duplicates(["id"])

    assert checker.results[0]["passed"] is False
    assert checker.results[0]["duplicate_count"] == 1


def test_check_value_range_passes_all_in_range(spark: SparkSession) -> None:
    """check_value_range phải pass khi tất cả giá trị trong range."""
    from data_quality.dq_checks import DataQualityChecker

    df = spark.createDataFrame([(1,), (5,), (10,)], ["num"])
    checker = DataQualityChecker(df, "test_table")
    checker.check_value_range("num", min_val=1, max_val=10)

    assert checker.results[0]["passed"] is True
    assert checker.results[0]["out_of_range_count"] == 0


def test_check_value_range_fails_with_outliers(spark: SparkSession) -> None:
    """check_value_range phải fail khi có giá trị ngoài range."""
    from data_quality.dq_checks import DataQualityChecker

    df = spark.createDataFrame([(1,), (-5,), (10,), (999,)], ["num"])
    checker = DataQualityChecker(df, "test_table")
    checker.check_value_range("num", min_val=0, max_val=100)

    assert checker.results[0]["passed"] is False
    assert checker.results[0]["out_of_range_count"] == 2


def test_check_accepted_values_passes(spark: SparkSession) -> None:
    """check_accepted_values phải pass khi tất cả giá trị trong danh sách."""
    from data_quality.dq_checks import DataQualityChecker

    df = spark.createDataFrame([("basic",), ("standard",), ("premium",)], ["plan_type"])
    checker = DataQualityChecker(df, "test_table")
    checker.check_accepted_values("plan_type", ["basic", "standard", "premium"])

    assert checker.results[0]["passed"] is True


def test_check_accepted_values_fails_with_invalid(spark: SparkSession) -> None:
    """check_accepted_values phải fail khi có giá trị không trong danh sách."""
    from data_quality.dq_checks import DataQualityChecker

    df = spark.createDataFrame([("basic",), ("enterprise",), ("premium",)], ["plan_type"])
    checker = DataQualityChecker(df, "test_table")
    checker.check_accepted_values("plan_type", ["basic", "standard", "premium"])

    assert checker.results[0]["passed"] is False
    assert checker.results[0]["invalid_count"] == 1


def test_check_row_count_passes(spark: SparkSession) -> None:
    """check_row_count phải pass khi có đủ rows."""
    from data_quality.dq_checks import DataQualityChecker

    df = spark.createDataFrame([(i,) for i in range(100)], ["id"])
    checker = DataQualityChecker(df, "test_table")
    checker.check_row_count(min_rows=50)

    assert checker.results[0]["passed"] is True


def test_check_row_count_fails_empty(spark: SparkSession) -> None:
    """check_row_count phải fail khi DataFrame rỗng."""
    from data_quality.dq_checks import DataQualityChecker

    df = spark.createDataFrame([], spark.createDataFrame([(1,)], ["id"]).schema)
    checker = DataQualityChecker(df, "test_table")
    checker.check_row_count(min_rows=1)

    assert checker.results[0]["passed"] is False
    assert checker.results[0]["actual_count"] == 0


def test_assert_all_pass_raises_on_failure(spark: SparkSession) -> None:
    """assert_all_pass phải raise AssertionError khi có check fail."""
    from data_quality.dq_checks import DataQualityChecker

    df = spark.createDataFrame([(None,), ("B",)], ["col1"])
    checker = DataQualityChecker(df, "test_table")
    checker.check_not_null("col1")

    with pytest.raises(AssertionError, match="Data quality FAILED"):
        checker.assert_all_pass()


def test_assert_all_pass_succeeds_when_all_pass(spark: SparkSession) -> None:
    """assert_all_pass phải không raise khi tất cả checks pass."""
    from data_quality.dq_checks import DataQualityChecker

    df = spark.createDataFrame([("A", 1), ("B", 2)], ["col1", "col2"])
    checker = DataQualityChecker(df, "test_table")
    checker.check_not_null("col1").check_row_count(min_rows=1)

    checker.assert_all_pass()
