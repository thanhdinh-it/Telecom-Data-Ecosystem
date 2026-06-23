"""
Data Quality Checker — Telecom Data Ecosystem
Reusable validation class cho tất cả layers của pipeline.

Usage:
    checker = DataQualityChecker(spark_df, "daily_cdr_agg")
    checker.check_not_null("caller_id") \\
           .check_no_duplicates(["caller_id", "call_date"]) \\
           .check_value_range("total_calls", 1, 10_000) \\
           .assert_all_pass()
"""
import json
from datetime import datetime
from typing import Any, List, Optional

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


class DataQualityChecker:
    """
    Kiểm tra data quality cho PySpark DataFrames.
    Mỗi check ghi kết quả vào self.results. Dùng assert_all_pass() để block nếu có fail.
    """

    def __init__(self, df: DataFrame, table_name: str) -> None:
        self.df = df
        self.table_name = table_name
        self.results: List[dict] = []
        self._total_count: Optional[int] = None

    def _get_total(self) -> int:
        if self._total_count is None:
            self._total_count = self.df.count()
        return self._total_count

    def check_not_null(self, column: str, threshold_pct: float = 0.0) -> "DataQualityChecker":
        """Kiểm tra tỷ lệ null của column không vượt quá threshold_pct (%)."""
        total = self._get_total()
        null_count = self.df.filter(F.col(column).isNull()).count()
        null_pct = (null_count / total * 100) if total > 0 else 0.0

        passed = null_pct <= threshold_pct
        self.results.append({
            "check": "not_null",
            "column": column,
            "passed": passed,
            "null_count": null_count,
            "null_pct": round(null_pct, 2),
            "threshold_pct": threshold_pct,
        })
        return self

    def check_no_duplicates(self, subset: List[str]) -> "DataQualityChecker":
        """Kiểm tra không có duplicate theo tập hợp cột subset."""
        total = self._get_total()
        distinct = self.df.dropDuplicates(subset).count()
        duplicate_count = total - distinct
        passed = duplicate_count == 0

        self.results.append({
            "check": "no_duplicates",
            "columns": subset,
            "passed": passed,
            "duplicate_count": duplicate_count,
        })
        return self

    def check_value_range(self, column: str, min_val: Any, max_val: Any) -> "DataQualityChecker":
        """Kiểm tra tất cả giá trị trong column nằm trong [min_val, max_val]."""
        out_of_range = self.df.filter(
            (F.col(column) < min_val) | (F.col(column) > max_val)
        ).count()
        passed = out_of_range == 0

        self.results.append({
            "check": "value_range",
            "column": column,
            "passed": passed,
            "out_of_range_count": out_of_range,
            "expected_range": [min_val, max_val],
        })
        return self

    def check_accepted_values(self, column: str, accepted: List[Any]) -> "DataQualityChecker":
        """Kiểm tra giá trị trong column chỉ thuộc tập hợp accepted."""
        invalid_count = self.df.filter(~F.col(column).isin(accepted)).count()
        passed = invalid_count == 0

        self.results.append({
            "check": "accepted_values",
            "column": column,
            "passed": passed,
            "invalid_count": invalid_count,
            "accepted_values": accepted,
        })
        return self

    def check_row_count(self, min_rows: int) -> "DataQualityChecker":
        """Kiểm tra output pipeline không rỗng hoặc quá ít dòng."""
        count = self._get_total()
        passed = count >= min_rows

        self.results.append({
            "check": "row_count",
            "passed": passed,
            "actual_count": count,
            "min_expected": min_rows,
        })
        return self

    def check_referential_integrity(
        self,
        column: str,
        reference_df: DataFrame,
        reference_column: str,
    ) -> "DataQualityChecker":
        """Kiểm tra tất cả giá trị trong column tồn tại trong reference_df."""
        orphan_count = (
            self.df.join(
                reference_df.select(reference_column).distinct(),
                self.df[column] == reference_df[reference_column],
                "left_anti",
            ).count()
        )
        passed = orphan_count == 0

        self.results.append({
            "check": "referential_integrity",
            "column": column,
            "reference_column": reference_column,
            "passed": passed,
            "orphan_count": orphan_count,
        })
        return self

    def assert_all_pass(self) -> "DataQualityChecker":
        """In report và raise AssertionError nếu bất kỳ check nào fail."""
        failed = [r for r in self.results if not r["passed"]]

        report = {
            "table": self.table_name,
            "timestamp": datetime.now().isoformat(),
            "total_checks": len(self.results),
            "passed": len(self.results) - len(failed),
            "failed": len(failed),
            "details": self.results,
        }
        print(json.dumps(report, indent=2, default=str))

        if failed:
            raise AssertionError(
                f"Data quality FAILED for '{self.table_name}': "
                f"{len(failed)}/{len(self.results)} checks failed. See log above."
            )

        print(f"✓ All {len(self.results)} quality checks passed for '{self.table_name}'")
        return self

    def get_report(self) -> dict:
        """Trả về report dưới dạng dict (không raise exception)."""
        failed = [r for r in self.results if not r["passed"]]
        return {
            "table": self.table_name,
            "timestamp": datetime.now().isoformat(),
            "total_checks": len(self.results),
            "passed_count": len(self.results) - len(failed),
            "failed_count": len(failed),
            "all_passed": len(failed) == 0,
            "details": self.results,
        }


def run_cdr_quality_checks(df_processed: DataFrame) -> None:
    """Chạy tất cả DQ checks cho bảng daily_cdr_agg sau khi Spark job."""
    DataQualityChecker(df_processed, "daily_cdr_agg") \
        .check_not_null("caller_id") \
        .check_not_null("call_date") \
        .check_not_null("total_calls") \
        .check_no_duplicates(["caller_id", "call_date"]) \
        .check_value_range("total_calls", min_val=1, max_val=10_000) \
        .check_value_range("total_duration_seconds", min_val=0, max_val=86_400) \
        .check_row_count(min_rows=100) \
        .assert_all_pass()


def run_customers_quality_checks(
    df_customers: DataFrame, threshold_null_phone: float = 6.0
) -> None:
    """Chạy DQ checks cho bảng customers. Cho phép ~6% null phone."""
    DataQualityChecker(df_customers, "customers") \
        .check_not_null("customer_id") \
        .check_no_duplicates(["customer_id"]) \
        .check_not_null("phone_number", threshold_pct=threshold_null_phone) \
        .check_accepted_values("plan_type", ["basic", "standard", "premium"]) \
        .check_row_count(min_rows=1_000) \
        .assert_all_pass()
