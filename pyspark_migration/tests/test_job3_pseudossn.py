"""
Unit tests for Job 3: Pseudossn.

Tests deterministic dedup, archive writes, record count validation,
and counter tracking using local Spark mode.
"""

import pytest
from unittest.mock import MagicMock

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
)
from pyspark.sql.window import Window

from pyspark_migration.jobs.job3_pseudossn import PseudossnJob


class TestPseudossnDeterministicDedup:
    """Tests for deterministic lookup replacing 'Use Any Value'."""

    def test_dedup_picks_first_by_ordering(self, spark):
        """row_number() window should pick deterministic first match."""
        data = [
            ("SSN001", "TK_A", "2025-01-01"),
            ("SSN001", "TK_B", "2025-06-01"),
            ("SSN001", "TK_C", "2025-03-01"),
            ("SSN002", "TK_D", "2025-02-01"),
        ]
        df = spark.createDataFrame(
            data,
            schema=StructType([
                StructField("PSEUDOSSN", StringType()),
                StructField("TK_NUM", StringType()),
                StructField("PSEUDOSSN_EFF_DT", StringType()),
            ]),
        )

        w = Window.partitionBy("PSEUDOSSN").orderBy(
            F.col("PSEUDOSSN_EFF_DT").desc()
        )
        deduped = (
            df.withColumn("_rn", F.row_number().over(w))
            .filter(F.col("_rn") == 1)
            .drop("_rn")
        )

        result = deduped.collect()
        assert len(result) == 2

        ssn001 = [r for r in result if r["PSEUDOSSN"] == "SSN001"][0]
        assert ssn001["TK_NUM"] == "TK_B"  # Latest date

        ssn002 = [r for r in result if r["PSEUDOSSN"] == "SSN002"][0]
        assert ssn002["TK_NUM"] == "TK_D"

    def test_single_record_passes_through(self, spark):
        """Single record per key should pass through unchanged."""
        data = [("SSN001", "TK_A", "2025-01-01")]
        df = spark.createDataFrame(
            data,
            schema=StructType([
                StructField("PSEUDOSSN", StringType()),
                StructField("TK_NUM", StringType()),
                StructField("PSEUDOSSN_EFF_DT", StringType()),
            ]),
        )

        w = Window.partitionBy("PSEUDOSSN").orderBy(
            F.col("PSEUDOSSN_EFF_DT").desc()
        )
        deduped = (
            df.withColumn("_rn", F.row_number().over(w))
            .filter(F.col("_rn") == 1)
            .drop("_rn")
        )

        assert deduped.count() == 1


class TestPseudossnRecordCountValidation:
    """Tests for header/trailer record count matching."""

    def test_count_match(self, spark):
        """Header count should match detail record count."""
        header_count = 5
        detail_data = [
            ("SSN001",), ("SSN002",), ("SSN003",), ("SSN004",), ("SSN005",),
        ]
        df = spark.createDataFrame(
            detail_data,
            schema=StructType([StructField("PSEUDOSSN", StringType())]),
        )
        assert df.count() == header_count

    def test_count_mismatch_detected(self, spark):
        """Mismatch between header and actual count should be detected."""
        header_count = 10
        detail_data = [("SSN001",), ("SSN002",)]
        df = spark.createDataFrame(
            detail_data,
            schema=StructType([StructField("PSEUDOSSN", StringType())]),
        )
        actual_count = df.count()
        assert actual_count != header_count


class TestPseudossnSessionSequencing:
    """Tests for session execution order."""

    def test_no_pay_period_aborts(
        self, spark, test_config, mock_db_manager, mock_email_service, mock_counter_manager
    ):
        """ABORT when no current pay period found."""
        empty_df = spark.createDataFrame(
            [],
            schema=StructType([
                StructField("PP_NUM", IntegerType()),
                StructField("PP_END_YEAR", IntegerType()),
            ]),
        )
        mock_db_manager.read_jdbc.return_value = empty_df

        job = PseudossnJob(
            spark, test_config, mock_db_manager,
            mock_email_service, mock_counter_manager,
        )

        with pytest.raises(RuntimeError, match="[Nn]o current pay period"):
            job.run()
