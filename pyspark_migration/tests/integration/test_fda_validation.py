"""
Integration Tests for FDA Leave Validation Job

Tests validation against CPM staging tables.
"""

from unittest.mock import patch

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, StructField, StructType


@pytest.fixture(scope="module")
def spark():
    """Create a local SparkSession for testing."""
    session = (
        SparkSession.builder
        .master("local[1]")
        .appName("test_fda_validation")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


class TestFDAValidation:
    """Tests for FDA leave validation logic."""

    def test_validation_checks_configured(self):
        """All 4 validation checks are configured."""
        from pyspark_migration.jobs.fda_validation.fda_validation_job import (
            VALIDATION_CHECKS,
        )

        assert len(VALIDATION_CHECKS) == 4

        table_names = [c["table"] for c in VALIDATION_CHECKS]
        assert "CPM_YTD_DETAIL_STG_TBL" in table_names
        assert "CPM_PAD_DETAIL_STG_TBL" in table_names
        assert "CPM_MER_DETAIL_STG_TBL" in table_names
        assert "CPM_NEWPAY_TBL" in table_names

    def test_unmatched_records_detected(self, spark):
        """Left join detects unmatched FDA records."""
        fda_data = [("111111111",), ("222222222",), ("333333333",)]
        fda_df = spark.createDataFrame(
            fda_data,
            StructType([StructField("DFAS_PSEUDO_SSN", StringType())]),
        )

        staging_data = [("111111111",), ("333333333",)]
        staging_df = spark.createDataFrame(
            staging_data,
            StructType([StructField("DFAS_PSEUDO_SSN", StringType())]),
        )

        joined = fda_df.alias("fda").join(
            staging_df.alias("stg"),
            on=F.col("fda.DFAS_PSEUDO_SSN") == F.col("stg.DFAS_PSEUDO_SSN"),
            how="left",
        )
        unmatched = joined.filter(F.col("stg.DFAS_PSEUDO_SSN").isNull())

        assert unmatched.count() == 1

    def test_all_matched_no_errors(self, spark):
        """When all records match, no errors are generated."""
        fda_data = [("111111111",), ("222222222",)]
        fda_df = spark.createDataFrame(
            fda_data,
            StructType([StructField("DFAS_PSEUDO_SSN", StringType())]),
        )

        staging_data = [("111111111",), ("222222222",)]
        staging_df = spark.createDataFrame(
            staging_data,
            StructType([StructField("DFAS_PSEUDO_SSN", StringType())]),
        )

        joined = fda_df.alias("fda").join(
            staging_df.alias("stg"),
            on=F.col("fda.DFAS_PSEUDO_SSN") == F.col("stg.DFAS_PSEUDO_SSN"),
            how="left",
        )
        unmatched = joined.filter(F.col("stg.DFAS_PSEUDO_SSN").isNull())

        assert unmatched.count() == 0

    def test_error_messages_correct(self):
        """Error messages match expected patterns."""
        from pyspark_migration.jobs.fda_validation.fda_validation_job import (
            VALIDATION_CHECKS,
        )

        ytd_check = next(c for c in VALIDATION_CHECKS if c["table"] == "CPM_YTD_DETAIL_STG_TBL")
        assert "YTD RECORD" in ytd_check["error_message"]
        assert "P6791X01" in ytd_check["error_message"]

        mer_check = next(c for c in VALIDATION_CHECKS if c["table"] == "CPM_MER_DETAIL_STG_TBL")
        assert "MER RECORD" in mer_check["error_message"]
