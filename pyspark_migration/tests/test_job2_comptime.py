"""
Unit tests for Job 2: COMPTIME.

Tests flat file schema, SSN validation, date conversion,
counter writes, and error handling using local Spark mode.
"""

import pytest
from datetime import datetime
from unittest.mock import MagicMock

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, LongType,
)

from pyspark_migration.jobs.job2_comptime import CompTimeJob, U0287D01_SCHEMA


class TestCompTimeSchema:
    """Tests for COMP_TIME flat file schema definition."""

    def test_schema_field_count(self):
        """Verify schema has correct number of fields for headerless file."""
        assert len(U0287D01_SCHEMA.fields) == 12

    def test_schema_field_names(self):
        """Verify key field names exist."""
        field_names = [f.name for f in U0287D01_SCHEMA.fields]
        assert "SSN" in field_names
        assert "PP_END_DATE" in field_names
        assert "COMP_TIME_HOURS" in field_names

    def test_all_string_types(self):
        """Flat file fields should all be StringType before conversion."""
        for field in U0287D01_SCHEMA.fields:
            assert isinstance(field.dataType, StringType)


class TestCompTimeSSNValidation:
    """Tests for SSN IS_NUMBER filter replacement."""

    def test_valid_ssn_passes(self, spark):
        """Valid numeric SSN should pass filter."""
        df = spark.createDataFrame(
            [("123456789",), ("987654321",)],
            schema=StructType([StructField("SSN", StringType())]),
        )
        # Replicate the IS_NUMBER filter
        filtered = df.filter(F.col("SSN").rlike("^[0-9]{9}$"))
        assert filtered.count() == 2

    def test_invalid_ssn_filtered(self, spark):
        """Non-numeric SSN should be filtered out."""
        df = spark.createDataFrame(
            [("12345678A",), ("ABCDEFGHI",), ("12345",), ("",)],
            schema=StructType([StructField("SSN", StringType())]),
        )
        filtered = df.filter(F.col("SSN").rlike("^[0-9]{9}$"))
        assert filtered.count() == 0

    def test_mixed_ssns(self, spark):
        """Mix of valid and invalid SSNs."""
        df = spark.createDataFrame(
            [("123456789",), ("ABCDEFGHI",), ("987654321",), ("12345",)],
            schema=StructType([StructField("SSN", StringType())]),
        )
        filtered = df.filter(F.col("SSN").rlike("^[0-9]{9}$"))
        assert filtered.count() == 2


class TestCompTimeDateConversion:
    """Tests for YYYYMMDD date format conversion."""

    def test_valid_date(self, spark):
        """Valid YYYYMMDD should parse correctly."""
        df = spark.createDataFrame(
            [("20250615",)],
            schema=StructType([StructField("PP_END_DATE", StringType())]),
        )
        result = df.withColumn(
            "parsed_date",
            F.to_date(F.col("PP_END_DATE"), "yyyyMMdd"),
        )
        row = result.collect()[0]
        assert row["parsed_date"] is not None
        assert str(row["parsed_date"]) == "2025-06-15"

    def test_invalid_date(self, spark):
        """Invalid date should produce null."""
        df = spark.createDataFrame(
            [("99999999",), ("BADDATE",)],
            schema=StructType([StructField("PP_END_DATE", StringType())]),
        )
        # Use try_to_date for Spark 4 compatibility (strict ANSI mode)
        result = df.withColumn(
            "parsed_date",
            F.try_to_timestamp(F.col("PP_END_DATE"), F.lit("yyyyMMdd")).cast("date"),
        )
        rows = result.collect()
        for row in rows:
            assert row["parsed_date"] is None


class TestCompTimeSessionSequencing:
    """Tests for COMPTIME session order and failures."""

    def test_no_current_pay_period_aborts(
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

        job = CompTimeJob(
            spark, test_config, mock_db_manager,
            mock_email_service, mock_counter_manager,
        )

        with pytest.raises(RuntimeError, match="[Nn]o current pay period"):
            job.run(source_file_path="/tmp/test_comptime.csv")
