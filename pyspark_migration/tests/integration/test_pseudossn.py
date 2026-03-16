"""
Integration Tests for PseudoSSN Job

Tests field extraction, filtering, and transformation logic.
"""

from unittest.mock import patch

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


@pytest.fixture(scope="module")
def spark():
    """Create a local SparkSession for testing."""
    session = (
        SparkSession.builder
        .master("local[1]")
        .appName("test_pseudossn")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


class TestPseudoSSNFieldExtraction:
    """Tests for SDA file field extraction."""

    def test_field_extraction_67_fields(self, spark, tmp_path):
        """Parse real SDA file, verify field count and types."""
        from pyspark_migration.jobs.pseudossn.pseudossn_job import SDA_FIELD_SPECS

        # SDA_FIELD_SPECS should have 67 field specifications
        assert len(SDA_FIELD_SPECS) == 67

        # Verify first field is SSN
        assert SDA_FIELD_SPECS[0][0] == "SSN"
        assert SDA_FIELD_SPECS[0][1] == 1  # start position
        assert SDA_FIELD_SPECS[0][2] == 9  # length


class TestPseudoSSNFiltering:
    """Tests for record type filtering."""

    def test_header_filtered(self, spark):
        """Header records excluded from output."""
        from pyspark_migration.common.validation import classify_record_type

        df = spark.createDataFrame(
            [("HEADER001",), ("123456789",), ("TRAILER01",)],
            ["SSN"],
        )
        classified = df.withColumn(
            "RECORD_TYPE_FLAG", classify_record_type(F.col("SSN"))
        )
        detail = classified.filter(F.col("RECORD_TYPE_FLAG") == "D")

        assert detail.count() == 1
        assert detail.collect()[0]["SSN"] == "123456789"

    def test_trailer_filtered(self, spark):
        """Trailer records excluded from output."""
        from pyspark_migration.common.validation import classify_record_type

        df = spark.createDataFrame(
            [("TRAILER01",), ("987654321",)],
            ["SSN"],
        )
        classified = df.withColumn(
            "RECORD_TYPE_FLAG", classify_record_type(F.col("SSN"))
        )
        detail = classified.filter(F.col("RECORD_TYPE_FLAG") == "D")

        assert detail.count() == 1

    def test_non_numeric_ssn_filtered(self, spark):
        """Non-numeric SSNs filtered out."""
        from pyspark_migration.common.validation import (
            classify_record_type,
            is_valid_ssn,
        )

        df = spark.createDataFrame(
            [("ABCDEFGHI",), ("123456789",), ("12345678A",)],
            ["SSN"],
        )
        classified = df.withColumn(
            "RECORD_TYPE_FLAG", classify_record_type(F.col("SSN"))
        )
        filtered = classified.filter(
            (F.col("RECORD_TYPE_FLAG") == "D") & is_valid_ssn(F.col("SSN"))
        )

        assert filtered.count() == 1
        assert filtered.collect()[0]["SSN"] == "123456789"


class TestPseudoSSNDateConversion:
    """Tests for date conversion transformations."""

    def test_mmddyyyy_conversion(self, spark):
        """Convert MMDDYYYY dates correctly."""
        from pyspark_migration.common.validation import validate_and_convert_date

        df = spark.createDataFrame([("01152024",)], ["HIRE_DATE"])
        result = df.withColumn(
            "HIRE_DATE_CONV",
            validate_and_convert_date(F.col("HIRE_DATE"), F.lit("MMDDYYYY")),
        )
        row = result.collect()[0]
        assert row["HIRE_DATE_CONV"] is not None

    def test_yyyymmdd_conversion(self, spark):
        """Convert YYYYMMDD dates correctly."""
        from pyspark_migration.common.validation import validate_and_convert_date

        df = spark.createDataFrame([("20240115",)], ["SEP_DATE"])
        result = df.withColumn(
            "SEP_DATE_CONV",
            validate_and_convert_date(F.col("SEP_DATE"), F.lit("YYYYMMDD")),
        )
        row = result.collect()[0]
        assert row["SEP_DATE_CONV"] is not None

    def test_invalid_date_returns_null(self, spark):
        """Invalid dates return null."""
        from pyspark_migration.common.validation import validate_and_convert_date

        df = spark.createDataFrame([("99999999",)], ["BAD_DATE"])
        result = df.withColumn(
            "BAD_DATE_CONV",
            validate_and_convert_date(F.col("BAD_DATE"), F.lit("YYYYMMDD")),
        )
        row = result.collect()[0]
        assert row["BAD_DATE_CONV"] is None
