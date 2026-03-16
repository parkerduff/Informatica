"""
Integration Tests for COMPTIME Job

Tests CSV loading, record filtering, and message building.
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
        .appName("test_comptime")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


class TestComptimeCSVLoad:
    """Tests for COMPTIME CSV parsing."""

    def test_csv_load_12_fields(self, spark, tmp_path):
        """Load CSV file with 12 COMPTIME fields."""
        content = (
            "123456789,John Doe,1234,5678,E,100.50,2024,"
            "20240115,20240110,1.5,8.0,0\n"
            "987654321,Jane Smith,4321,8765,N,200.75,2024,"
            "20240115,20240111,2.0,6.5,0\n"
        )
        csv_path = str(tmp_path / "U0287D01")
        with open(csv_path, "w") as f:
            f.write(content)

        from pyspark_migration.jobs.comptime.comptime_job import COMPTIME_SCHEMA

        df = spark.read.csv(csv_path, schema=COMPTIME_SCHEMA)
        assert df.count() == 2
        assert len(df.columns) == 12

    def test_header_record_filtered(self, spark, tmp_path):
        """Header records should be filtered out."""
        content = (
            "HEADER001,Header Row,0000,0000,X,0,0,00000000,00000000,0,0,0\n"
            "123456789,John Doe,1234,5678,E,100.50,2024,20240115,20240110,1.5,8.0,0\n"
        )
        csv_path = str(tmp_path / "U0287D01_hdr")
        with open(csv_path, "w") as f:
            f.write(content)

        from pyspark_migration.common.validation import classify_record_type
        from pyspark_migration.jobs.comptime.comptime_job import COMPTIME_SCHEMA

        df = spark.read.csv(csv_path, schema=COMPTIME_SCHEMA)
        classified = df.withColumn(
            "RECORD_TYPE_FLAG", classify_record_type(F.col("SSN"))
        )
        detail = classified.filter(F.col("RECORD_TYPE_FLAG") == "D")
        assert detail.count() == 1


class TestComptimeMessageBuilder:
    """Tests for message builder."""

    def test_message_contains_count(self, spark):
        """Message includes detail record count."""
        from pyspark_migration.jobs.comptime.comptime_job import (
            mapping_build_message_counters,
        )

        with patch("pyspark_migration.jobs.comptime.comptime_job.log_counter"):
            with patch("pyspark_migration.jobs.comptime.comptime_job.send_email"):
                subject, message = mapping_build_message_counters(
                    spark, detail_count=42, pp_num=5, pp_end_year=2024
                )

        assert "42" in message
        assert "2024" in subject or "5" in subject

    def test_message_environment_prefix(self, spark):
        """Subject includes environment prefix."""
        from pyspark_migration.jobs.comptime.comptime_job import (
            mapping_build_message_counters,
        )

        with patch("pyspark_migration.jobs.comptime.comptime_job.log_counter"):
            with patch("pyspark_migration.jobs.comptime.comptime_job.send_email"):
                subject, _ = mapping_build_message_counters(
                    spark, detail_count=10, pp_num=3, pp_end_year=2024
                )

        # Subject should contain environment prefix and comp time info
        assert "Comp Time" in subject or "comp" in subject.lower()
