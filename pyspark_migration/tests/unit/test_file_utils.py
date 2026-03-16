"""
Unit Tests for File Utilities

Tests fixed-width file parsing and CSV parsing from common/file_utils.py.
"""

import os
import tempfile

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType

from pyspark_migration.common.file_utils import build_csv_schema, parse_csv, parse_fixed_width


@pytest.fixture(scope="module")
def spark():
    """Create a local SparkSession for testing."""
    session = (
        SparkSession.builder
        .master("local[1]")
        .appName("test_file_utils")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


class TestParseFixedWidth:
    """Tests for fixed-width file extraction."""

    def test_fixed_width_extraction(self, spark, tmp_path):
        """Parse sample line with known offsets, verify all fields."""
        # Create a sample fixed-width file
        content = "123456789CANCODE1PSEUDO123LASTNAME                      FIRST               \n"
        file_path = str(tmp_path / "test_fw.dat")
        with open(file_path, "w") as f:
            f.write(content)

        field_specs = [
            ("SSN", 1, 9, "string"),
            ("CAN_CD", 10, 8, "string"),
            ("PSEUDOSSN", 18, 9, "string"),
            ("LAST_NAME", 27, 30, "string"),
            ("FIRST_NAME", 57, 20, "string"),
        ]

        df = parse_fixed_width(spark, file_path, field_specs)
        row = df.collect()[0]

        assert row["SSN"] == "123456789"
        assert row["CAN_CD"] == "CANCODE1"
        assert row["PSEUDOSSN"] == "PSEUDO123"
        assert row["LAST_NAME"] == "LASTNAME"
        assert row["FIRST_NAME"] == "FIRST"

    def test_fixed_width_multiple_rows(self, spark, tmp_path):
        """Parse multiple rows."""
        content = "111111111LINE1   \n222222222LINE2   \n333333333LINE3   \n"
        file_path = str(tmp_path / "test_multi.dat")
        with open(file_path, "w") as f:
            f.write(content)

        field_specs = [
            ("SSN", 1, 9, "string"),
            ("DATA", 10, 8, "string"),
        ]

        df = parse_fixed_width(spark, file_path, field_specs)
        assert df.count() == 3

    def test_fixed_width_integer_cast(self, spark, tmp_path):
        """Test integer type casting."""
        content = "12345ABCDE\n67890FGHIJ\n"
        file_path = str(tmp_path / "test_int.dat")
        with open(file_path, "w") as f:
            f.write(content)

        field_specs = [
            ("NUM_FIELD", 1, 5, "integer"),
            ("STR_FIELD", 6, 5, "string"),
        ]

        df = parse_fixed_width(spark, file_path, field_specs)
        rows = df.collect()
        assert rows[0]["NUM_FIELD"] == 12345
        assert rows[1]["NUM_FIELD"] == 67890


class TestParseCSV:
    """Tests for CSV parsing."""

    def test_csv_parsing(self, spark, tmp_path):
        """Parse sample CSV with 12 COMPTIME fields."""
        content = (
            "123456789,John Doe,1234,5678,E,100.50,2024,"
            "20240115,20240110,1.5,8.0,0\n"
        )
        file_path = str(tmp_path / "test_comptime.csv")
        with open(file_path, "w") as f:
            f.write(content)

        schema = build_csv_schema([
            ("SSN", "string"),
            ("NAME", "string"),
            ("CURRENT_ACCT", "string"),
            ("CURRENT_ORG", "string"),
            ("FLSA_STATUS", "string"),
            ("COMP_TIME_CUR_BAL", "string"),
            ("COMP_TIME_YEAR_EARNED", "string"),
            ("PP_END_DATE", "string"),
            ("DAILY_DATE_EARNED", "string"),
            ("COMP_TIME_RATE", "string"),
            ("COMP_TIME_HOURS", "string"),
            ("COMP_TIME_UNDEF", "string"),
        ])

        df = parse_csv(spark, file_path, schema=schema)
        row = df.collect()[0]

        assert row["SSN"] == "123456789"
        assert row["NAME"] == "John Doe"
        assert row["FLSA_STATUS"] == "E"

    def test_csv_with_header(self, spark, tmp_path):
        """Parse CSV that has a header row."""
        content = "id,name,value\n1,test,100\n2,test2,200\n"
        file_path = str(tmp_path / "test_header.csv")
        with open(file_path, "w") as f:
            f.write(content)

        df = parse_csv(spark, file_path, header=True)
        assert df.count() == 2
        assert "id" in df.columns


class TestBuildCSVSchema:
    """Tests for schema builder."""

    def test_string_schema(self):
        """Build schema with string fields."""
        schema = build_csv_schema([("A", "string"), ("B", "string")])
        assert len(schema.fields) == 2
        assert schema.fields[0].name == "A"
        assert isinstance(schema.fields[0].dataType, StringType)

    def test_mixed_schema(self):
        """Build schema with mixed types."""
        schema = build_csv_schema([
            ("STR", "string"),
            ("INT", "integer"),
            ("DEC", "decimal(10,2)"),
        ])
        assert len(schema.fields) == 3
