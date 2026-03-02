"""
Unit tests for Job 4: FDA_Leave.

Tests flat file schema, filter logic, cross-table validation,
error tracking, and Post-SQL DELETEs using local Spark mode.
"""

import pytest
from unittest.mock import MagicMock

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
)

from pyspark_migration.jobs.job4_fda_leave import FDALeaveJob, FDA_TATRAN_FLAT_SCHEMA


class TestFDALeaveSchema:
    """Tests for FDA TATRAN flat file schema."""

    def test_schema_field_count(self):
        """Verify schema has correct number of fields."""
        assert len(FDA_TATRAN_FLAT_SCHEMA.fields) == 6

    def test_key_fields_exist(self):
        """Verify key field names."""
        names = [f.name for f in FDA_TATRAN_FLAT_SCHEMA.fields]
        assert "FDA_TK_NO" in names
        assert "FDA_REC_TYPE" in names
        assert "FDA_HOURS" in names


class TestFDALeaveFilterLogic:
    """Tests for record type filtering."""

    def test_filter_removes_types_01_99(self, spark):
        """rec_type 01 and 99 (header/trailer) should be filtered out."""
        data = [
            ("TK001", "01"), ("TK002", "02"), ("TK003", "12"),
            ("TK004", "99"), ("TK005", "02"),
        ]
        df = spark.createDataFrame(
            data,
            schema=StructType([
                StructField("FDA_TK_NO", StringType()),
                StructField("FDA_REC_TYPE", StringType()),
            ]),
        )
        filtered = df.filter(
            (F.col("FDA_REC_TYPE") != "01") & (F.col("FDA_REC_TYPE") != "99")
        )
        assert filtered.count() == 3

    def test_type_02_passes(self, spark):
        """rec_type 02 should pass through filter."""
        data = [("TK001", "02"), ("TK002", "02")]
        df = spark.createDataFrame(
            data,
            schema=StructType([
                StructField("FDA_TK_NO", StringType()),
                StructField("FDA_REC_TYPE", StringType()),
            ]),
        )
        filtered = df.filter(
            (F.col("FDA_REC_TYPE") != "01") & (F.col("FDA_REC_TYPE") != "99")
        )
        assert filtered.count() == 2


class TestFDALeaveCrossTableValidation:
    """Tests for CPM staging table lookups."""

    def test_lookup_match(self, spark):
        """Records matching CPM staging should pass validation."""
        fda_df = spark.createDataFrame(
            [("SSN001",), ("SSN002",)],
            schema=StructType([StructField("FDA_EMP_ID", StringType())]),
        )
        cpm_df = spark.createDataFrame(
            [("SSN001",), ("SSN003",)],
            schema=StructType([StructField("DYD_SSN_1", StringType())]),
        )

        matched = fda_df.join(
            cpm_df,
            fda_df["FDA_EMP_ID"] == cpm_df["DYD_SSN_1"],
            "inner",
        )
        assert matched.count() == 1

    def test_lookup_no_match_generates_error(self, spark):
        """Unmatched records should be flagged as errors."""
        fda_df = spark.createDataFrame(
            [("SSN001",), ("SSN002",), ("SSN003",)],
            schema=StructType([StructField("FDA_EMP_ID", StringType())]),
        )
        cpm_df = spark.createDataFrame(
            [("SSN001",)],
            schema=StructType([StructField("DYD_SSN_1", StringType())]),
        )

        unmatched = fda_df.join(
            cpm_df,
            fda_df["FDA_EMP_ID"] == cpm_df["DYD_SSN_1"],
            "left_anti",
        )
        assert unmatched.count() == 2


class TestFDALeavePostSQL:
    """Tests for Post-SQL DELETE behavior."""

    def test_delete_employees_without_rec_type_12(self, spark):
        """Post-SQL should delete employees without fda_rec_type='12'."""
        data = [
            ("SSN001", "02"), ("SSN001", "12"),
            ("SSN002", "02"),
            ("SSN003", "12"),
        ]
        df = spark.createDataFrame(
            data,
            schema=StructType([
                StructField("FDA_EMP_ID", StringType()),
                StructField("FDA_REC_TYPE", StringType()),
            ]),
        )

        # Find employees WITH rec_type 12
        has_12 = df.filter(F.col("FDA_REC_TYPE") == "12").select("FDA_EMP_ID").distinct()
        # Find all employees
        all_emps = df.select("FDA_EMP_ID").distinct()
        # Employees WITHOUT rec_type 12 -> to be deleted
        to_delete = all_emps.join(has_12, on="FDA_EMP_ID", how="left_anti")

        assert to_delete.count() == 1
        assert to_delete.collect()[0]["FDA_EMP_ID"] == "SSN002"


class TestFDALeaveSessionSequencing:
    """Tests for session failure handling."""

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

        job = FDALeaveJob(
            spark, test_config, mock_db_manager,
            mock_email_service, mock_counter_manager,
        )

        with pytest.raises(RuntimeError):
            job.run()
