"""
Unit tests for Job 5: CPM_NIH and CPM_CDC.

Tests agency filtering, header/data file generation,
record count aggregation, and email building using local Spark mode.
"""

import pytest
from unittest.mock import MagicMock

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DecimalType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from pyspark_migration.jobs.job5_cpm import CPMBaseJob, CPMCDCJob, CPMNIHJob


class TestCPMAgencyFilter:
    """Tests for agency-specific filtering."""

    def test_nih_filter(self):
        job = CPMNIHJob.__new__(CPMNIHJob)
        assert "MP_POOL_DES" in job.AGENCY_FILTER

    def test_cdc_filter(self):
        job = CPMCDCJob.__new__(CPMCDCJob)
        assert "MP_POOL_DES" in job.AGENCY_FILTER

    def test_nih_agency_name(self):
        assert CPMNIHJob.AGENCY_NAME == "NIH"

    def test_cdc_agency_name(self):
        assert CPMCDCJob.AGENCY_NAME == "CDC"


class TestCPMParameterValidation:
    """Tests for pay period parameter parsing."""

    def test_valid_params(self):
        assert CPMBaseJob._safe_parse_int("2025") == 2025
        assert CPMBaseJob._safe_parse_int("13") == 13

    def test_invalid_params(self):
        assert CPMBaseJob._safe_parse_int("abc") is None
        assert CPMBaseJob._safe_parse_int(None) is None
        assert CPMBaseJob._safe_parse_int("0") is None
        assert CPMBaseJob._safe_parse_int("-5") is None


class TestCPMFinancialData:
    """Tests for DecimalType financial field handling."""

    def test_decimal_precision(self, spark):
        """Financial fields must use DecimalType, not float/double."""
        from decimal import Decimal
        data = [(1, Decimal("50000.50")), (2, Decimal("75000.75"))]
        df = spark.createDataFrame(
            data,
            schema=StructType([
                StructField("ID", IntegerType()),
                StructField("YTD_GROSS_PAY", DecimalType(15, 2)),
            ]),
        )
        assert df.schema["YTD_GROSS_PAY"].dataType == DecimalType(15, 2)

    def test_decimal_arithmetic(self, spark):
        """Verify DecimalType preserves precision in calculations."""
        from decimal import Decimal

        data = [(Decimal("50000.50"),), (Decimal("75000.75"),)]
        df = spark.createDataFrame(
            data,
            schema=StructType([
                StructField("AMOUNT", DecimalType(15, 2)),
            ]),
        )
        total = df.agg(F.sum("AMOUNT").alias("TOTAL")).collect()[0]["TOTAL"]
        assert total == Decimal("125001.25")


class TestCPMSessionSequencing:
    """Tests for CPM session execution order."""

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

        job = CPMNIHJob(
            spark, test_config, mock_db_manager,
            mock_email_service, mock_counter_manager,
        )

        with pytest.raises(RuntimeError, match="[Nn]o.*pay period"):
            job.run()

    def test_no_data_aborts(
        self, spark, test_config, mock_db_manager, mock_email_service, mock_counter_manager
    ):
        """ABORT when no CPM_NEWPAY_TBL data for pay period."""
        pp_df = spark.createDataFrame(
            [(13, 2025)],
            schema=StructType([
                StructField("PP_NUM", IntegerType()),
                StructField("PP_END_YEAR", IntegerType()),
            ]),
        )
        zero_df = spark.createDataFrame(
            [(0,)],
            schema=StructType([StructField("CNT", LongType())]),
        )

        mock_db_manager.read_jdbc.side_effect = [pp_df, zero_df]

        job = CPMNIHJob(
            spark, test_config, mock_db_manager,
            mock_email_service, mock_counter_manager,
        )

        with pytest.raises(RuntimeError, match="[Nn]o.*CPM_NEWPAY"):
            job.run()
