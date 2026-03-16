"""
Unit Tests for Pay Period Utilities

Tests pay period lookup and broadcast join from common/pay_period.py.
"""

from datetime import date
from unittest.mock import MagicMock, patch

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DateType,
    DecimalType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)


@pytest.fixture(scope="module")
def spark():
    """Create a local SparkSession for testing."""
    session = (
        SparkSession.builder
        .master("local[1]")
        .appName("test_pay_period")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


PAY_PERIOD_SCHEMA = StructType([
    StructField("PP_NUM", IntegerType(), True),
    StructField("PP_END_YEAR", IntegerType(), True),
    StructField("PP_START_DTE", DateType(), True),
    StructField("PP_END_DTE", DateType(), True),
    StructField("CURR_PP_FLAG", StringType(), True),
])


class TestGetCurrentPayPeriod:
    """Tests for current pay period retrieval."""

    @patch("pyspark_migration.common.pay_period.read_oracle_table")
    def test_current_pay_period_returns_one_row(self, mock_read, spark):
        """Mock PAY_PERIOD with one 'Y' row."""
        from pyspark_migration.common.pay_period import get_current_pay_period

        mock_df = spark.createDataFrame(
            [
                (1, 2024, date(2024, 1, 1), date(2024, 1, 13), None),
                (2, 2024, date(2024, 1, 14), date(2024, 1, 27), "Y"),
                (3, 2024, date(2024, 1, 28), date(2024, 2, 10), None),
            ],
            schema=PAY_PERIOD_SCHEMA,
        )
        mock_read.return_value = mock_df

        result = get_current_pay_period(spark)
        assert result.count() == 1

        row = result.collect()[0]
        assert row["PP_NUM"] == 2
        assert row["PP_END_YEAR"] == 2024

    @patch("pyspark_migration.common.pay_period.read_oracle_table")
    def test_no_current_pay_period_raises(self, mock_read, spark):
        """Should raise RuntimeError when no current pay period."""
        from pyspark_migration.common.pay_period import get_current_pay_period

        mock_df = spark.createDataFrame(
            [
                (1, 2024, date(2024, 1, 1), date(2024, 1, 13), None),
                (2, 2024, date(2024, 1, 14), date(2024, 1, 27), None),
            ],
            schema=PAY_PERIOD_SCHEMA,
        )
        mock_read.return_value = mock_df

        with pytest.raises(RuntimeError, match="No current pay period"):
            get_current_pay_period(spark)

    @patch("pyspark_migration.common.pay_period.read_oracle_table")
    def test_multiple_current_pay_periods_raises(self, mock_read, spark):
        """Should raise RuntimeError when multiple current pay periods."""
        from pyspark_migration.common.pay_period import get_current_pay_period

        mock_df = spark.createDataFrame(
            [
                (1, 2024, date(2024, 1, 1), date(2024, 1, 13), "Y"),
                (2, 2024, date(2024, 1, 14), date(2024, 1, 27), "Y"),
            ],
            schema=PAY_PERIOD_SCHEMA,
        )
        mock_read.return_value = mock_df

        with pytest.raises(RuntimeError, match="Multiple current pay periods"):
            get_current_pay_period(spark)


class TestBroadcastJoin:
    """Tests for broadcast join enrichment."""

    @patch("pyspark_migration.common.pay_period.read_oracle_table")
    def test_broadcast_join_enriches_data(self, mock_read, spark):
        """Verify PP_NUM and PP_END_YEAR are added to DataFrame."""
        from pyspark_migration.common.pay_period import enrich_with_pay_period

        # Mock pay period table
        mock_pp_df = spark.createDataFrame(
            [(5, 2024, date(2024, 3, 3), date(2024, 3, 16), "Y")],
            schema=PAY_PERIOD_SCHEMA,
        )
        mock_read.return_value = mock_pp_df

        # Sample data to enrich
        data_df = spark.createDataFrame(
            [("123456789", "John"), ("987654321", "Jane")],
            schema=StructType([
                StructField("SSN", StringType()),
                StructField("NAME", StringType()),
            ]),
        )

        result = enrich_with_pay_period(data_df, spark)

        assert "PP_NUM" in result.columns
        assert "PP_END_YEAR" in result.columns
        assert result.count() == 2

        row = result.collect()[0]
        assert row["PP_NUM"] == 5
        assert row["PP_END_YEAR"] == 2024
