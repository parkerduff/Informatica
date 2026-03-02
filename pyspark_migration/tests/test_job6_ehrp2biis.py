"""
Unit tests for Job 6: EHRP2BIIS_UPDATE.

Tests cross-database lookups, deterministic dedup, multi-target writes,
stored procedure execution, and signal handling using local Spark mode.
"""

import pytest
import signal
from unittest.mock import MagicMock, patch

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
)
from pyspark.sql.window import Window

from pyspark_migration.jobs.job6_ehrp2biis import EHRP2BIISUpdateJob


class TestEHRPDeterministicLookups:
    """Tests for deterministic lookup joins replacing 'Use Any Value'."""

    def test_dedup_by_effdt_desc(self, spark):
        """row_number() should pick most recent EFFDT per EMPLID."""
        data = [
            ("E001", 0, "2025-01-01", 0, "D100"),
            ("E001", 0, "2025-06-01", 0, "D200"),
            ("E001", 0, "2025-03-01", 0, "D300"),
            ("E002", 0, "2025-02-01", 0, "D400"),
        ]
        df = spark.createDataFrame(
            data,
            schema=StructType([
                StructField("EMPLID", StringType()),
                StructField("EMPL_RCD", IntegerType()),
                StructField("EFFDT", StringType()),
                StructField("EFFSEQ", IntegerType()),
                StructField("DEPTID", StringType()),
            ]),
        )

        w = Window.partitionBy("EMPLID", "EMPL_RCD").orderBy(
            F.col("EFFDT").desc()
        )
        deduped = (
            df.withColumn("_rn", F.row_number().over(w))
            .filter(F.col("_rn") == 1)
            .drop("_rn")
        )

        assert deduped.count() == 2
        e001 = deduped.filter(F.col("EMPLID") == "E001").collect()[0]
        assert e001["DEPTID"] == "D200"  # Latest EFFDT

    def test_lookup_join_with_broadcast(self, spark):
        """Broadcast join should correctly enrich source data."""
        source = spark.createDataFrame(
            [("E001", 0), ("E002", 0), ("E003", 0)],
            schema=StructType([
                StructField("EMPLID", StringType()),
                StructField("EMPL_RCD", IntegerType()),
            ]),
        )
        lookup = spark.createDataFrame(
            [("E001", "US"), ("E002", "UK")],
            schema=StructType([
                StructField("EMPLID", StringType()),
                StructField("CITIZENSHIP", StringType()),
            ]),
        )

        result = source.join(
            F.broadcast(lookup), on="EMPLID", how="left"
        )

        rows = {r["EMPLID"]: r for r in result.collect()}
        assert rows["E001"]["CITIZENSHIP"] == "US"
        assert rows["E002"]["CITIZENSHIP"] == "UK"
        assert rows["E003"]["CITIZENSHIP"] is None  # No match


class TestEHRPMultiTargetWrites:
    """Tests for writing to multiple target tables."""

    def test_three_target_selection(self, spark):
        """Source should be split correctly into 3 target selections."""
        data = [
            ("E001", 0, "2025-06-01", 0, "D100", "J100"),
            ("E002", 0, "2025-06-15", 0, "D200", "J200"),
        ]
        df = spark.createDataFrame(
            data,
            schema=StructType([
                StructField("EMPLID", StringType()),
                StructField("EMPL_RCD", IntegerType()),
                StructField("EFFDT", StringType()),
                StructField("EFFSEQ", IntegerType()),
                StructField("DEPTID", StringType()),
                StructField("JOBCODE", StringType()),
            ]),
        )

        # EHRP_RECS_TRACKING_TBL selection
        tracking = df.select("EMPLID", "EMPL_RCD", "EFFDT", "EFFSEQ", "DEPTID")
        assert tracking.count() == 2
        assert "JOBCODE" not in tracking.columns

        # NWK_ACTION_PRIMARY_TBL selection
        primary = df.select("EMPLID", "EMPL_RCD", "EFFDT", "EFFSEQ", "DEPTID", "JOBCODE")
        assert primary.count() == 2

        # NWK_ACTION_SECONDARY_TBL selection
        secondary = df.select("EMPLID", "EMPL_RCD", "EFFDT", "EFFSEQ")
        assert secondary.count() == 2


class TestEHRPSignalHandling:
    """Tests for graceful shutdown via signal handling."""

    def test_shutdown_flag_set(self, spark, test_config, mock_db_manager, mock_email_service):
        """SIGTERM/SIGINT should set shutdown flag."""
        job = EHRP2BIISUpdateJob(
            spark, test_config, mock_db_manager, mock_email_service,
        )
        job._install_signal_handlers()

        assert job._shutdown_requested is False

        # Simulate SIGTERM - restore original handler after test
        original = signal.getsignal(signal.SIGTERM)
        try:
            signal.getsignal(signal.SIGTERM)(signal.SIGTERM, None)
            assert job._shutdown_requested is True
        finally:
            signal.signal(signal.SIGTERM, original)


class TestEHRPPrePostLoad:
    """Tests for pre-load and after-load SQL execution."""

    def test_preload_calls_delete(
        self, spark, test_config, mock_db_manager, mock_email_service
    ):
        """Pre-load should DELETE FROM NWK_NEW_EHRP_ACTIONS_TBL."""
        job = EHRP2BIISUpdateJob(
            spark, test_config, mock_db_manager, mock_email_service,
        )
        job._execute_preload()

        mock_db_manager.execute_sql.assert_called()
        call_args = mock_db_manager.execute_sql.call_args[0][0]
        assert "DELETE" in call_args.upper()
        assert "NWK_NEW_EHRP_ACTIONS_TBL" in call_args

    def test_afterload_updates_cleanup(
        self, spark, test_config, mock_db_manager, mock_email_service
    ):
        """After-load should clean up numeric formatting."""
        job = EHRP2BIISUpdateJob(
            spark, test_config, mock_db_manager, mock_email_service,
        )
        job._execute_afterload()

        mock_db_manager.execute_sql.assert_called()


class TestEHRPEmptySource:
    """Tests for zero-row source handling."""

    def test_no_actions_processes_cleanly(
        self, spark, test_config, mock_db_manager, mock_email_service
    ):
        """Job should complete successfully with zero new EHRP actions."""
        empty_source = spark.createDataFrame(
            [],
            schema=StructType([
                StructField("EMPLID", StringType()),
                StructField("EMPL_RCD", IntegerType()),
                StructField("EFFDT", StringType()),
                StructField("EFFSEQ", IntegerType()),
                StructField("DEPTID", StringType()),
                StructField("JOBCODE", StringType()),
            ]),
        )

        mock_db_manager.read_jdbc.return_value = empty_source
        mock_db_manager.execute_sql.return_value = (True, "OK")
        mock_db_manager.execute_stored_procedure.return_value = (True, "OK")

        job = EHRP2BIISUpdateJob(
            spark, test_config, mock_db_manager, mock_email_service,
        )
        metrics = job.run()

        assert metrics.status == "SUCCEEDED"
        assert job.tracking_count == 0
        assert job.primary_count == 0
        assert job.secondary_count == 0
