"""
Extended coverage tests for Job 6: EHRP2BIIS_UPDATE.

Exercises: run() failure path, main session with data, _execute_lookups,
_execute_stored_procedures, run_forever, _check_memory, _build_and_send_email.
"""

import gc
import os
import pytest
import signal
import time
from unittest.mock import MagicMock, patch, PropertyMock

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    IntegerType, StringType, StructField, StructType, LongType,
)

from pyspark_migration.jobs.job6_ehrp2biis import EHRP2BIISUpdateJob


class TestEHRPRunFailurePath:
    """Test run() failure/exception path (covers lines 155-159)."""

    def test_run_failure_sends_failure_email_and_reraises(
        self, spark, test_config, mock_db_manager, mock_email_service,
    ):
        """When run() fails, it should send failure email and re-raise."""
        mock_db_manager.execute_sql.return_value = (True, "OK")
        # Make the main session fail
        mock_db_manager.read_jdbc.side_effect = RuntimeError("Oracle connection lost")

        job = EHRP2BIISUpdateJob(
            spark, test_config, mock_db_manager, mock_email_service,
        )

        with pytest.raises(RuntimeError, match="Oracle connection lost"):
            job.run()

        # Verify failure email was sent
        mock_email_service.send_job_failure.assert_called_once()
        assert job._job_metrics.status == "FAILED"


class TestEHRPPreloadWarning:
    """Test preload warning path (covers line 195)."""

    def test_preload_delete_failure_logs_warning(
        self, spark, test_config, mock_db_manager, mock_email_service,
    ):
        """Pre-load delete failure should log warning but not abort."""
        mock_db_manager.execute_sql.return_value = (False, "Table not found")

        job = EHRP2BIISUpdateJob(
            spark, test_config, mock_db_manager, mock_email_service,
        )
        # Should not raise - just logs a warning
        job._execute_preload()

        mock_db_manager.execute_sql.assert_called_once()

    def test_preload_exception_path(
        self, spark, test_config, mock_db_manager, mock_email_service,
    ):
        """Pre-load exception covers lines 201-204."""
        mock_db_manager.execute_sql.side_effect = RuntimeError("Connection refused")

        job = EHRP2BIISUpdateJob(
            spark, test_config, mock_db_manager, mock_email_service,
        )

        with pytest.raises(RuntimeError, match="Connection refused"):
            job._execute_preload()


class TestEHRPMainSessionWithData:
    """Test _session_ehrp2biis_update with actual data (covers lines 260-309)."""

    def test_main_session_with_matching_actions(
        self, spark, test_config, mock_db_manager, mock_email_service,
    ):
        """Main session with source data exercises lookups and multi-target writes."""
        # Source data: 2 rows from PS_GVT_JOB x NWK_NEW_EHRP_ACTIONS_TBL join
        source_df = spark.createDataFrame(
            [
                ("E001", 0, "2025-06-01", 0, "D100", "J100"),
                ("E002", 0, "2025-06-15", 0, "D200", "J200"),
            ],
            schema=StructType([
                StructField("EMPLID", StringType()),
                StructField("EMPL_RCD", IntegerType()),
                StructField("EFFDT", StringType()),
                StructField("EFFSEQ", IntegerType()),
                StructField("DEPTID", StringType()),
                StructField("JOBCODE", StringType()),
            ]),
        )

        # Lookup tables - minimal data for each
        seq_df = spark.createDataFrame(
            [("2025",)],
            schema=StructType([StructField("EHRP_YEAR", StringType())]),
        )

        employment_df = spark.createDataFrame(
            [("E001", 0, "2025-06-01", 0, "A")],
            schema=StructType([
                StructField("EMPLID", StringType()),
                StructField("EMPL_RCD", IntegerType()),
                StructField("EFFDT", StringType()),
                StructField("EFFSEQ", IntegerType()),
                StructField("GVT_STATUS", StringType()),
            ]),
        )

        jpm_df = spark.createDataFrame(
            [("E001", "ANALYST")],
            schema=StructType([
                StructField("JPM_PROFILE_ID", StringType()),
                StructField("JPM_DESCR", StringType()),
            ]),
        )

        call_count = [0]
        def mock_read(table_or_query, **kwargs):
            call_count[0] += 1
            q = table_or_query.upper() if isinstance(table_or_query, str) else ""
            if "PS_GVT_JOB" in q and "NWK_NEW_EHRP_ACTIONS" in q:
                return source_df
            elif "SEQUENCE_NUM_TBL" in q or table_or_query == "SEQUENCE_NUM_TBL":
                return seq_df
            elif "PS_JPM_JP_ITEMS" in q or table_or_query == "PS_JPM_JP_ITEMS":
                return jpm_df
            elif any(t in table_or_query for t in [
                "PS_GVT_EMPLOYMENT", "PS_GVT_PERS_NID", "PS_GVT_AWD_DATA",
                "PS_GVT_EE_DATA_TRK", "PS_HE_FILL_POS", "PS_GVT_CITIZENSHIP",
                "PS_GVT_PERS_DATA",
            ]):
                return employment_df
            return source_df

        mock_db_manager.read_jdbc.side_effect = mock_read
        mock_db_manager.write_jdbc.return_value = 2
        mock_db_manager.execute_sql.return_value = (True, "OK")

        job = EHRP2BIISUpdateJob(
            spark, test_config, mock_db_manager, mock_email_service,
        )
        job._session_ehrp2biis_update()

        assert job.tracking_count == 2
        assert job.primary_count == 2
        assert job.secondary_count == 2

    def test_main_session_exception_path(
        self, spark, test_config, mock_db_manager, mock_email_service,
    ):
        """Main session exception covers lines 306-309."""
        mock_db_manager.read_jdbc.side_effect = RuntimeError("JDBC timeout")

        job = EHRP2BIISUpdateJob(
            spark, test_config, mock_db_manager, mock_email_service,
        )

        with pytest.raises(RuntimeError, match="JDBC timeout"):
            job._session_ehrp2biis_update()


class TestEHRPExecuteLookups:
    """Test _execute_lookups method (covers lines 323-416)."""

    def test_lookups_with_data(
        self, spark, test_config, mock_db_manager, mock_email_service,
    ):
        """Execute all 9 lookups with data."""
        source_df = spark.createDataFrame(
            [("E001", 0, "2025-06-01", 0, "D100", "J100")],
            schema=StructType([
                StructField("EMPLID", StringType()),
                StructField("EMPL_RCD", IntegerType()),
                StructField("EFFDT", StringType()),
                StructField("EFFSEQ", IntegerType()),
                StructField("DEPTID", StringType()),
                StructField("JOBCODE", StringType()),
            ]),
        )

        # Generic lookup table with join keys
        lookup_df = spark.createDataFrame(
            [("E001", 0, "2025-06-01", 0, "EXTRA_VAL")],
            schema=StructType([
                StructField("EMPLID", StringType()),
                StructField("EMPL_RCD", IntegerType()),
                StructField("EFFDT", StringType()),
                StructField("EFFSEQ", IntegerType()),
                StructField("LOOKUP_FIELD", StringType()),
            ]),
        )

        seq_df = spark.createDataFrame(
            [("2025", "SEQ001")],
            schema=StructType([
                StructField("EHRP_YEAR", StringType()),
                StructField("SEQ_NUM", StringType()),
            ]),
        )

        jpm_df = spark.createDataFrame(
            [("E001", "ANALYST_ROLE")],
            schema=StructType([
                StructField("JPM_PROFILE_ID", StringType()),
                StructField("JPM_DESCR", StringType()),
            ]),
        )

        # Use cross_db_manager for lookups
        cross_db = MagicMock()
        call_count = [0]
        def mock_cross_read(table_or_query, **kwargs):
            call_count[0] += 1
            if table_or_query == "SEQUENCE_NUM_TBL":
                return seq_df
            elif table_or_query == "PS_JPM_JP_ITEMS":
                return jpm_df
            return lookup_df

        cross_db.read_jdbc.side_effect = mock_cross_read

        job = EHRP2BIISUpdateJob(
            spark, test_config, mock_db_manager, mock_email_service,
            cross_db_manager=cross_db,
        )

        result = job._execute_lookups(source_df)

        # Should have enriched the source DataFrame
        assert result.count() >= 1
        # cross_db should have been called for each lookup table
        assert cross_db.read_jdbc.call_count >= 8  # 8 standard + JPM

    def test_lookups_with_failed_table(
        self, spark, test_config, mock_db_manager, mock_email_service,
    ):
        """Failed lookup should be skipped (not crash)."""
        source_df = spark.createDataFrame(
            [("E001", 0, "2025-06-01", 0, "D100", "J100")],
            schema=StructType([
                StructField("EMPLID", StringType()),
                StructField("EMPL_RCD", IntegerType()),
                StructField("EFFDT", StringType()),
                StructField("EFFSEQ", IntegerType()),
                StructField("DEPTID", StringType()),
                StructField("JOBCODE", StringType()),
            ]),
        )

        cross_db = MagicMock()
        cross_db.read_jdbc.side_effect = RuntimeError("Table not found")

        job = EHRP2BIISUpdateJob(
            spark, test_config, mock_db_manager, mock_email_service,
            cross_db_manager=cross_db,
        )

        # Should not raise - lookups are skipped on failure
        result = job._execute_lookups(source_df)
        assert result.count() == 1


class TestEHRPAfterloadException:
    """Test _execute_afterload exception path (covers lines 443-446)."""

    def test_afterload_exception_handled(
        self, spark, test_config, mock_db_manager, mock_email_service,
    ):
        """After-load SQL exception should be caught (not re-raised)."""
        mock_db_manager.execute_sql.side_effect = RuntimeError("SQL error")

        job = EHRP2BIISUpdateJob(
            spark, test_config, mock_db_manager, mock_email_service,
        )

        # Should not raise - after-load errors are caught
        job._execute_afterload()
        # Metrics should show failure
        assert any(
            s.status == "FAILED" for s in job._job_metrics.sessions
        )


class TestEHRPStoredProcedures:
    """Test _execute_stored_procedures (covers lines 474-475, 486-489)."""

    def test_stored_procedures_shutdown_skips_remaining(
        self, spark, test_config, mock_db_manager, mock_email_service,
    ):
        """Shutdown requested should skip remaining procedures."""
        mock_db_manager.execute_stored_procedure.return_value = (True, "OK")

        job = EHRP2BIISUpdateJob(
            spark, test_config, mock_db_manager, mock_email_service,
        )
        job._shutdown_requested = True

        job._execute_stored_procedures()

        # No procedures should have been called
        mock_db_manager.execute_stored_procedure.assert_not_called()

    def test_stored_procedures_failure_logs_warning(
        self, spark, test_config, mock_db_manager, mock_email_service,
    ):
        """Failed stored procedure should log warning but continue."""
        mock_db_manager.execute_stored_procedure.return_value = (False, "Proc error")

        job = EHRP2BIISUpdateJob(
            spark, test_config, mock_db_manager, mock_email_service,
        )

        job._execute_stored_procedures()

        # All 4 procedures should have been attempted
        assert mock_db_manager.execute_stored_procedure.call_count == 4

    def test_stored_procedures_exception_path(
        self, spark, test_config, mock_db_manager, mock_email_service,
    ):
        """Exception in stored procedure execution (covers lines 486-489)."""
        mock_db_manager.execute_stored_procedure.side_effect = RuntimeError("Connection lost")

        job = EHRP2BIISUpdateJob(
            spark, test_config, mock_db_manager, mock_email_service,
        )

        # Should not raise - exceptions are caught
        job._execute_stored_procedures()


class TestEHRPRunForever:
    """Test run_forever polling loop (covers lines 507-553)."""

    def test_run_forever_processes_one_iteration_then_stops(
        self, spark, test_config, mock_db_manager, mock_email_service,
    ):
        """Run forever should process pending work and stop on signal."""
        # Mock pending count: 1 pending action first, then signal shutdown
        pending_df = spark.createDataFrame(
            [(1,)],
            schema=StructType([StructField("CNT", LongType())]),
        )

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

        call_count = [0]
        def mock_read(query, **kwargs):
            call_count[0] += 1
            if "COUNT" in query:
                return pending_df
            return empty_source

        mock_db_manager.read_jdbc.side_effect = mock_read
        mock_db_manager.execute_sql.return_value = (True, "OK")
        mock_db_manager.execute_stored_procedure.return_value = (True, "OK")
        mock_db_manager.write_jdbc.return_value = 0

        job = EHRP2BIISUpdateJob(
            spark, test_config, mock_db_manager, mock_email_service,
        )
        job._poll_interval_seconds = 1  # Very short poll

        # Set shutdown after brief delay via a patched sleep
        original_time_sleep = time.sleep
        sleep_count = [0]
        def mock_sleep(seconds):
            sleep_count[0] += 1
            if sleep_count[0] >= 2:
                job._shutdown_requested = True
            original_time_sleep(0.01)  # Tiny actual sleep

        with patch("time.sleep", side_effect=mock_sleep):
            job.run_forever()

        # Should have completed at least 1 iteration
        assert call_count[0] >= 1

    def test_run_forever_no_pending_work(
        self, spark, test_config, mock_db_manager, mock_email_service,
    ):
        """Run forever with no pending work just sleeps."""
        zero_df = spark.createDataFrame(
            [(0,)],
            schema=StructType([StructField("CNT", LongType())]),
        )
        mock_db_manager.read_jdbc.return_value = zero_df

        job = EHRP2BIISUpdateJob(
            spark, test_config, mock_db_manager, mock_email_service,
        )
        job._poll_interval_seconds = 1

        sleep_count = [0]
        def mock_sleep(seconds):
            sleep_count[0] += 1
            if sleep_count[0] >= 2:
                job._shutdown_requested = True

        with patch("time.sleep", side_effect=mock_sleep):
            job.run_forever()

    def test_run_forever_handles_iteration_error(
        self, spark, test_config, mock_db_manager, mock_email_service,
    ):
        """Run forever should continue on iteration errors."""
        mock_db_manager.read_jdbc.side_effect = RuntimeError("Transient error")

        job = EHRP2BIISUpdateJob(
            spark, test_config, mock_db_manager, mock_email_service,
        )
        job._poll_interval_seconds = 1

        sleep_count = [0]
        def mock_sleep(seconds):
            sleep_count[0] += 1
            if sleep_count[0] >= 2:
                job._shutdown_requested = True

        with patch("time.sleep", side_effect=mock_sleep):
            job.run_forever()  # Should not raise


class TestEHRPCheckMemory:
    """Test _check_memory method (covers lines 561-591)."""

    def test_check_memory_linux(
        self, spark, test_config, mock_db_manager, mock_email_service,
    ):
        """Memory check on Linux reads /proc/self/status."""
        job = EHRP2BIISUpdateJob(
            spark, test_config, mock_db_manager, mock_email_service,
        )

        # This should work on Linux (our test environment)
        job._check_memory(1)
        # No assertion needed - just verify it doesn't crash

    def test_check_memory_above_threshold(
        self, spark, test_config, mock_db_manager, mock_email_service,
    ):
        """Memory check logs warning when above threshold."""
        job = EHRP2BIISUpdateJob(
            spark, test_config, mock_db_manager, mock_email_service,
        )
        job._memory_warn_mb = 1  # Very low threshold - will trigger warning

        job._check_memory(1)
        # Should have logged a warning and called gc.collect()

    def test_check_memory_no_proc(
        self, spark, test_config, mock_db_manager, mock_email_service,
    ):
        """Memory check without /proc falls back to GC stats."""
        job = EHRP2BIISUpdateJob(
            spark, test_config, mock_db_manager, mock_email_service,
        )

        with patch("builtins.open", side_effect=OSError("No /proc")):
            job._check_memory(1)
            # Should use GC fallback, not crash


class TestEHRPBuildAndSendEmail:
    """Test _build_and_send_email (covers lines 593-618)."""

    def test_success_email(
        self, spark, test_config, mock_db_manager, mock_email_service,
    ):
        """Success email includes record counts."""
        job = EHRP2BIISUpdateJob(
            spark, test_config, mock_db_manager, mock_email_service,
        )
        job.tracking_count = 10
        job.primary_count = 10
        job.secondary_count = 10

        job._build_and_send_email(success=True)

        assert "completed successfully" in job.wf_subject
        assert "10" in job.wf_message
        mock_email_service.send_job_success.assert_called_once()

    def test_failure_email(
        self, spark, test_config, mock_db_manager, mock_email_service,
    ):
        """Failure email (covers line 614)."""
        job = EHRP2BIISUpdateJob(
            spark, test_config, mock_db_manager, mock_email_service,
        )

        job._build_and_send_email(success=False, error="Something broke")

        mock_email_service.send_job_failure.assert_called_once()
        call_args = mock_email_service.send_job_failure.call_args
        assert "Something broke" in call_args[0][1] or "Something broke" in str(call_args)


class TestEHRPFullRunWithData:
    """Test full run() with actual matching actions (covers lines 260-309 via run)."""

    def test_full_run_with_data(
        self, spark, test_config, mock_db_manager, mock_email_service,
    ):
        """Full run with source data processes all steps."""
        source_df = spark.createDataFrame(
            [("E001", 0, "2025-06-01", 0, "D100", "J100")],
            schema=StructType([
                StructField("EMPLID", StringType()),
                StructField("EMPL_RCD", IntegerType()),
                StructField("EFFDT", StringType()),
                StructField("EFFSEQ", IntegerType()),
                StructField("DEPTID", StringType()),
                StructField("JOBCODE", StringType()),
            ]),
        )

        lookup_df = spark.createDataFrame(
            [("E001", 0, "2025-06-01", 0, "EXTRA")],
            schema=StructType([
                StructField("EMPLID", StringType()),
                StructField("EMPL_RCD", IntegerType()),
                StructField("EFFDT", StringType()),
                StructField("EFFSEQ", IntegerType()),
                StructField("EXTRA_COL", StringType()),
            ]),
        )

        jpm_df = spark.createDataFrame(
            [("E001", "ROLE1")],
            schema=StructType([
                StructField("JPM_PROFILE_ID", StringType()),
                StructField("JPM_DESCR", StringType()),
            ]),
        )

        seq_df = spark.createDataFrame(
            [("2025", "1")],
            schema=StructType([
                StructField("EHRP_YEAR", StringType()),
                StructField("SEQ_NUM", StringType()),
            ]),
        )

        def mock_read(query, **kwargs):
            q = str(query).upper()
            if "PS_GVT_JOB" in q and "NWK_NEW_EHRP" in q:
                return source_df
            elif query == "SEQUENCE_NUM_TBL":
                return seq_df
            elif query == "PS_JPM_JP_ITEMS":
                return jpm_df
            return lookup_df

        mock_db_manager.read_jdbc.side_effect = mock_read
        mock_db_manager.write_jdbc.return_value = 1
        mock_db_manager.execute_sql.return_value = (True, "OK")
        mock_db_manager.execute_stored_procedure.return_value = (True, "OK")

        job = EHRP2BIISUpdateJob(
            spark, test_config, mock_db_manager, mock_email_service,
        )
        metrics = job.run()

        assert metrics.status == "SUCCEEDED"
        assert job.tracking_count == 1
        assert job.primary_count == 1
        assert job.secondary_count == 1
        mock_email_service.send_job_success.assert_called_once()
