"""
Extended coverage tests for Job 3: Pseudossn.

Exercises all session methods, full run() path, archive loading,
SDA file processing, timekeeper updates, and counter writes.
"""

import os
import pytest
import tempfile
from unittest.mock import MagicMock, call

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DecimalType,
)

from pyspark_migration.jobs.job3_pseudossn import PseudossnJob


@pytest.fixture
def pseudossn_test_files():
    """Create temporary SDA flat files for testing."""
    # PSEUDOSSN_FILE: header + 3 detail + trailer = 5 rows
    psn_content = (
        "HDR,20250314,PSEUDOSSN_FILE\n"
        "SSN001,TK001,2025-01-01,DETAIL1,F1,F2,F3,F4,F5\n"
        "SSN002,TK002,2025-02-01,DETAIL2,F1,F2,F3,F4,F5\n"
        "SSN003,TK003,2025-03-01,DETAIL3,F1,F2,F3,F4,F5\n"
        "TRL,3,END\n"
    )
    # PSEUDOSSN_FILE_TK_NUM: header + 2 detail + trailer = 4 rows
    tk_content = (
        "HDR,20250314,TK_NUM_FILE\n"
        "SSN001,TK_NEW_001,2025-01-01,DETAIL1\n"
        "SSN004,TK_NEW_004,2025-04-01,DETAIL2\n"
        "TRL,2,END\n"
    )

    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".csv", delete=False, dir="/tmp"
    ) as f:
        f.write(psn_content)
        psn_path = f.name

    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".csv", delete=False, dir="/tmp"
    ) as f:
        f.write(tk_content)
        tk_path = f.name

    yield psn_path, tk_path

    os.unlink(psn_path)
    os.unlink(tk_path)


class TestPseudossnFullRun:
    """Test the full run() method path (covers lines 132-175)."""

    def test_successful_end_to_end_run(
        self, spark, test_config, mock_db_manager, mock_email_service,
        mock_counter_manager, pseudossn_test_files,
    ):
        """Full successful run covers all 10 sessions + email."""
        psn_path, tk_path = pseudossn_test_files

        # Mock PAY_PERIOD read
        pp_df = spark.createDataFrame(
            [(5, 2025, "2025-03-01", "2025-03-14")],
            schema=StructType([
                StructField("PP_NUM", IntegerType()),
                StructField("PP_END_YEAR", IntegerType()),
                StructField("PP_START_DTE", StringType()),
                StructField("PP_END_DTE", StringType()),
            ]),
        )

        # Mock PSEUDOSSN_TBL read for archive (session 5)
        pseudossn_df = spark.createDataFrame(
            [("SSN001", "TK001", "2025-01-01"), ("SSN002", "TK002", "2025-02-01")],
            schema=StructType([
                StructField("PSEUDOSSN", StringType()),
                StructField("TK_NUM", StringType()),
                StructField("PSEUDOSSN_EFF_DT", StringType()),
            ]),
        )

        # Mock for session 9 - records with NULL TK_NUM
        null_tk_df = spark.createDataFrame(
            [("SSN003", None)],
            schema=StructType([
                StructField("PSEUDOSSN", StringType()),
                StructField("TK_NUM", StringType()),
            ]),
        )

        # SDA lookup for session 9
        sda_tk_df = spark.createDataFrame(
            [("SSN003", "TK_NEW_003")],
            schema=StructType([
                StructField("PSEUDOSSN", StringType()),
                StructField("TK_NUM", StringType()),
            ]),
        )

        # Set up read_jdbc to return different DataFrames for different queries
        call_count = [0]
        def mock_read_jdbc(query, **kwargs):
            call_count[0] += 1
            if "PAY_PERIOD" in query:
                return pp_df
            elif "PSEUDOSSN_TBL" in query and "TK_NUM IS NULL" in query:
                return null_tk_df
            elif "PSEUDOSSN_FROM_SDA_TBL" in query:
                return sda_tk_df
            elif "PSEUDOSSN_TBL" in query:
                return pseudossn_df
            return spark.createDataFrame([], schema="col1 string")

        mock_db_manager.read_jdbc.side_effect = mock_read_jdbc
        mock_db_manager.write_jdbc.return_value = 2

        job = PseudossnJob(
            spark, test_config, mock_db_manager,
            mock_email_service, mock_counter_manager,
        )

        metrics = job.run(
            pseudossn_file_path=psn_path,
            pseudossn_tk_file_path=tk_path,
        )

        assert metrics.status == "SUCCEEDED"
        assert job.map_pp_end_year == 2025
        assert job.map_pp_num == 5
        mock_email_service.send_job_success.assert_called_once()

    def test_run_failure_sends_failure_email(
        self, spark, test_config, mock_db_manager, mock_email_service,
        mock_counter_manager,
    ):
        """Failed run sends failure email (covers lines 167-173)."""
        mock_db_manager.read_jdbc.side_effect = RuntimeError("DB down")

        job = PseudossnJob(
            spark, test_config, mock_db_manager,
            mock_email_service, mock_counter_manager,
        )

        with pytest.raises(RuntimeError, match="DB down"):
            job.run(
                pseudossn_file_path="/tmp/dummy_psn.csv",
                pseudossn_tk_file_path="/tmp/dummy_tk.csv",
            )

        mock_email_service.send_job_failure.assert_called_once()


class TestPseudossnSessionCurrentPayPeriod:
    """Test _session_current_pay_period success path (covers lines 198-210)."""

    def test_sets_workflow_variables(
        self, spark, test_config, mock_db_manager, mock_email_service, mock_counter_manager,
    ):
        """Session 1 sets pay period workflow variables."""
        pp_df = spark.createDataFrame(
            [(7, 2025, "2025-04-01", "2025-04-14")],
            schema=StructType([
                StructField("PP_NUM", IntegerType()),
                StructField("PP_END_YEAR", IntegerType()),
                StructField("PP_START_DTE", StringType()),
                StructField("PP_END_DTE", StringType()),
            ]),
        )
        mock_db_manager.read_jdbc.side_effect = lambda *a, **kw: pp_df

        job = PseudossnJob(
            spark, test_config, mock_db_manager,
            mock_email_service, mock_counter_manager,
        )
        job._session_current_pay_period()

        assert job.map_pp_end_year == 2025
        assert job.map_pp_num == 7
        assert job.map_pp_year_num == "202507"

    def test_multiple_pp_aborts(
        self, spark, test_config, mock_db_manager, mock_email_service, mock_counter_manager,
    ):
        """Multiple current pay periods should abort."""
        multi_df = spark.createDataFrame(
            [(5, 2025, "a", "b"), (6, 2025, "c", "d")],
            schema=StructType([
                StructField("PP_NUM", IntegerType()),
                StructField("PP_END_YEAR", IntegerType()),
                StructField("PP_START_DTE", StringType()),
                StructField("PP_END_DTE", StringType()),
            ]),
        )
        mock_db_manager.read_jdbc.side_effect = lambda *a, **kw: multi_df

        job = PseudossnJob(
            spark, test_config, mock_db_manager,
            mock_email_service, mock_counter_manager,
        )

        with pytest.raises(RuntimeError, match="[Mm]ultiple"):
            job._session_current_pay_period()


class TestPseudossnSessionVerifyHeaderDate:
    """Test _session_verify_header_date (covers lines 226-244)."""

    def test_verify_header_date(
        self, spark, test_config, mock_db_manager, mock_email_service,
        mock_counter_manager, pseudossn_test_files,
    ):
        """Session 2 reads header and validates."""
        psn_path, _ = pseudossn_test_files

        job = PseudossnJob(
            spark, test_config, mock_db_manager,
            mock_email_service, mock_counter_manager,
        )
        job._session_verify_header_date(psn_path)

        # Should complete without error
        assert len(job._job_metrics.sessions) == 1
        assert job._job_metrics.sessions[0].status == "SUCCEEDED"


class TestPseudossnSessionVerifyRecordCount:
    """Test _session_verify_record_count (covers lines 254-275)."""

    def test_verify_record_count(
        self, spark, test_config, mock_db_manager, mock_email_service,
        mock_counter_manager, pseudossn_test_files,
    ):
        """Session 3 counts detail records (total - header - trailer)."""
        psn_path, _ = pseudossn_test_files

        job = PseudossnJob(
            spark, test_config, mock_db_manager,
            mock_email_service, mock_counter_manager,
        )
        job._session_verify_record_count(psn_path)

        # 5 total rows - 2 (header+trailer) = 3 detail records
        assert job.detail_count == 3
        assert job._job_metrics.sessions[0].status == "SUCCEEDED"


class TestPseudossnSessionLoadPseudossnTbl:
    """Test _session_load_pseudossn_tbl (covers lines 292-314)."""

    def test_load_pseudossn_tbl(
        self, spark, test_config, mock_db_manager, mock_email_service,
        mock_counter_manager, pseudossn_test_files,
    ):
        """Session 4 loads detail records to PSEUDOSSN_TBL."""
        psn_path, _ = pseudossn_test_files

        job = PseudossnJob(
            spark, test_config, mock_db_manager,
            mock_email_service, mock_counter_manager,
        )
        job._session_load_pseudossn_tbl(psn_path)

        assert job.loaded_count == 3
        assert job._job_metrics.sessions[0].status == "SUCCEEDED"


class TestPseudossnSessionLoadArchive:
    """Test _session_load_archive (covers lines 322-341)."""

    def test_load_archive(
        self, spark, test_config, mock_db_manager, mock_email_service, mock_counter_manager,
    ):
        """Session 5 copies PSEUDOSSN_TBL to HI_ARCH_PSEUDOSSN_TBL."""
        pseudossn_df = spark.createDataFrame(
            [("SSN001", "TK001"), ("SSN002", "TK002")],
            schema=StructType([
                StructField("PSEUDOSSN", StringType()),
                StructField("TK_NUM", StringType()),
            ]),
        )
        mock_db_manager.read_jdbc.side_effect = lambda *a, **kw: pseudossn_df
        mock_db_manager.write_jdbc.return_value = 2

        job = PseudossnJob(
            spark, test_config, mock_db_manager,
            mock_email_service, mock_counter_manager,
        )
        job._session_load_archive()

        assert job.archive_count == 2
        mock_db_manager.write_jdbc.assert_called_once()
        call_args = mock_db_manager.write_jdbc.call_args
        assert call_args[0][1] == "HI_ARCH_PSEUDOSSN_TBL"


class TestPseudossnSessionVerifyHeaderDateSDA:
    """Test _session_verify_header_date_sda (covers lines 349-364)."""

    def test_verify_header_date_sda(
        self, spark, test_config, mock_db_manager, mock_email_service,
        mock_counter_manager, pseudossn_test_files,
    ):
        """Session 6 validates SDA file header date."""
        _, tk_path = pseudossn_test_files

        job = PseudossnJob(
            spark, test_config, mock_db_manager,
            mock_email_service, mock_counter_manager,
        )
        job._session_verify_header_date_sda(tk_path)

        assert job._job_metrics.sessions[0].status == "SUCCEEDED"


class TestPseudossnSessionLoadFromSDA:
    """Test _session_load_pseudossn_from_sda (covers lines 376-395)."""

    def test_load_from_sda(
        self, spark, test_config, mock_db_manager, mock_email_service,
        mock_counter_manager, pseudossn_test_files,
    ):
        """Session 7 loads SDA records to PSEUDOSSN_FROM_SDA_TBL."""
        _, tk_path = pseudossn_test_files

        job = PseudossnJob(
            spark, test_config, mock_db_manager,
            mock_email_service, mock_counter_manager,
        )
        job._session_load_pseudossn_from_sda(tk_path)

        # 4 total rows - 2 (header+trailer) = 2 detail records
        assert job.sda_count == 2
        assert job._job_metrics.sessions[0].status == "SUCCEEDED"


class TestPseudossnSessionLoadSDARecords:
    """Test _session_load_sda_records_pseudossn_tbl (covers lines 404-419)."""

    def test_load_sda_records(
        self, spark, test_config, mock_db_manager, mock_email_service, mock_counter_manager,
    ):
        """Session 8 inserts new SDA records not already in PSEUDOSSN_TBL."""
        job = PseudossnJob(
            spark, test_config, mock_db_manager,
            mock_email_service, mock_counter_manager,
        )
        job._session_load_sda_records_pseudossn_tbl()

        assert job._job_metrics.sessions[0].status == "SUCCEEDED"


class TestPseudossnSessionUpdateTimekeeperNumber:
    """Test _session_update_timekeeper_number (covers lines 430-481)."""

    def test_update_timekeeper_with_matching_records(
        self, spark, test_config, mock_db_manager, mock_email_service, mock_counter_manager,
    ):
        """Session 9 updates TK_NUM from SDA lookup when matches found."""
        # Source: records with NULL TK_NUM
        null_tk_df = spark.createDataFrame(
            [("SSN001", None), ("SSN002", None)],
            schema=StructType([
                StructField("PSEUDOSSN", StringType()),
                StructField("TK_NUM", StringType()),
            ]),
        )
        # SDA lookup: TK_NUM values for some SSNs
        sda_df = spark.createDataFrame(
            [("SSN001", "TK_NEW_001"), ("SSN003", "TK_NEW_003")],
            schema=StructType([
                StructField("PSEUDOSSN", StringType()),
                StructField("TK_NUM", StringType()),
            ]),
        )

        call_count = [0]
        def mock_read_jdbc(query, **kwargs):
            call_count[0] += 1
            if "TK_NUM IS NULL" in query:
                return null_tk_df
            return sda_df

        mock_db_manager.read_jdbc.side_effect = mock_read_jdbc

        job = PseudossnJob(
            spark, test_config, mock_db_manager,
            mock_email_service, mock_counter_manager,
        )
        job._session_update_timekeeper_number()

        assert job._job_metrics.sessions[0].status == "SUCCEEDED"
        # Should have found 1 match (SSN001 in both)
        assert job._job_metrics.sessions[0].tgt_success_rows == 1

    def test_update_timekeeper_no_null_records(
        self, spark, test_config, mock_db_manager, mock_email_service, mock_counter_manager,
    ):
        """Session 9 with zero NULL TK_NUM records does nothing."""
        empty_df = spark.createDataFrame(
            [],
            schema=StructType([
                StructField("PSEUDOSSN", StringType()),
                StructField("TK_NUM", StringType()),
            ]),
        )
        mock_db_manager.read_jdbc.side_effect = lambda *a, **kw: empty_df

        job = PseudossnJob(
            spark, test_config, mock_db_manager,
            mock_email_service, mock_counter_manager,
        )
        job._session_update_timekeeper_number()

        assert job._job_metrics.sessions[0].tgt_success_rows == 0


class TestPseudossnSessionCounters:
    """Test _session_counters (covers lines 490-537)."""

    def test_counters_and_email_message(
        self, spark, test_config, mock_db_manager, mock_email_service, mock_counter_manager,
    ):
        """Session 10 writes counters and builds email message."""
        job = PseudossnJob(
            spark, test_config, mock_db_manager,
            mock_email_service, mock_counter_manager,
        )

        # Set state from previous sessions
        job.map_pp_end_year = 2025
        job.map_pp_num = 5
        job.loaded_count = 100
        job.archive_count = 95
        job.sda_count = 20
        job.error_count = 5

        job._session_counters()

        # Verify two counter records written
        assert mock_counter_manager.write_counter.call_count == 2

        # Verify email message built
        assert "100" in job.wf_message
        assert "95" in job.wf_message
        assert "20" in job.wf_message
        assert "5" in job.wf_message
        assert "2025-05" in job.wf_subject
