"""
Integration Tests for EHRP2BIIS Jobs

Tests preload, main ETL, and afterload workflows.
"""

from unittest.mock import MagicMock, call, patch

import pytest


class TestEHRP2BIISPreload:
    """Tests for EHRP2BIIS preload."""

    @patch("pyspark_migration.jobs.ehrp2biis.preload.execute_sql")
    @patch("pyspark_migration.jobs.ehrp2biis.preload.send_success_email")
    def test_preload_success(self, mock_email, mock_sql):
        """Successful preload sends success email."""
        from pyspark_migration.jobs.ehrp2biis.preload import run

        mock_sql.return_value = None
        run()

        mock_sql.assert_called_once()
        mock_email.assert_called_once()

    @patch("pyspark_migration.jobs.ehrp2biis.preload.execute_sql")
    @patch("pyspark_migration.jobs.ehrp2biis.preload.send_failure_email")
    def test_preload_failure(self, mock_fail_email, mock_sql):
        """Failed preload sends failure email and raises."""
        from pyspark_migration.jobs.ehrp2biis.preload import run

        mock_sql.side_effect = Exception("SQL error")

        with pytest.raises(Exception):
            run()


class TestEHRP2BIISAfterload:
    """Tests for EHRP2BIIS afterload steps."""

    @patch("pyspark_migration.jobs.ehrp2biis.afterload.execute_sql")
    def test_retained_step_cleanup(self, mock_sql):
        """Step 1: retained step cleanup runs UPDATE."""
        from pyspark_migration.jobs.ehrp2biis.afterload import step_retained_step_cleanup

        mock_sql.return_value = 5
        step_retained_step_cleanup()

        mock_sql.assert_called_once()
        sql = mock_sql.call_args[0][1]
        assert "RETND1_STEP_CD" in sql
        assert "NULL" in sql

    @patch("pyspark_migration.jobs.ehrp2biis.afterload.execute_procedure")
    @patch("pyspark_migration.jobs.ehrp2biis.afterload.compile_procedure")
    def test_sequence_number_update(self, mock_compile, mock_exec):
        """Step 2: compile and execute sequence procedure."""
        from pyspark_migration.jobs.ehrp2biis.afterload import step_sequence_number_update

        step_sequence_number_update()

        mock_compile.assert_called_once_with("ORA_BIIS", "UPDATE_SEQUENCE_NUMBER_TBL_P")
        mock_exec.assert_called_once_with("ORA_BIIS", "UPDATE_SEQUENCE_NUMBER_TBL_P")

    @patch("pyspark_migration.jobs.ehrp2biis.afterload.execute_procedure")
    def test_formatting_procedures(self, mock_exec):
        """Step 3: all 5 formatting procedures are executed."""
        from pyspark_migration.jobs.ehrp2biis.afterload import step_formatting_procedures

        step_formatting_procedures()

        assert mock_exec.call_count == 5
        proc_names = [c[0][1] for c in mock_exec.call_args_list]
        assert "HISTDBA.UPDT_ERP2BIIS_CRE8_REMARKS01_P" in proc_names
        assert "HISTDBA.UPDATE_ERP2BIIS_NO900S01_P" in proc_names
        assert "HISTDBA.ERP2BIIS_CRE8_REMARKS_900S01" in proc_names
        assert "HISTDBA.UPDATE_ERP2BIIS_900SONLY01_P" in proc_names
        assert "HISTDBA.UPDT_ORIG_CANCELLED_TRANS01_P" in proc_names

    @patch("pyspark_migration.jobs.ehrp2biis.afterload.truncate_table")
    def test_truncate_staging(self, mock_truncate):
        """Step 7: staging table is truncated."""
        from pyspark_migration.jobs.ehrp2biis.afterload import step_truncate_staging

        step_truncate_staging()

        mock_truncate.assert_called_once_with(
            "ORA_BIIS", "NKNIGHT.NWK_NEW_EHRP_ACTIONS_TBL"
        )
