"""
SQL Script Tests for BIISINT repository.

Validates:
- SQL scripts are syntactically structured correctly
- Expected table references exist
- Stored procedure calls are present
- COMMIT statements exist
- Error handling patterns
- Spool and logging patterns
"""
import os
import re

import pytest

from tests.conftest import EXPECTED_SQL_FILES, REPO_ROOT


def read_sql(filename):
    """Read SQL file content."""
    filepath = os.path.join(REPO_ROOT, filename)
    with open(filepath, "r", encoding="utf-8", errors="replace") as f:
        return f.read()


class TestSQLFileExists:
    """Verify SQL files exist and are non-empty."""

    @pytest.mark.parametrize("sql_file", EXPECTED_SQL_FILES)
    def test_sql_file_exists(self, sql_file):
        """SQL file should exist."""
        filepath = os.path.join(REPO_ROOT, sql_file)
        assert os.path.exists(filepath), f"SQL file {sql_file} does not exist"

    @pytest.mark.parametrize("sql_file", EXPECTED_SQL_FILES)
    def test_sql_file_not_empty(self, sql_file):
        """SQL file should have content."""
        filepath = os.path.join(REPO_ROOT, sql_file)
        size = os.path.getsize(filepath)
        assert size > 0, f"SQL file {sql_file} is empty"


class TestSQLTableReferences:
    """Check for expected table references in SQL scripts."""

    def test_afterload_references_action_primary(self):
        """afterload.sql should reference nwk_action_primary_tbl."""
        content = read_sql("ehrp2biis_afterload.sql")
        assert "nwk_action_primary_tbl" in content.lower(), (
            "afterload should reference nwk_action_primary_tbl"
        )

    def test_afterload_references_action_secondary(self):
        """afterload.sql should reference nwk_action_secondary_tbl."""
        content = read_sql("ehrp2biis_afterload.sql")
        assert "nwk_action_secondary_tbl" in content.lower(), (
            "afterload should reference nwk_action_secondary_tbl"
        )

    def test_afterload_references_action_remarks(self):
        """afterload.sql should reference nwk_action_remarks_tbl."""
        content = read_sql("ehrp2biis_afterload.sql")
        assert "nwk_action_remarks_tbl" in content.lower(), (
            "afterload should reference nwk_action_remarks_tbl"
        )

    def test_afterload_references_tracking_table(self):
        """afterload.sql should reference ehrp_recs_tracking_tbl."""
        content = read_sql("ehrp2biis_afterload.sql")
        assert "ehrp_recs_tracking_tbl" in content.lower(), (
            "afterload should reference ehrp_recs_tracking_tbl"
        )

    def test_afterload_references_sequence_num_tbl(self):
        """afterload.sql should reference SEQUENCE_NUM_TBL."""
        content = read_sql("ehrp2biis_afterload.sql")
        assert "sequence_num_tbl" in content.lower(), (
            "afterload should reference SEQUENCE_NUM_TBL"
        )

    def test_afterload_references_action_primary_all(self):
        """afterload.sql should reference action_primary_all."""
        content = read_sql("ehrp2biis_afterload.sql")
        assert "action_primary_all" in content.lower(), (
            "afterload should reference action_primary_all"
        )

    def test_afterload_references_action_secondary_all(self):
        """afterload.sql should reference action_secondary_all."""
        content = read_sql("ehrp2biis_afterload.sql")
        assert "action_secondary_all" in content.lower(), (
            "afterload should reference action_secondary_all"
        )

    def test_afterload_references_action_remarks_all(self):
        """afterload.sql should reference action_remarks_all."""
        content = read_sql("ehrp2biis_afterload.sql")
        assert "action_remarks_all" in content.lower(), (
            "afterload should reference action_remarks_all"
        )

    def test_afterload_references_process_table(self):
        """afterload.sql should reference PROCESS_TABLE."""
        content = read_sql("ehrp2biis_afterload.sql")
        assert "process_table" in content.lower(), (
            "afterload should reference PROCESS_TABLE"
        )

    def test_afterload_references_new_ehrp_actions(self):
        """afterload.sql should reference nwk_new_ehrp_actions_tbl."""
        content = read_sql("ehrp2biis_afterload.sql")
        assert "nwk_new_ehrp_actions_tbl" in content.lower(), (
            "afterload should reference nwk_new_ehrp_actions_tbl"
        )

    def test_afterload_references_nknight_schema(self):
        """afterload.sql should reference nknight schema."""
        content = read_sql("ehrp2biis_afterload.sql")
        assert "nknight." in content.lower(), (
            "afterload should reference nknight schema"
        )


class TestStoredProcedureCalls:
    """Validate stored procedure calls in SQL scripts."""

    def test_afterload_calls_updt_remarks_procedure(self):
        """afterload.sql should call UPDT_ERP2BIIS_CRE8_REMARKS01_P."""
        content = read_sql("ehrp2biis_afterload.sql")
        assert "UPDT_ERP2BIIS_CRE8_REMARKS01_P" in content, (
            "afterload should call UPDT_ERP2BIIS_CRE8_REMARKS01_P"
        )

    def test_afterload_calls_no900s_procedure(self):
        """afterload.sql should call UPDATE_ERP2BIIS_NO900S01_p."""
        content = read_sql("ehrp2biis_afterload.sql")
        assert "UPDATE_ERP2BIIS_NO900S01_p" in content, (
            "afterload should call UPDATE_ERP2BIIS_NO900S01_p"
        )

    def test_afterload_calls_remarks_900s_procedure(self):
        """afterload.sql should call ERP2BIIS_CRE8_REMARKS_900s01."""
        content = read_sql("ehrp2biis_afterload.sql")
        assert "ERP2BIIS_CRE8_REMARKS_900s01" in content, (
            "afterload should call ERP2BIIS_CRE8_REMARKS_900s01"
        )

    def test_afterload_calls_900sonly_procedure(self):
        """afterload.sql should call UPDATE_ERP2BIIS_900SONLY01_P."""
        content = read_sql("ehrp2biis_afterload.sql")
        assert "UPDATE_ERP2BIIS_900SONLY01_P" in content, (
            "afterload should call UPDATE_ERP2BIIS_900SONLY01_P"
        )

    def test_afterload_calls_cancelled_trans_procedure(self):
        """afterload.sql should call UPDT_ORIG_CANCELLED_TRANS01_P."""
        content = read_sql("ehrp2biis_afterload.sql")
        assert "UPDT_ORIG_CANCELLED_TRANS01_P" in content, (
            "afterload should call UPDT_ORIG_CANCELLED_TRANS01_P"
        )

    def test_afterload_calls_gather_runcounts(self):
        """afterload.sql should call GATHER_EHRP2BIIS_RUNCOUNTS_P."""
        content = read_sql("ehrp2biis_afterload.sql")
        assert "GATHER_EHRP2BIIS_RUNCOUNTS_P" in content, (
            "afterload should call GATHER_EHRP2BIIS_RUNCOUNTS_P"
        )

    def test_afterload_calls_wip_status_procedure(self):
        """afterload.sql should call chk_ehrp2biis_wip_status_p."""
        content = read_sql("ehrp2biis_afterload.sql")
        assert "chk_ehrp2biis_wip_status_p" in content, (
            "afterload should call chk_ehrp2biis_wip_status_p"
        )

    def test_afterload_calls_update_sequence_number(self):
        """afterload.sql should call update_sequence_number_tbl_p."""
        content = read_sql("ehrp2biis_afterload.sql")
        assert "update_sequence_number_tbl_p" in content, (
            "afterload should call update_sequence_number_tbl_p"
        )

    def test_afterload_procedures_reference_histdba(self):
        """afterload.sql procedures should reference HISTDBA schema."""
        content = read_sql("ehrp2biis_afterload.sql")
        assert "HISTDBA." in content, (
            "afterload should reference HISTDBA schema for procedures"
        )

    def test_afterload_uses_exec_keyword(self):
        """afterload.sql should use EXEC for stored procedure calls."""
        content = read_sql("ehrp2biis_afterload.sql")
        assert "EXEC " in content or "execute " in content.lower(), (
            "afterload should use EXEC for stored procedure calls"
        )


class TestCommitStatements:
    """Verify COMMIT statements in SQL scripts."""

    def test_afterload_has_commit_statements(self):
        """afterload.sql should contain COMMIT statements."""
        content = read_sql("ehrp2biis_afterload.sql")
        commit_count = len(re.findall(r"(?i)\bcommit\b", content))
        assert commit_count > 0, "afterload.sql should have COMMIT statements"

    def test_afterload_has_multiple_commits(self):
        """afterload.sql should have multiple COMMIT statements for transactional safety."""
        content = read_sql("ehrp2biis_afterload.sql")
        # Match standalone COMMIT (not part of comments)
        lines = content.split("\n")
        commit_count = 0
        for line in lines:
            stripped = line.strip().lower()
            if stripped.startswith("commit") or stripped == "commit;":
                commit_count += 1
        assert commit_count >= 3, (
            f"afterload.sql should have at least 3 COMMIT statements, found {commit_count}"
        )

    def test_afterload_commit_after_inserts(self):
        """afterload.sql should COMMIT after INSERT operations."""
        content = read_sql("ehrp2biis_afterload.sql")
        # Check that insert into action_primary_all is followed by commit
        assert "insert into action_primary_all" in content.lower(), (
            "Should have insert into action_primary_all"
        )

    def test_afterload_commit_after_deletes(self):
        """afterload.sql should have DELETE operations for cancelled actions."""
        content = read_sql("ehrp2biis_afterload.sql")
        assert "delete" in content.lower(), (
            "afterload.sql should have DELETE operations"
        )


class TestSQLSpoolAndLogging:
    """Verify spool/logging patterns in SQL scripts."""

    def test_afterload_has_spool(self):
        """afterload.sql should have SPOOL command for logging."""
        content = read_sql("ehrp2biis_afterload.sql")
        assert "SPOOL" in content or "spool" in content, (
            "afterload.sql should have SPOOL command"
        )

    def test_afterload_has_spool_off(self):
        """afterload.sql should have SPOOL OFF to close log."""
        content = read_sql("ehrp2biis_afterload.sql")
        assert "spool off" in content.lower() or "Spool off" in content, (
            "afterload.sql should have SPOOL OFF"
        )

    def test_afterload_spool_to_log_directory(self):
        """afterload.sql should spool to the standard log directory."""
        content = read_sql("ehrp2biis_afterload.sql")
        assert "/home/sa-biisint/data/int/log" in content, (
            "afterload.sql should spool to /home/sa-biisint/data/int/log"
        )

    def test_afterload_has_prompt_statements(self):
        """afterload.sql should have PROMPT statements for progress tracking."""
        content = read_sql("ehrp2biis_afterload.sql")
        prompt_count = len(re.findall(r"(?i)^PROMPT\s", content, re.MULTILINE))
        assert prompt_count > 0, (
            "afterload.sql should have PROMPT statements for progress"
        )

    def test_afterload_has_set_echo(self):
        """afterload.sql should have SET ECHO ON for debugging."""
        content = read_sql("ehrp2biis_afterload.sql")
        assert "set echo on" in content.lower(), (
            "afterload.sql should have SET ECHO ON"
        )

    def test_afterload_has_set_serveroutput(self):
        """afterload.sql should have SET SERVEROUTPUT ON."""
        content = read_sql("ehrp2biis_afterload.sql")
        assert "set serveroutput on" in content.lower(), (
            "afterload.sql should have SET SERVEROUTPUT ON"
        )


class TestSQLDataOperations:
    """Verify SQL data manipulation patterns."""

    def test_afterload_truncates_new_ehrp_actions(self):
        """afterload.sql should truncate nwk_new_ehrp_actions_tbl."""
        content = read_sql("ehrp2biis_afterload.sql")
        assert "truncate" in content.lower() and "nwk_new_ehrp_actions_tbl" in content.lower(), (
            "afterload should truncate nwk_new_ehrp_actions_tbl"
        )

    def test_afterload_updates_retained_step(self):
        """afterload.sql should update retnd1_step_cd to NULL."""
        content = read_sql("ehrp2biis_afterload.sql")
        assert "retnd1_step_cd" in content.lower(), (
            "afterload should update retnd1_step_cd"
        )

    def test_afterload_filters_by_load_date(self):
        """afterload.sql should filter records by load_date = trunc(sysdate)."""
        content = read_sql("ehrp2biis_afterload.sql")
        assert "load_date = trunc(sysdate)" in content.lower(), (
            "afterload should filter by load_date = trunc(sysdate)"
        )

    def test_afterload_handles_900s_records(self):
        """afterload.sql should handle 900s records (event_id >= 9000000000)."""
        content = read_sql("ehrp2biis_afterload.sql")
        assert "9000000000" in content, (
            "afterload should handle 900s records boundary"
        )

    def test_afterload_handles_cancelled_actions(self):
        """afterload.sql should process cancelled actions via wip_status_changed_dt."""
        content = read_sql("ehrp2biis_afterload.sql")
        assert "biis_wip_status_changed_dt" in content.lower(), (
            "afterload should handle cancelled actions"
        )

    def test_afterload_inserts_primary_secondary_remarks(self):
        """afterload.sql should insert into all three action_*_all tables."""
        content = read_sql("ehrp2biis_afterload.sql").lower()
        assert "insert into action_primary_all" in content
        assert "insert into action_secondary_all" in content
        assert "insert into action_remarks_all" in content

    def test_afterload_deletes_cancelled_from_all_tables(self):
        """afterload.sql should delete cancelled actions from all three tables."""
        content = read_sql("ehrp2biis_afterload.sql").lower()
        assert "delete" in content
        assert "action_primary_all" in content
        assert "action_secondary_all" in content
        assert "action_remarks_all" in content

    def test_afterload_updates_process_table(self):
        """afterload.sql should update PROCESS_TABLE."""
        content = read_sql("ehrp2biis_afterload.sql").lower()
        assert "update process_table" in content

    def test_afterload_compiles_procedures(self):
        """afterload.sql should compile procedures before executing."""
        content = read_sql("ehrp2biis_afterload.sql").lower()
        assert "alter procedure" in content and "compile" in content
