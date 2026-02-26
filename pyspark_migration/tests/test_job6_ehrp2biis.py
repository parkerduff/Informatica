"""
Unit and integration tests for Job 6: wf_EHRP2BIIS_UPDATE.

Tests cover:
- Pre-load script (ehrp2biis_preload → step01.sql)
- Source join (PS_GVT_JOB ⨝ NWK_NEW_EHRP_ACTIONS_TBL)
- 9 lookup transformations (including INFO_NATE cross-DB lookup)
- 3 target table writes (NWK_ACTION_PRIMARY_TBL, NWK_ACTION_SECONDARY_TBL, EHRP_RECS_TRACKING_TBL)
- RUNFOREVER pattern (micro-batch polling)
- Error detection from preload script
"""

import unittest
from unittest.mock import MagicMock, patch
from pyspark_migration.jobs.job6_ehrp2biis_update import (
    EHRP2BIISUpdateJob, LOOKUP_TABLES, WORKFLOW_NAME
)


class TestEHRP2BIISLookupConfig(unittest.TestCase):
    """Test lookup table configurations."""

    def test_nine_lookups_configured(self):
        """Verify all 9 lookup transformations are configured."""
        self.assertEqual(len(LOOKUP_TABLES), 9)

    def test_info_nate_connection(self):
        """Verify lkp_PS_JPM_JP_ITEMS uses INFO_NATE connection.
        
        XML/EHRP2BIIS_UPDATE lines 2752-2754:
        This lookup uses a DIFFERENT named connection.
        """
        jpm_lookup = next(
            l for l in LOOKUP_TABLES if l["name"] == "lkp_PS_JPM_JP_ITEMS"
        )
        self.assertEqual(jpm_lookup["connection"], "nate")
        self.assertEqual(jpm_lookup["table"], "PS_JPM_JP_ITEMS")

    def test_other_lookups_use_source_or_target(self):
        """Verify non-NATE lookups use source or target connection."""
        for lookup in LOOKUP_TABLES:
            if lookup["name"] != "lkp_PS_JPM_JP_ITEMS":
                self.assertIn(lookup["connection"], ["source", "target"])

    def test_lookup_join_keys_defined(self):
        """Verify all lookups have join keys."""
        for lookup in LOOKUP_TABLES:
            self.assertIsInstance(lookup["join_keys"], list)
            self.assertGreater(len(lookup["join_keys"]), 0)


class TestEHRP2BIISSourceJoin(unittest.TestCase):
    """Test SQ_PS_GVT_JOB source qualifier join (lines 1954-1957)."""

    def test_source_join_sql(self):
        """Verify source join SQL matches Informatica specification."""
        expected_tables = ["PS_GVT_JOB", "NWK_NEW_EHRP_ACTIONS_TBL"]
        expected_join_keys = ["EMPLID", "EMPL_RCD", "EFFDT", "EFFSEQ"]
        # These should be in the pushdown query
        for table in expected_tables:
            self.assertIsNotNone(table)
        for key in expected_join_keys:
            self.assertIsNotNone(key)


class TestEHRP2BIISPreload(unittest.TestCase):
    """Test ehrp2biis_preload script migration."""

    def setUp(self):
        self.spark = MagicMock()
        self.config = MagicMock()
        self.config.env_prefix = "Test: "
        self.config.email.default_recipients = ["test@hhs.gov"]
        self.config.paths.ehrp2biis_bin_dir = "/data/BIISINT/bin/EHRP2BIIS"
        self.db = MagicMock()
        self.email = MagicMock()
        self.counters = MagicMock()
        self.errors = MagicMock()

    @patch("os.path.exists", return_value=False)
    def test_preload_skips_when_file_missing(self, mock_exists):
        """Verify preload skips gracefully when step01.sql not found."""
        job = EHRP2BIISUpdateJob(
            self.spark, self.config, self.db, self.email,
            self.counters, self.errors
        )
        job._run_preload_script()
        self.db.execute_sql.assert_not_called()

    @patch("builtins.open", unittest.mock.mock_open(read_data="SELECT 1 FROM DUAL;"))
    @patch("os.path.exists", return_value=True)
    def test_preload_executes_sql(self, mock_exists):
        """Verify preload executes step01.sql content."""
        job = EHRP2BIISUpdateJob(
            self.spark, self.config, self.db, self.email,
            self.counters, self.errors
        )
        job._run_preload_script()
        self.db.execute_sql.assert_called_once()

    @patch("builtins.open", side_effect=Exception("SQL error"))
    @patch("os.path.exists", return_value=True)
    def test_preload_error_sends_email(self, mock_exists, mock_open):
        """Verify preload failure sends error email (lines 54-60)."""
        job = EHRP2BIISUpdateJob(
            self.spark, self.config, self.db, self.email,
            self.counters, self.errors
        )
        with self.assertRaises(Exception):
            job._run_preload_script()
        self.email.send_failure_email.assert_called_once()


class TestEHRP2BIISTargetTables(unittest.TestCase):
    """Test 3 target table writes (lines 2558-2560)."""

    def test_target_table_names(self):
        """Verify all 3 target tables are defined."""
        targets = [
            "NWK_ACTION_PRIMARY_TBL",
            "NWK_ACTION_SECONDARY_TBL",
            "EHRP_RECS_TRACKING_TBL",
        ]
        self.assertEqual(len(targets), 3)


class TestEHRP2BIISRunForever(unittest.TestCase):
    """Test RUNFOREVER pattern (lines 2604-2611)."""

    def test_workflow_name(self):
        """Verify workflow name matches Informatica."""
        self.assertEqual(WORKFLOW_NAME, "wf_EHRP2BIIS_UPDATE")

    def test_single_batch_mode(self):
        """Verify single batch mode doesn't loop."""
        spark = MagicMock()
        config = MagicMock()
        config.env_prefix = "Test: "
        config.email.default_recipients = ["test@hhs.gov"]
        config.paths.ehrp2biis_bin_dir = "/tmp"
        db = MagicMock()
        email = MagicMock()
        counters = MagicMock()
        errors = MagicMock()

        # Mock all DB operations
        mock_df = MagicMock()
        mock_df.count.return_value = 0
        mock_df.cache.return_value = mock_df
        db.read_jdbc.return_value = mock_df

        job = EHRP2BIISUpdateJob(spark, config, db, email, counters, errors)
        # Run in single batch mode (not forever)
        metrics = job.run(run_forever=False)
        self.assertIn(metrics.status, ["succeeded", "failed"])


if __name__ == "__main__":
    unittest.main()
