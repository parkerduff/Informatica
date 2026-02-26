"""
Unit tests for common utility modules.

Tests configuration management, database connections, email service,
logging utilities, and counter/error management.
"""

import os
import unittest
from unittest.mock import patch, MagicMock
from datetime import datetime

from pyspark_migration.common.config import (
    MigrationConfig, OracleConnectionConfig, SparkConfig,
    EmailConfig, PathConfig, load_config_from_env
)
from pyspark_migration.common.logging_utils import SessionMetrics, JobMetrics
from pyspark_migration.common.email_service import EmailService


class TestOracleConnectionConfig(unittest.TestCase):
    """Test Oracle connection configuration."""

    def test_jdbc_url_generation(self):
        config = OracleConnectionConfig(
            host="localhost", port=1521, service_name="BIIS",
            username="user", password="pass"
        )
        url = config.jdbc_url
        self.assertIn("localhost", url)
        self.assertIn("1521", url)
        self.assertIn("BIIS", url)

    def test_default_port(self):
        config = OracleConnectionConfig(
            host="db.example.com", service_name="TEST",
            username="user", password="pass"
        )
        self.assertEqual(config.port, 1521)


class TestSparkConfig(unittest.TestCase):
    """Test Spark configuration."""

    def test_default_values(self):
        config = SparkConfig()
        self.assertEqual(config.app_name, "BIIS_ETL_Migration")
        self.assertEqual(config.executor_memory, "2g")
        self.assertEqual(config.driver_memory, "1g")
        self.assertEqual(config.shuffle_partitions, 200)
        self.assertFalse(config.checkpoint_enabled)

    def test_custom_values(self):
        config = SparkConfig(
            executor_memory="4g",
            shuffle_partitions=100,
            checkpoint_enabled=True,
            checkpoint_dir="/tmp/checkpoints"
        )
        self.assertEqual(config.executor_memory, "4g")
        self.assertEqual(config.shuffle_partitions, 100)
        self.assertTrue(config.checkpoint_enabled)


class TestEmailConfig(unittest.TestCase):
    """Test email configuration."""

    def test_default_recipients(self):
        config = EmailConfig(
            smtp_host="smtp.example.com",
            default_recipients=[
                "peter.chen@hhs.gov",
                "nathan.knight@hhs.gov",
                "marvin.simon@hhs.gov"
            ]
        )
        self.assertEqual(len(config.default_recipients), 3)
        self.assertIn("peter.chen@hhs.gov", config.default_recipients)


class TestPathConfig(unittest.TestCase):
    """Test path configuration."""

    def test_default_paths(self):
        config = PathConfig()
        self.assertIn("BIISINT", config.root_directory)
        self.assertIn("COMPTIME", config.comptime_input_dir)
        self.assertIn("archive", config.comptime_archive_dir)
        self.assertIn("CPM", config.cpm_output_dir)
        self.assertIn("log", config.log_dir)

    def test_custom_root(self):
        config = PathConfig(root_directory="/custom/root")
        self.assertEqual(config.root_directory, "/custom/root")


class TestMigrationConfig(unittest.TestCase):
    """Test migration configuration."""

    def test_env_prefix_prod(self):
        config = MigrationConfig(
            environment="Prod",
            repository_service="Prd_Repo_Srvc"
        )
        self.assertEqual(config.env_prefix, "Prod: ")

    def test_env_prefix_test(self):
        config = MigrationConfig(
            environment="Test",
            repository_service="Test_Repo_Srvc"
        )
        self.assertEqual(config.env_prefix, "Test: ")

    def test_env_prefix_dev(self):
        config = MigrationConfig(
            environment="Dev",
            repository_service="Dev_Repo_Srvc"
        )
        self.assertEqual(config.env_prefix, "Dev: ")

    @patch.dict(os.environ, {
        "ENVIRONMENT": "Test",
        "REPOSITORY_SERVICE": "Test_Repo_Srvc",
        "DB_TARGET_HOST": "testdb.hhs.gov",
        "DB_TARGET_SERVICE": "BIIS_TEST",
        "DB_TARGET_USERNAME": "test_user",
        "DB_TARGET_PASSWORD": "test_pass",
    })
    def test_load_from_env(self):
        config = load_config_from_env()
        self.assertEqual(config.environment, "Test")
        self.assertEqual(config.env_prefix, "Test: ")


class TestSessionMetrics(unittest.TestCase):
    """Test session metrics tracking."""

    def test_initial_state(self):
        metrics = SessionMetrics(
            session_name="test_session",
            mapping_name="test_mapping"
        )
        self.assertEqual(metrics.session_name, "test_session")
        self.assertEqual(metrics.mapping_name, "test_mapping")
        self.assertEqual(metrics.src_success_rows, 0)
        self.assertEqual(metrics.tgt_success_rows, 0)
        self.assertEqual(metrics.status, "NOT_STARTED")

    def test_mark_started(self):
        metrics = SessionMetrics(
            session_name="test_session",
            mapping_name="test_mapping"
        )
        metrics.mark_started()
        self.assertEqual(metrics.status, "RUNNING")
        self.assertIsNotNone(metrics.start_time)

    def test_mark_succeeded(self):
        metrics = SessionMetrics(
            session_name="test_session",
            mapping_name="test_mapping"
        )
        metrics.mark_started()
        metrics.src_success_rows = 100
        metrics.tgt_success_rows = 100
        metrics.mark_succeeded()
        self.assertEqual(metrics.status, "SUCCEEDED")
        self.assertIsNotNone(metrics.end_time)

    def test_mark_failed(self):
        metrics = SessionMetrics(
            session_name="test_session",
            mapping_name="test_mapping"
        )
        metrics.mark_started()
        metrics.mark_failed(error_code=-1, error_msg="Test error")
        self.assertEqual(metrics.status, "FAILED")
        self.assertEqual(metrics.first_error_code, -1)
        self.assertEqual(metrics.first_error_msg, "Test error")

    def test_duration_calculation(self):
        metrics = SessionMetrics(
            session_name="test_session",
            mapping_name="test_mapping"
        )
        metrics.mark_started()
        metrics.mark_succeeded()
        self.assertGreaterEqual(metrics.duration_seconds, 0)


class TestJobMetrics(unittest.TestCase):
    """Test job-level metrics aggregation."""

    def test_add_session(self):
        job_metrics = JobMetrics(
            job_name="test_job",
            workflow_name="test_workflow"
        )
        job_metrics.mark_started()

        session1 = SessionMetrics(
            session_name="s1", mapping_name="m1"
        )
        session1.mark_started()
        session1.src_success_rows = 50
        session1.tgt_success_rows = 50
        session1.mark_succeeded()
        job_metrics.add_session(session1)

        session2 = SessionMetrics(
            session_name="s2", mapping_name="m2"
        )
        session2.mark_started()
        session2.src_success_rows = 30
        session2.tgt_success_rows = 30
        session2.mark_succeeded()
        job_metrics.add_session(session2)

        self.assertEqual(len(job_metrics.sessions), 2)
        self.assertEqual(job_metrics.total_src_rows, 80)
        self.assertEqual(job_metrics.total_tgt_rows, 80)

    def test_summary_generation(self):
        job_metrics = JobMetrics(
            job_name="test_job",
            workflow_name="test_workflow"
        )
        job_metrics.mark_started()
        job_metrics.mark_succeeded()
        summary = job_metrics.summary()
        self.assertIn("test_job", summary)


class TestEmailService(unittest.TestCase):
    """Test email notification service."""

    def test_initialization(self):
        config = EmailConfig(
            smtp_host="smtp.example.com",
            smtp_port=25,
            default_recipients=["test@hhs.gov"]
        )
        service = EmailService(config)
        self.assertEqual(service.config.smtp_host, "smtp.example.com")

    @patch("smtplib.SMTP")
    def test_send_email(self, mock_smtp):
        config = EmailConfig(
            smtp_host="smtp.example.com",
            smtp_port=25,
            default_recipients=["test@hhs.gov"]
        )
        service = EmailService(config)
        service.send_email(
            subject="Test Subject",
            body="Test Body",
            recipients=["test@hhs.gov"]
        )
        # Verify SMTP was used
        mock_smtp.assert_called_once()


if __name__ == "__main__":
    unittest.main()
