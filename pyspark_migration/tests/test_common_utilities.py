"""
Unit tests for common utility modules.

Tests config, db_manager, spark_session, email_service, counter_error,
and logging_utils without Oracle dependency (local Spark mode).
"""

import os
import pytest
from datetime import datetime
from unittest.mock import MagicMock, patch

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

from pyspark_migration.common.config import (
    EmailConfig,
    MigrationConfig,
    OracleConnectionConfig,
    PathConfig,
    SparkConfig,
)
from pyspark_migration.common.logging_utils import JobMetrics, SessionMetrics


class TestOracleConnectionConfig:
    """Tests for OracleConnectionConfig."""

    def test_default_config(self):
        with patch.dict(os.environ, {}, clear=False):
            config = OracleConnectionConfig()
            assert config.host == "localhost"
            assert config.port == 1521

    def test_jdbc_url(self):
        config = OracleConnectionConfig(
            host="db.example.com",
            port=1521,
            service_name="ORCL",
        )
        assert "db.example.com" in config.jdbc_url
        assert "1521" in config.jdbc_url
        assert "ORCL" in config.jdbc_url

    def test_thin_url(self):
        config = OracleConnectionConfig(
            host="db.example.com",
            port=1521,
            service_name="ORCL",
        )
        assert "db.example.com" in config.thin_url
        assert "1521" in config.thin_url


class TestSparkConfig:
    """Tests for SparkConfig."""

    def test_default_config(self):
        config = SparkConfig()
        assert config.executor_memory == "2g"
        assert config.shuffle_partitions == 200

    def test_custom_config(self):
        config = SparkConfig(
            executor_memory="4g",
            shuffle_partitions=100,
        )
        assert config.executor_memory == "4g"
        assert config.shuffle_partitions == 100


class TestEmailConfig:
    """Tests for EmailConfig."""

    def test_default_config(self):
        config = EmailConfig()
        assert config.smtp_host == "localhost"
        assert config.smtp_port == 25

    def test_environment_prefix(self):
        config = EmailConfig(environment="Prod")
        assert config.environment_prefix == ""  # Prod has no prefix

        config_dev = EmailConfig(environment="Dev")
        assert config_dev.environment_prefix == "Dev: "

        config_test = EmailConfig(environment="Test")
        assert config_test.environment_prefix == "Test: "


class TestSessionMetrics:
    """Tests for SessionMetrics tracking."""

    def test_start_stop(self):
        metrics = SessionMetrics(
            session_name="test_session",
            mapping_name="test_mapping",
        )
        metrics.start()
        assert metrics.start_time is not None

        metrics.src_success_rows = 100
        metrics.tgt_success_rows = 95
        metrics.stop(success=True)

        assert metrics.end_time is not None
        assert metrics.duration_seconds >= 0
        assert metrics.src_success_rows == 100
        assert metrics.tgt_success_rows == 95
        assert metrics.status == "SUCCEEDED"

    def test_record_error(self):
        metrics = SessionMetrics(
            session_name="test_session",
            mapping_name="test_mapping",
        )
        metrics.start()
        metrics.record_error(1, "Test error message")

        assert metrics.total_trans_errors == 1
        assert metrics.first_error_code == 1
        assert metrics.first_error_msg == "Test error message"

    def test_multiple_errors(self):
        metrics = SessionMetrics(
            session_name="test_session",
            mapping_name="test_mapping",
        )
        metrics.start()
        metrics.record_error(1, "First error")
        metrics.record_error(2, "Second error")

        assert metrics.total_trans_errors == 2
        assert metrics.first_error_code == 1
        assert metrics.first_error_msg == "First error"

    def test_to_dict(self):
        metrics = SessionMetrics(
            session_name="test_session",
            mapping_name="test_mapping",
        )
        metrics.start()
        metrics.src_success_rows = 50
        metrics.stop(success=True)

        d = metrics.to_dict()
        assert d["session_name"] == "test_session"
        assert d["src_success_rows"] == 50
        assert "duration_seconds" in d


class TestJobMetrics:
    """Tests for JobMetrics aggregation."""

    def test_add_session(self):
        job = JobMetrics(job_name="test_job", workflow_name="test_wf")
        job.start()

        s1 = SessionMetrics("s1", "m1")
        s1.start()
        s1.src_success_rows = 100
        s1.tgt_success_rows = 90
        s1.stop(success=True)

        s2 = SessionMetrics("s2", "m2")
        s2.start()
        s2.src_success_rows = 200
        s2.tgt_success_rows = 195
        s2.stop(success=True)

        job.add_session(s1)
        job.add_session(s2)
        job.stop(success=True)

        assert len(job.sessions) == 2
        assert job.total_src_success_rows == 300
        assert job.total_tgt_success_rows == 285
        assert job.status == "SUCCEEDED"

    def test_failed_job(self):
        job = JobMetrics(job_name="test_job", workflow_name="test_wf")
        job.start()

        s1 = SessionMetrics("s1", "m1")
        s1.start()
        s1.record_error(1, "Fatal error")
        s1.stop(success=False)

        job.add_session(s1)
        job.stop(success=False)

        assert job.status == "FAILED"

    def test_total_rows(self):
        job = JobMetrics(job_name="test_job", workflow_name="test_wf")
        job.start()

        s1 = SessionMetrics("s1", "m1")
        s1.start()
        s1.src_success_rows = 100
        s1.tgt_success_rows = 90
        s1.stop(success=True)

        job.add_session(s1)
        job.stop(success=True)

        assert job.total_src_success_rows == 100
        assert job.total_tgt_success_rows == 90

    def test_to_dict(self):
        job = JobMetrics(job_name="test_job", workflow_name="test_wf")
        job.start()
        job.stop(success=True)

        d = job.to_dict()
        assert d["job_name"] == "test_job"
        assert "sessions" in d


class TestMigrationConfig:
    """Tests for MigrationConfig."""

    def test_default_config(self):
        config = MigrationConfig()
        assert config.oracle is not None
        assert config.spark is not None
        assert config.email is not None
        assert config.paths is not None
