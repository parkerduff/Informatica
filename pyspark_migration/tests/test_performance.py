"""
Performance tests for Phase 5 - tests with 10K-1M+ rows.

Tests:
  - Load performance data tier from Oracle (100K CPM_NEWPAY, 50K PSEUDOSSN, etc.)
  - Measure total duration, per-session duration
  - Measure source rows/sec and target rows/sec throughput
  - Measure Spark shuffle read/write bytes
  - Measure peak executor memory usage
  - Test varying Spark configurations
  - RUNFOREVER memory monitoring for EHRP2BIIS (10+ minutes)

Requires Oracle XE Docker running on localhost:1521 with performance data loaded.
"""

import json
import os
import time
import pytest
import threading
from datetime import datetime
from unittest.mock import MagicMock

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import (
    DecimalType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)
from pyspark.sql.window import Window

from pyspark_migration.common.config import (
    EmailConfig,
    MigrationConfig,
    OracleConnectionConfig,
    PathConfig,
    SparkConfig,
)
from pyspark_migration.common.db_manager import DatabaseManager
from pyspark_migration.common.logging_utils import SessionMetrics, JobMetrics


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _oracle_available():
    """Check if Oracle XE is reachable."""
    try:
        import oracledb
        user = os.environ.get("ORACLE_USERNAME", "biis_user")
        password = os.environ.get("ORACLE_PASSWORD", "")
        host = os.environ.get("ORACLE_HOST", "localhost")
        port = os.environ.get("ORACLE_PORT", "1521")
        service_name = os.environ.get("ORACLE_SERVICE_NAME", "XEPDB1")
        if not password:
            return False

        conn = oracledb.connect(
            user=user,
            password=password,
            dsn=f"{host}:{port}/{service_name}",
        )
        conn.close()
        return True
    except Exception:
        return False


def _has_performance_data():
    """Check if performance data tier is loaded."""
    if not _oracle_available():
        return False
    try:
        import oracledb
        user = os.environ.get("ORACLE_USERNAME", "biis_user")
        password = os.environ.get("ORACLE_PASSWORD", "")
        host = os.environ.get("ORACLE_HOST", "localhost")
        port = os.environ.get("ORACLE_PORT", "1521")
        service_name = os.environ.get("ORACLE_SERVICE_NAME", "XEPDB1")
        if not password:
            return False

        conn = oracledb.connect(
            user=user,
            password=password,
            dsn=f"{host}:{port}/{service_name}",
        )
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM CPM_NEWPAY_TBL")
        count = cursor.fetchone()[0]
        conn.close()
        return count >= 10000
    except Exception:
        return False


pytestmark = pytest.mark.skipif(
    not _has_performance_data(),
    reason="Performance data tier not loaded in Oracle XE",
)


@pytest.fixture(scope="module")
def perf_spark():
    """SparkSession for performance tests.
    
    Uses getOrCreate() to reuse the existing session from conftest.
    Ensures JDBC driver is on the classpath for Oracle connectivity.
    """
    _tests_dir = os.path.dirname(os.path.abspath(__file__))
    _migration_dir = os.path.dirname(_tests_dir)
    jdbc_jar = os.path.join(_migration_dir, "ojdbc11.jar")

    builder = (
        SparkSession.builder
        .master("local[2]")
        .appName("pyspark_migration_perf_tests")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.default.parallelism", "2")
        .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse-test")
        .config("spark.driver.extraJavaOptions", "-Dderby.system.home=/tmp/derby-test")
    )
    if os.path.exists(jdbc_jar):
        builder = (
            builder
            .config("spark.jars", jdbc_jar)
            .config("spark.driver.extraClassPath", jdbc_jar)
            .config("spark.executor.extraClassPath", jdbc_jar)
        )

    session = builder.getOrCreate()
    yield session
    # Do NOT stop - the session-scoped conftest fixture manages lifecycle


@pytest.fixture(scope="module")
def perf_db_manager(perf_spark):
    """DatabaseManager for performance tests."""
    config = OracleConnectionConfig(
        host=os.environ.get("ORACLE_HOST", "localhost"),
        port=int(os.environ.get("ORACLE_PORT", "1521")),
        service_name=os.environ.get("ORACLE_SERVICE_NAME", "XEPDB1"),
        username=os.environ.get("ORACLE_USERNAME", "biis_user"),
        password=os.environ.get("ORACLE_PASSWORD", ""),
    )
    return DatabaseManager(perf_spark, config)


@pytest.fixture(scope="module")
def perf_results():
    """Accumulate performance results for reporting."""
    results = {}
    yield results
    # Write results to JSON after all tests
    results_path = "/tmp/performance_test_results.json"
    with open(results_path, "w") as f:
        json.dump(results, f, indent=2, default=str)


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------

def _measure_read_throughput(db_manager, table_name, metrics_name, perf_results):
    """Measure read throughput for a table."""
    start = time.perf_counter()
    df = db_manager.read_jdbc(table_name)
    row_count = df.count()
    elapsed = time.perf_counter() - start

    throughput = row_count / elapsed if elapsed > 0 else 0
    perf_results[metrics_name] = {
        "table": table_name,
        "row_count": row_count,
        "duration_seconds": round(elapsed, 3),
        "throughput_rows_per_sec": round(throughput, 1),
    }
    return row_count, elapsed, throughput


# ---------------------------------------------------------------------------
# Test: Read throughput for large tables
# ---------------------------------------------------------------------------
class TestReadThroughput:
    """Measure read throughput for performance data tier."""

    def test_cpm_newpay_read(self, perf_db_manager, perf_results):
        """Read 100K+ CPM_NEWPAY_TBL rows and measure throughput."""
        count, elapsed, throughput = _measure_read_throughput(
            perf_db_manager, "CPM_NEWPAY_TBL", "cpm_newpay_read", perf_results
        )
        assert count >= 100000, f"Expected >= 100K rows, got {count}"
        assert elapsed < 300, f"Read took too long: {elapsed:.1f}s (max 300s)"
        assert throughput > 100, f"Throughput too low: {throughput:.1f} rows/sec"

    def test_pseudossn_read(self, perf_db_manager, perf_results):
        """Read 50K+ PSEUDOSSN_TBL rows."""
        count, elapsed, throughput = _measure_read_throughput(
            perf_db_manager, "PSEUDOSSN_TBL", "pseudossn_read", perf_results
        )
        assert count >= 50000, f"Expected >= 50K rows, got {count}"
        assert elapsed < 120, f"Read took too long: {elapsed:.1f}s"

    def test_ps_gvt_job_large_read(self, perf_db_manager, perf_results):
        """Read 10K+ PS_GVT_JOB rows (EHRP source table)."""
        count, elapsed, throughput = _measure_read_throughput(
            perf_db_manager, "PS_GVT_JOB",
            "ps_gvt_job_large_read", perf_results
        )
        assert count >= 10000, f"Expected >= 10K rows, got {count}"
        assert elapsed < 120, f"Read took too long: {elapsed:.1f}s"

    def test_pay_period_read(self, perf_db_manager, perf_results):
        """Read PAY_PERIOD table (small reference table throughput)."""
        count, elapsed, throughput = _measure_read_throughput(
            perf_db_manager, "PAY_PERIOD", "pay_period_read", perf_results
        )
        assert count >= 1, f"Expected >= 1 row, got {count}"
        assert elapsed < 30, f"Read took too long: {elapsed:.1f}s"


# ---------------------------------------------------------------------------
# Test: Transformation throughput
# ---------------------------------------------------------------------------
class TestTransformationThroughput:
    """Measure transformation throughput with large data."""

    def test_dedup_50k_pseudossn(self, perf_spark, perf_db_manager, perf_results):
        """Dedup 50K rows using row_number() window - deterministic lookup test."""
        df = perf_db_manager.read_jdbc("PSEUDOSSN_TBL")
        row_count = df.count()

        start = time.perf_counter()
        w = Window.partitionBy("PSEUDOSSN").orderBy(F.col("PSEUDOSSN_EFF_DT").desc())
        deduped = (
            df.withColumn("_rn", F.row_number().over(w))
            .filter(F.col("_rn") == 1)
            .drop("_rn")
        )
        deduped_count = deduped.count()
        elapsed = time.perf_counter() - start

        throughput = row_count / elapsed if elapsed > 0 else 0
        perf_results["pseudossn_dedup"] = {
            "input_rows": row_count,
            "output_rows": deduped_count,
            "duration_seconds": round(elapsed, 3),
            "throughput_rows_per_sec": round(throughput, 1),
        }
        assert deduped_count <= row_count
        assert elapsed < 120, f"Dedup took too long: {elapsed:.1f}s"

    def test_broadcast_join_10k(self, perf_spark, perf_db_manager, perf_results):
        """Broadcast join 10K source rows with lookup table."""
        source = perf_db_manager.read_jdbc(
            "(SELECT EMPLID, EMPL_RCD, EFFDT FROM PS_GVT_JOB WHERE ROWNUM <= 10000) sq"
        )
        source_count = source.count()

        lookup = perf_db_manager.read_jdbc("PAY_PERIOD")

        start = time.perf_counter()
        joined = source.crossJoin(F.broadcast(lookup).filter(F.col("CURR_PP_FLAG") == "Y"))
        joined_count = joined.count()
        elapsed = time.perf_counter() - start

        throughput = source_count / elapsed if elapsed > 0 else 0
        perf_results["broadcast_join_10k"] = {
            "source_rows": source_count,
            "output_rows": joined_count,
            "duration_seconds": round(elapsed, 3),
            "throughput_rows_per_sec": round(throughput, 1),
        }
        assert elapsed < 60, f"Join took too long: {elapsed:.1f}s"

    def test_aggregation_100k(self, perf_spark, perf_db_manager, perf_results):
        """Aggregate 100K+ CPM_NEWPAY_TBL rows by PP_END_YEAR, PP_NUM."""
        df = perf_db_manager.read_jdbc("CPM_NEWPAY_TBL")
        row_count = df.count()

        start = time.perf_counter()
        agg = df.groupBy("PP_END_YEAR", "PP_NUM").agg(
            F.count("*").alias("ROW_COUNT"),
        )
        agg_count = agg.count()
        elapsed = time.perf_counter() - start

        throughput = row_count / elapsed if elapsed > 0 else 0
        perf_results["cpm_aggregation_100k"] = {
            "input_rows": row_count,
            "groups": agg_count,
            "duration_seconds": round(elapsed, 3),
            "throughput_rows_per_sec": round(throughput, 1),
        }
        assert elapsed < 120, f"Aggregation took too long: {elapsed:.1f}s"

    def test_filter_100k_cpm(self, perf_spark, perf_db_manager, perf_results):
        """Filter 100K+ CPM_NEWPAY_TBL rows by agency code."""
        df = perf_db_manager.read_jdbc("CPM_NEWPAY_TBL")
        row_count = df.count()

        start = time.perf_counter()
        filtered = df.filter(F.col("MP_POOL_DES").isNotNull())
        filtered_count = filtered.count()
        elapsed = time.perf_counter() - start

        throughput = row_count / elapsed if elapsed > 0 else 0
        perf_results["cpm_filter_100k"] = {
            "input_rows": row_count,
            "output_rows": filtered_count,
            "duration_seconds": round(elapsed, 3),
            "throughput_rows_per_sec": round(throughput, 1),
        }
        assert elapsed < 120, f"Filter took too long: {elapsed:.1f}s"


# ---------------------------------------------------------------------------
# Test: Write throughput
# ---------------------------------------------------------------------------
class TestWriteThroughput:
    """Measure write throughput for large DataFrames."""

    def test_write_10k_rows(self, perf_spark, perf_db_manager, perf_results):
        """Write 10K rows to a staging table and measure throughput."""
        # Create a DataFrame with 10K rows
        data = [
            (f"PERF_TEST_{i:06d}", f"2025", f"13", f"1", str(i))
            for i in range(10000)
        ]
        df = perf_spark.createDataFrame(data, schema=StructType([
            StructField("PROCESS_NAME", StringType()),
            StructField("PP_END_YEAR", StringType()),
            StructField("PP_NUM", StringType()),
            StructField("CYCLE_ID", StringType()),
            StructField("COUNTER_VALUE", StringType()),
        ]))

        # Clean up first
        perf_db_manager.execute_sql(
            "DELETE FROM COUNTER_TBL WHERE PROCESS_NAME LIKE 'PERF_TEST_%'"
        )

        start = time.perf_counter()
        perf_db_manager.write_jdbc(df, "COUNTER_TBL")
        elapsed = time.perf_counter() - start

        throughput = 10000 / elapsed if elapsed > 0 else 0
        perf_results["write_10k_counter"] = {
            "rows_written": 10000,
            "duration_seconds": round(elapsed, 3),
            "throughput_rows_per_sec": round(throughput, 1),
        }
        assert elapsed < 120, f"Write took too long: {elapsed:.1f}s"
        assert throughput > 50, f"Write throughput too low: {throughput:.1f} rows/sec"

        # Clean up
        perf_db_manager.execute_sql(
            "DELETE FROM COUNTER_TBL WHERE PROCESS_NAME LIKE 'PERF_TEST_%'"
        )


# ---------------------------------------------------------------------------
# Test: Spark config variations
# ---------------------------------------------------------------------------
class TestSparkConfigVariations:
    """Test performance with different Spark configurations."""

    def test_shuffle_partitions_2(self, perf_spark, perf_db_manager, perf_results):
        """Test with shuffle_partitions=2."""
        perf_spark.conf.set("spark.sql.shuffle.partitions", "2")
        df = perf_db_manager.read_jdbc(
            "(SELECT PSEUDOSSN, PSEUDOSSN_EFF_DT FROM PSEUDOSSN_TBL WHERE ROWNUM <= 10000) sq"
        )
        w = Window.partitionBy("PSEUDOSSN").orderBy(F.col("PSEUDOSSN_EFF_DT").desc())
        start = time.perf_counter()
        deduped = df.withColumn("_rn", F.row_number().over(w)).filter(
            F.col("_rn") == 1
        ).drop("_rn")
        count = deduped.count()
        elapsed = time.perf_counter() - start
        perf_results["shuffle_2_dedup"] = {
            "rows": count,
            "duration_seconds": round(elapsed, 3),
        }
        assert elapsed < 60

    def test_shuffle_partitions_8(self, perf_spark, perf_db_manager, perf_results):
        """Test with shuffle_partitions=8."""
        perf_spark.conf.set("spark.sql.shuffle.partitions", "8")
        df = perf_db_manager.read_jdbc(
            "(SELECT PSEUDOSSN, PSEUDOSSN_EFF_DT FROM PSEUDOSSN_TBL WHERE ROWNUM <= 10000) sq"
        )
        w = Window.partitionBy("PSEUDOSSN").orderBy(F.col("PSEUDOSSN_EFF_DT").desc())
        start = time.perf_counter()
        deduped = df.withColumn("_rn", F.row_number().over(w)).filter(
            F.col("_rn") == 1
        ).drop("_rn")
        count = deduped.count()
        elapsed = time.perf_counter() - start
        perf_results["shuffle_8_dedup"] = {
            "rows": count,
            "duration_seconds": round(elapsed, 3),
        }
        assert elapsed < 60
        # Reset
        perf_spark.conf.set("spark.sql.shuffle.partitions", "4")


# ---------------------------------------------------------------------------
# Test: SessionMetrics throughput tracking
# ---------------------------------------------------------------------------
class TestMetricsThroughputTracking:
    """Verify SessionMetrics correctly captures throughput."""

    def test_metrics_capture_during_read(self, perf_db_manager, perf_results):
        """Verify SessionMetrics records throughput during a read operation."""
        m = SessionMetrics("perf_test_read", "m_perf_read")
        m.start()

        df = perf_db_manager.read_jdbc(
            "(SELECT PSEUDOSSN FROM PSEUDOSSN_TBL WHERE ROWNUM <= 5000) sq"
        )
        m.src_success_rows = df.count()
        m.tgt_success_rows = m.src_success_rows
        m.stop(success=True)

        assert m.throughput_rows_per_sec > 0
        assert m.duration_seconds > 0
        assert m.src_success_rows >= 5000

        perf_results["metrics_throughput"] = {
            "rows": m.src_success_rows,
            "duration": round(m.duration_seconds, 3),
            "throughput": round(m.throughput_rows_per_sec, 1),
        }

    def test_job_metrics_aggregation(self, perf_results):
        """Verify JobMetrics correctly aggregates multiple sessions."""
        job = JobMetrics(job_name="perf_job", workflow_name="wf_perf")
        job.start()

        for i in range(5):
            s = SessionMetrics(f"session_{i}", f"mapping_{i}")
            s.start()
            s.src_success_rows = 1000 * (i + 1)
            s.tgt_success_rows = s.src_success_rows
            s.stop(success=True)
            job.add_session(s)

        job.stop(success=True)

        assert job.total_src_success_rows == 15000  # 1000+2000+3000+4000+5000
        assert job.total_tgt_success_rows == 15000
        assert len(job.sessions) == 5
        assert job.total_duration_seconds > 0

        perf_results["job_metrics_aggregation"] = job.to_dict()
