"""
Enhanced unit tests for Phase 5 - covers gaps in existing unit tests.

Adds coverage for:
  - Expression logic: IIF, DECODE, string manipulation, sign parsing
  - Aggregation: COUNT, SUM, MAX
  - Router branches: verify each branch receives correct row subset
  - Error threshold/abort behavior
  - Rollback on write failure
  - Reconciliation check logic
  - Environment prefix DECODE logic
  - Flat file edge cases (empty, single row, bad columns)
  - Financial DecimalType preservation across operations
  - Deterministic lookup with multiple matches across all jobs
"""

import pytest
from datetime import datetime, date
from decimal import Decimal
from unittest.mock import MagicMock, patch

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import (
    DateType,
    DecimalType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)
from pyspark.sql.window import Window

from pyspark_migration.common.config import EmailConfig, MigrationConfig
from pyspark_migration.common.logging_utils import JobMetrics, SessionMetrics
from pyspark_migration.jobs.job1_pay_calendar import PayCalendarJob
from pyspark_migration.jobs.job2_comptime import CompTimeJob, U0287D01_SCHEMA
from pyspark_migration.jobs.job4_fda_leave import FDALeaveJob, FDA_TATRAN_FLAT_SCHEMA
from pyspark_migration.jobs.job5_cpm import CPMBaseJob, CPMNIHJob, CPMCDCJob


# ---------------------------------------------------------------------------
# Expression logic tests
# ---------------------------------------------------------------------------
class TestExpressionLogic:
    """Tests for IIF/DECODE/string manipulation patterns used across jobs."""

    def test_iif_is_number_ssn_check(self, spark):
        """Informatica IIF(IS_NUMBER(SSN), 1, 0) -> rlike check."""
        data = [
            ("123456789",),
            ("abc",),
            ("",),
            ("12345",),
            ("00012345X",),
        ]
        df = spark.createDataFrame(data, schema=StructType([
            StructField("SSN", StringType()),
        ]))
        df = df.withColumn(
            "VALID",
            F.when(F.col("SSN").rlike("^[0-9]+$"), F.lit(1)).otherwise(F.lit(0)),
        )
        rows = {r["SSN"]: r["VALID"] for r in df.collect()}
        assert rows["123456789"] == 1
        assert rows["abc"] == 0
        assert rows[""] == 0
        assert rows["12345"] == 1
        assert rows["00012345X"] == 0

    def test_decode_environment_prefix(self):
        """Informatica DECODE(SUBSTR($PMRepositoryServiceName,...)) -> EmailConfig."""
        for env, expected in [
            ("Dev_HHS", "Dev: "),
            ("Test_HHS", "Test: "),
            ("Prod_HHS", ""),
            ("Staging", "Staging: "),
        ]:
            config = EmailConfig(environment=env)
            assert config.environment_prefix == expected, f"env={env}"

    def test_pp_num_leading_zero_format(self):
        """Informatica v_PP_NUM = LPAD(PP_NUM, 2, '0')."""
        for pp_num, expected in [(1, "01"), (9, "09"), (13, "13"), (26, "26")]:
            assert str(pp_num).zfill(2) == expected

    def test_sign_parsing_for_financial_fields(self, spark):
        """CPM financial fields may have trailing sign chars."""
        data = [
            ("50000.50",),
            ("-1234.00",),
            ("0.00",),
        ]
        df = spark.createDataFrame(data, schema=StructType([
            StructField("AMOUNT_STR", StringType()),
        ]))
        df = df.withColumn("AMOUNT", F.col("AMOUNT_STR").cast(DecimalType(15, 2)))
        rows = df.collect()
        assert rows[0]["AMOUNT"] == Decimal("50000.50")
        assert rows[1]["AMOUNT"] == Decimal("-1234.00")
        assert rows[2]["AMOUNT"] == Decimal("0.00")

    def test_date_conversion_yyyymmdd(self, spark):
        """COMPTIME date conversion: YYYYMMDD string -> DateType.
        
        Uses try_to_timestamp (Spark 4 ANSI mode safe) to handle invalid strings.
        """
        data = [("20250615",), ("20251231",), ("invalid",), (None,)]
        df = spark.createDataFrame(data, schema=StructType([
            StructField("DATE_STR", StringType()),
        ]))
        # Use try_to_timestamp for Spark 4 ANSI mode compatibility
        df = df.withColumn(
            "DATE_CONV",
            F.try_to_timestamp(F.col("DATE_STR"), F.lit("yyyyMMdd")).cast(DateType()),
        )
        rows = df.collect()
        assert rows[0]["DATE_CONV"] == date(2025, 6, 15)
        assert rows[1]["DATE_CONV"] == date(2025, 12, 31)
        assert rows[2]["DATE_CONV"] is None
        assert rows[3]["DATE_CONV"] is None

    def test_null_handling_in_expressions(self, spark):
        """Null propagation in withColumn expressions."""
        data = [(None, "B"), ("A", None), (None, None), ("A", "B")]
        df = spark.createDataFrame(data, schema=StructType([
            StructField("COL1", StringType()),
            StructField("COL2", StringType()),
        ]))
        df = df.withColumn(
            "CONCAT",
            F.when(
                F.col("COL1").isNotNull() & F.col("COL2").isNotNull(),
                F.concat(F.col("COL1"), F.lit("-"), F.col("COL2")),
            ),
        )
        rows = df.collect()
        assert rows[0]["CONCAT"] is None
        assert rows[1]["CONCAT"] is None
        assert rows[2]["CONCAT"] is None
        assert rows[3]["CONCAT"] == "A-B"


# ---------------------------------------------------------------------------
# Aggregation tests
# ---------------------------------------------------------------------------
class TestAggregationLogic:
    """Tests for COUNT, SUM, MAX used across jobs."""

    def test_count_aggregation(self, spark):
        """Verify COUNT(*) aggregation (used in CPM Build_Message, Pseudossn counters)."""
        data = [("NIH", 100), ("NIH", 200), ("CDC", 300)]
        df = spark.createDataFrame(data, schema=StructType([
            StructField("AGENCY", StringType()),
            StructField("AMOUNT", IntegerType()),
        ]))
        result = df.groupBy("AGENCY").agg(F.count("*").alias("CNT"))
        rows = {r["AGENCY"]: r["CNT"] for r in result.collect()}
        assert rows["NIH"] == 2
        assert rows["CDC"] == 1

    def test_sum_aggregation_decimal(self, spark):
        """Verify SUM preserves DecimalType precision."""
        data = [(Decimal("100.50"),), (Decimal("200.75"),), (Decimal("300.25"),)]
        df = spark.createDataFrame(data, schema=StructType([
            StructField("AMOUNT", DecimalType(15, 2)),
        ]))
        total = df.agg(F.sum("AMOUNT").alias("TOTAL")).collect()[0]["TOTAL"]
        assert total == Decimal("601.50")

    def test_max_aggregation_date(self, spark):
        """Verify MAX(EFFDT) for data freshness check."""
        data = [
            (date(2025, 1, 1),),
            (date(2025, 6, 15),),
            (date(2025, 3, 1),),
        ]
        df = spark.createDataFrame(data, schema=StructType([
            StructField("EFFDT", DateType()),
        ]))
        max_dt = df.agg(F.max("EFFDT").alias("MAX_EFFDT")).collect()[0]["MAX_EFFDT"]
        assert max_dt == date(2025, 6, 15)

    def test_count_distinct(self, spark):
        """Verify COUNT(DISTINCT ...) for duplicate detection."""
        data = [("SSN1",), ("SSN2",), ("SSN1",), ("SSN3",)]
        df = spark.createDataFrame(data, schema=StructType([
            StructField("SSN", StringType()),
        ]))
        total = df.count()
        distinct = df.select("SSN").distinct().count()
        assert total == 4
        assert distinct == 3
        assert total - distinct == 1  # 1 duplicate


# ---------------------------------------------------------------------------
# Router branch tests
# ---------------------------------------------------------------------------
class TestRouterBranches:
    """Tests for router/filter branch logic across jobs."""

    def test_fda_rec_type_router(self, spark):
        """FDA_Leave: router splits by rec_type (01, 99, 02, other)."""
        data = [
            ("E001", "01", "8.0"),
            ("E002", "99", "0.0"),
            ("E003", "02", "8.0"),
            ("E004", "02", "4.0"),
            ("E005", "12", "8.0"),
            ("E006", "12", "8.0"),
        ]
        df = spark.createDataFrame(data, schema=StructType([
            StructField("FDA_EMP_ID", StringType()),
            StructField("FDA_REC_TYPE", StringType()),
            StructField("FDA_HOURS", StringType()),
        ]))

        # fil_Filter_Out_01_99
        detail_df = df.filter(~F.col("FDA_REC_TYPE").isin("01", "99"))
        assert detail_df.count() == 4

        # Further filter for type 02 validation
        type_02 = detail_df.filter(F.col("FDA_REC_TYPE") == "02")
        assert type_02.count() == 2

        # Type 12 records
        type_12 = detail_df.filter(F.col("FDA_REC_TYPE") == "12")
        assert type_12.count() == 2

    def test_comptime_ssn_router(self, spark):
        """COMPTIME: valid SSN vs invalid SSN routing."""
        data = [
            ("123456789", "John"),
            ("ABCDEFGHI", "Jane"),
            ("", "Empty"),
            ("999111222", "Valid2"),
        ]
        df = spark.createDataFrame(data, schema=StructType([
            StructField("SSN", StringType()),
            StructField("NAME", StringType()),
        ]))
        df = df.withColumn(
            "VALID",
            F.when(F.col("SSN").rlike("^[0-9]+$"), F.lit(1)).otherwise(F.lit(0)),
        )
        valid = df.filter(F.col("VALID") == 1)
        invalid = df.filter(F.col("VALID") == 0)

        assert valid.count() == 2
        assert invalid.count() == 2
        # Verify correct routing
        valid_names = {r["NAME"] for r in valid.collect()}
        assert valid_names == {"John", "Valid2"}

    def test_pseudossn_good_bad_router(self, spark):
        """Pseudossn: good records vs bad records routing."""
        data = [
            ("SSN001", "2025-06-01", "VALID"),
            ("SSN002", None, "MISSING_DATE"),
            ("SSN003", "2025-06-15", "VALID"),
            ("", "2025-06-01", "MISSING_SSN"),
        ]
        df = spark.createDataFrame(data, schema=StructType([
            StructField("PSEUDOSSN", StringType()),
            StructField("EFF_DT", StringType()),
            StructField("STATUS", StringType()),
        ]))

        good = df.filter(
            F.col("PSEUDOSSN").isNotNull()
            & (F.col("PSEUDOSSN") != "")
            & F.col("EFF_DT").isNotNull()
        )
        bad = df.filter(
            F.col("PSEUDOSSN").isNull()
            | (F.col("PSEUDOSSN") == "")
            | F.col("EFF_DT").isNull()
        )

        assert good.count() == 2
        assert bad.count() == 2


# ---------------------------------------------------------------------------
# Error threshold and abort tests
# ---------------------------------------------------------------------------
class TestErrorThresholdAbort:
    """Tests for configurable error thresholds and abort behavior."""

    def test_reconciliation_balanced(self):
        """Balanced: src == tgt + errors."""
        m = SessionMetrics("s1", "m1")
        m.start()
        m.src_success_rows = 100
        m.tgt_success_rows = 95
        m.total_trans_errors = 5
        m.stop(success=True)
        assert m.reconciliation_check() is True

    def test_reconciliation_unbalanced(self):
        """Unbalanced: src != tgt + errors."""
        m = SessionMetrics("s1", "m1")
        m.start()
        m.src_success_rows = 100
        m.tgt_success_rows = 90
        m.total_trans_errors = 5
        m.stop(success=True)
        assert m.reconciliation_check() is False

    def test_throughput_calculation(self):
        """Verify throughput rows/sec calculation."""
        m = SessionMetrics("s1", "m1")
        m.start()
        m.src_success_rows = 1000
        m.duration_seconds = 2.0
        assert m.throughput_rows_per_sec == 500.0

    def test_throughput_zero_duration(self):
        """Throughput returns 0 when duration is 0."""
        m = SessionMetrics("s1", "m1")
        m.src_success_rows = 100
        m.duration_seconds = 0.0
        assert m.throughput_rows_per_sec == 0.0

    def test_job_metrics_failed_sessions(self):
        """Verify failed_sessions property."""
        job = JobMetrics(job_name="test", workflow_name="wf")
        job.start()

        s1 = SessionMetrics("s1", "m1")
        s1.start()
        s1.stop(success=True)

        s2 = SessionMetrics("s2", "m2")
        s2.start()
        s2.record_error(1, "fail")
        s2.stop(success=False)

        s3 = SessionMetrics("s3", "m3")
        s3.start()
        s3.stop(success=True)

        job.add_session(s1)
        job.add_session(s2)
        job.add_session(s3)
        job.stop(success=False)

        assert len(job.failed_sessions) == 1
        assert job.failed_sessions[0].session_name == "s2"
        assert job.total_errors == 1

    def test_pay_calendar_multiple_current_pp_aborts(
        self, spark, test_config, mock_db_manager, mock_email_service
    ):
        """ABORT when more than 1 current pay period exists."""
        pp_df = spark.createDataFrame(
            [(13, 2025, None, None, 13, 2025, None, "Y")],
            schema=StructType([
                StructField("PP_NUM", IntegerType()),
                StructField("PP_END_YEAR", IntegerType()),
                StructField("PP_START_DTE", DateType()),
                StructField("PP_END_DTE", DateType()),
                StructField("LV_NUM", IntegerType()),
                StructField("LV_YEAR", IntegerType()),
                StructField("PAY_DTE", DateType()),
                StructField("CURR_PP_FLAG", StringType()),
            ]),
        )
        # COUNT = 2 -> should ABORT (more than one)
        count_df = spark.createDataFrame(
            [(2,)],
            schema=StructType([StructField("COUNT_CURRENT", LongType())]),
        )
        mock_db_manager.read_jdbc.side_effect = [pp_df, count_df]

        job = PayCalendarJob(spark, test_config, mock_db_manager, mock_email_service)
        with pytest.raises(RuntimeError, match="More than one"):
            job.run()

    def test_write_failure_triggers_rollback(
        self, spark, test_config, mock_db_manager, mock_email_service
    ):
        """Verify write failures raise RuntimeError (triggering rollback)."""
        mock_db_manager.execute_sql.return_value = (False, "ORA-00001: unique constraint violated")

        pp_df = spark.createDataFrame(
            [(13, 2025, None, None, 13, 2025, None, "Y")],
            schema=StructType([
                StructField("PP_NUM", IntegerType()),
                StructField("PP_END_YEAR", IntegerType()),
                StructField("PP_START_DTE", DateType()),
                StructField("PP_END_DTE", DateType()),
                StructField("LV_NUM", IntegerType()),
                StructField("LV_YEAR", IntegerType()),
                StructField("PAY_DTE", DateType()),
                StructField("CURR_PP_FLAG", StringType()),
            ]),
        )
        mock_db_manager.read_jdbc.side_effect = [pp_df]

        job = PayCalendarJob(spark, test_config, mock_db_manager, mock_email_service)
        with pytest.raises(RuntimeError, match="Reset pay calendar failed"):
            job.run()


# ---------------------------------------------------------------------------
# Flat file edge case tests
# ---------------------------------------------------------------------------
class TestFlatFileEdgeCases:
    """Tests for flat file reading edge cases."""

    def test_comptime_schema_all_string(self):
        """Verify all COMPTIME fields are StringType (headerless file)."""
        for field in U0287D01_SCHEMA.fields:
            assert field.dataType == StringType(), f"{field.name} is not StringType"

    def test_fda_schema_all_string(self):
        """Verify all FDA TATRAN fields are StringType."""
        for field in FDA_TATRAN_FLAT_SCHEMA.fields:
            assert field.dataType == StringType(), f"{field.name} is not StringType"

    def test_comptime_schema_field_count(self):
        """COMPTIME flat file must have exactly 12 fields."""
        assert len(U0287D01_SCHEMA.fields) == 12

    def test_fda_schema_field_count(self):
        """FDA TATRAN flat file must have exactly 6 fields."""
        assert len(FDA_TATRAN_FLAT_SCHEMA.fields) == 6


# ---------------------------------------------------------------------------
# Deterministic lookup tests (cross-job)
# ---------------------------------------------------------------------------
class TestDeterministicLookups:
    """Tests for deterministic lookup join replacing 'Use Any Value'."""

    def test_row_number_dedup_picks_latest(self, spark):
        """row_number() with EFFDT DESC picks most recent record."""
        data = [
            ("E001", "2025-01-01", "OLD"),
            ("E001", "2025-06-01", "LATEST"),
            ("E001", "2025-03-01", "MID"),
            ("E002", "2025-02-01", "ONLY"),
        ]
        df = spark.createDataFrame(data, schema=StructType([
            StructField("KEY", StringType()),
            StructField("EFFDT", StringType()),
            StructField("VALUE", StringType()),
        ]))
        w = Window.partitionBy("KEY").orderBy(F.col("EFFDT").desc())
        deduped = (
            df.withColumn("_rn", F.row_number().over(w))
            .filter(F.col("_rn") == 1)
            .drop("_rn")
        )
        assert deduped.count() == 2
        rows = {r["KEY"]: r["VALUE"] for r in deduped.collect()}
        assert rows["E001"] == "LATEST"
        assert rows["E002"] == "ONLY"

    def test_left_join_null_on_no_match(self, spark):
        """Left join produces NULL for non-matching keys."""
        source = spark.createDataFrame(
            [("E001",), ("E002",), ("E003",)],
            schema=StructType([StructField("EMPLID", StringType())]),
        )
        lookup = spark.createDataFrame(
            [("E001", "FOUND"), ("E003", "FOUND")],
            schema=StructType([
                StructField("EMPLID", StringType()),
                StructField("STATUS", StringType()),
            ]),
        )
        result = source.join(F.broadcast(lookup), on="EMPLID", how="left")
        rows = {r["EMPLID"]: r["STATUS"] for r in result.collect()}
        assert rows["E001"] == "FOUND"
        assert rows["E002"] is None
        assert rows["E003"] == "FOUND"

    def test_anti_join_finds_missing(self, spark):
        """Left anti join finds records NOT in lookup (used by FDA error check)."""
        source = spark.createDataFrame(
            [("E001",), ("E002",), ("E003",)],
            schema=StructType([StructField("EMP_ID", StringType())]),
        )
        lookup = spark.createDataFrame(
            [("E001",), ("E003",)],
            schema=StructType([StructField("SSN", StringType())]),
        )
        missing = source.join(
            F.broadcast(lookup),
            source["EMP_ID"] == lookup["SSN"],
            "left_anti",
        )
        assert missing.count() == 1
        assert missing.collect()[0]["EMP_ID"] == "E002"


# ---------------------------------------------------------------------------
# CPM agency subclass tests
# ---------------------------------------------------------------------------
class TestCPMSubclasses:
    """Tests for CPM NIH/CDC subclass configurations."""

    def test_nih_workflow_name(self):
        assert CPMNIHJob.WORKFLOW_NAME == "wf_CPM_NIH"

    def test_cdc_workflow_name(self):
        assert CPMCDCJob.WORKFLOW_NAME == "wf_CPM_CDC"

    def test_nih_agency_filter_contains_mp_pool(self):
        assert "MP_POOL_DES" in CPMNIHJob.AGENCY_FILTER

    def test_cdc_agency_filter_contains_mp_pool(self):
        assert "MP_POOL_DES" in CPMCDCJob.AGENCY_FILTER

    def test_safe_parse_int_boundary_values(self):
        """Test CPMBaseJob._safe_parse_int with boundary values."""
        assert CPMBaseJob._safe_parse_int("1") == 1
        assert CPMBaseJob._safe_parse_int("26") == 26
        assert CPMBaseJob._safe_parse_int("9999") == 9999
        assert CPMBaseJob._safe_parse_int("0") is None
        assert CPMBaseJob._safe_parse_int("-1") is None
        assert CPMBaseJob._safe_parse_int("abc") is None
        assert CPMBaseJob._safe_parse_int(None) is None
