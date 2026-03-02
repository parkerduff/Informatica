"""
Job 2: COMPTIME - Compensatory time processing workflow.

Migrated from: Informatica workflow wf_COMPTIME
Complexity: Low-Medium
Sessions (in order):
  1. s_COMPTIME_Current_Pay_Period      -> m_COMPTIME_Current_Pay_Period
  2. s_COMPTIME_Load_COMP_TIME_DAILY_TBL -> m_COMPTIME_Load_COMP_TIME_DAILY_TBL
  3. s_COMPTIME_Build_Message_Counters   -> m_COMPTIME_Build_Message_Counters
  4. email_COMPTIME_Complete             -> email task

Source: U0287D01 (headerless flat file CSV), PAY_PERIOD (Oracle)
Target tables: COMP_TIME_DAILY_TBL (Oracle), COUNTER_TBL (Oracle)
Flat file targets: COMPTIME_MESSAGE_FILE, COMP_TIME_DATE_FILE

Transformation logic:
  - Current PP: Read PAY_PERIOD WHERE CURR_PP_FLAG='Y', set workflow vars
  - Load: Read flat file, filter valid records (IS_NUMBER(SSN)),
          lookup PAY_PERIOD for PP details, convert dates, write to COMP_TIME_DAILY_TBL
  - Counters: Count detail records, write to COUNTER_TBL, build email message

Lookups (all "Use Any Value" -> deterministic):
  - lkp_PAY_PERIOD: WHERE CURR_PP_FLAG='Y' (deterministic - single row expected)
"""

import logging
import os
from datetime import datetime
from typing import Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DateType,
    DecimalType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from pyspark_migration.common.config import MigrationConfig
from pyspark_migration.common.counter_error import CounterErrorManager
from pyspark_migration.common.db_manager import DatabaseManager
from pyspark_migration.common.email_service import EmailService
from pyspark_migration.common.logging_utils import JobMetrics, SessionMetrics

logger = logging.getLogger(__name__)

# Explicit schema for headerless flat file U0287D01
# (Informatica Source Definition: COMPTIME flat file with no header)
U0287D01_SCHEMA = StructType([
    StructField("SSN", StringType(), True),
    StructField("NAME", StringType(), True),
    StructField("CURRENT_ACCT", StringType(), True),
    StructField("CURRENT_ORG", StringType(), True),
    StructField("FLSA_STATUS", StringType(), True),
    StructField("COMP_TIME_CUR_BAL", StringType(), True),
    StructField("COMP_TIME_YEAR_EARNED", StringType(), True),
    StructField("PP_END_DATE", StringType(), True),
    StructField("DAILY_DATE_EARNED", StringType(), True),
    StructField("COMP_TIME_RATE", StringType(), True),
    StructField("COMP_TIME_HOURS", StringType(), True),
    StructField("COMP_TIME_UNDEF", StringType(), True),
])


class CompTimeJob:
    """Migrates Informatica wf_COMPTIME workflow to PySpark.

    Session sequence:
      1. Get current pay period from PAY_PERIOD table
      2. Load flat file records to COMP_TIME_DAILY_TBL
      3. Build counters and email message
      4. Send email notification
    """

    MAPPING_NAME = "m_COMPTIME"
    WORKFLOW_NAME = "wf_COMPTIME"

    def __init__(
        self,
        spark: SparkSession,
        config: MigrationConfig,
        db_manager: DatabaseManager,
        email_service: EmailService,
        counter_manager: Optional[CounterErrorManager] = None,
    ):
        self._spark = spark
        self._config = config
        self._db = db_manager
        self._email = email_service
        self._counter = counter_manager or CounterErrorManager(
            spark, db_manager
        )

        # Workflow variables (replacing $$WF_*, $$MAP_*)
        self.map_pp_year_num: str = ""
        self.map_pp_end_year: int = 0
        self.map_pp_num: int = 0
        self.wf_subject: str = ""
        self.wf_message: str = ""
        self.record_count: int = 0

        self._job_metrics = JobMetrics(
            job_name="CompTime", workflow_name=self.WORKFLOW_NAME
        )
        self._session_start_time = datetime.now()

    def run(self, source_file_path: Optional[str] = None) -> JobMetrics:
        """Execute the full COMPTIME workflow.

        Args:
            source_file_path: Path to U0287D01 flat file.
                Defaults to PathConfig.source_dir/U0287D01.

        Returns:
            JobMetrics with execution results.
        """
        self._session_start_time = datetime.now()
        self._job_metrics.start()

        if source_file_path is None:
            source_file_path = os.path.join(
                self._config.paths.source_dir, "U0287D01"
            )

        try:
            # Session 1: Get current pay period
            self._session_current_pay_period()

            # Session 2: Load flat file to COMP_TIME_DAILY_TBL
            self._session_load_comp_time_daily(source_file_path)

            # Session 3: Build message and counters
            self._session_build_message_counters(source_file_path)

            # Session 4: Send email
            self._email.send_job_success(
                "COMPTIME",
                self._job_metrics,
                extra_message=self.wf_message,
            )

            self._job_metrics.stop(success=True)

        except Exception as exc:
            logger.error("CompTimeJob FAILED: %s", str(exc))
            self._email.send_job_failure(
                "COMPTIME", str(exc), metrics=self._job_metrics
            )
            self._job_metrics.stop(success=False)
            raise

        return self._job_metrics

    def _session_current_pay_period(self) -> None:
        """Session 1: Get current pay period.

        Informatica mapping: m_COMPTIME_Current_Pay_Period
        Source: PAY_PERIOD WHERE CURR_PP_FLAG='Y'
        Transform: exp_Build_Pay_Period formats PP_NUM, sets workflow vars
        Target: COMP_TIME_DATE_FILE (flat file with PAY_PERIOD string)
        """
        metrics = SessionMetrics(
            session_name="s_COMPTIME_Current_Pay_Period",
            mapping_name="m_COMPTIME_Current_Pay_Period",
        ).start()

        try:
            df = self._db.read_jdbc(
                "(SELECT PP_NUM, PP_END_YEAR, PP_START_DTE, PP_END_DTE "
                "FROM PAY_PERIOD WHERE CURR_PP_FLAG = 'Y') sq"
            )

            rows = df.collect()
            metrics.src_success_rows = len(rows)

            if not rows:
                raise RuntimeError(
                    "ABORT: No current pay period found (CURR_PP_FLAG='Y')"
                )
            if len(rows) > 1:
                raise RuntimeError(
                    f"ABORT: Multiple current pay periods found: {len(rows)}"
                )

            row = rows[0]
            self.map_pp_end_year = int(row["PP_END_YEAR"])
            self.map_pp_num = int(row["PP_NUM"])

            # Replaces exp_Build_Pay_Period: format PP_NUM with leading zero
            pp_num_str = str(self.map_pp_num).zfill(2)
            self.map_pp_year_num = f"{self.map_pp_end_year}{pp_num_str}"

            metrics.tgt_success_rows = 1
            metrics.stop(success=True)

        except Exception as exc:
            metrics.record_error(1, str(exc))
            metrics.stop(success=False)
            raise

        finally:
            self._job_metrics.add_session(metrics)

    def _session_load_comp_time_daily(self, source_file_path: str) -> None:
        """Session 2: Load flat file records to COMP_TIME_DAILY_TBL.

        Informatica mapping: m_COMPTIME_Load_COMP_TIME_DAILY_TBL
        Source: U0287D01 (headerless flat file)
        Transforms:
          - exp_Initial: Set CURR_PP_FLAG='Y', check IS_NUMBER(SSN) for valid flag
          - lkp_PAY_PERIOD: Lookup current pay period (deterministic)
          - exp_Convert: Convert date strings, get PP details from lookup
          - fil_Valid_Records: Filter on valid record flag
        Target: COMP_TIME_DAILY_TBL (Oracle INSERT)
        """
        metrics = SessionMetrics(
            session_name="s_COMPTIME_Load_COMP_TIME_DAILY_TBL",
            mapping_name="m_COMPTIME_Load_COMP_TIME_DAILY_TBL",
        ).start()

        try:
            # Read flat file with explicit schema (no header)
            df = (
                self._spark.read.option("header", "false")
                .option("inferSchema", "false")
                .schema(U0287D01_SCHEMA)
                .csv(source_file_path)
            )

            # exp_Initial: IS_NUMBER(SSN) check -> valid record flag
            df = df.withColumn(
                "VALID_RECORD_FLAG",
                F.when(F.col("SSN").rlike("^[0-9]+$"), F.lit(1)).otherwise(F.lit(0)),
            )

            # fil_Valid_Records: filter only valid records
            valid_df = df.filter(F.col("VALID_RECORD_FLAG") == 1)

            # exp_Convert: Convert date fields (YYYYMMDD format)
            valid_df = valid_df.withColumn(
                "PP_END_DATE_CONV",
                F.when(
                    F.to_date(F.col("PP_END_DATE"), "yyyyMMdd").isNotNull(),
                    F.to_date(F.col("PP_END_DATE"), "yyyyMMdd"),
                ),
            ).withColumn(
                "DAILY_DATE_EARNED_CONV",
                F.when(
                    F.to_date(F.col("DAILY_DATE_EARNED"), "yyyyMMdd").isNotNull(),
                    F.to_date(F.col("DAILY_DATE_EARNED"), "yyyyMMdd"),
                ),
            )

            # Add pay period columns from lookup
            pp_num_str = str(self.map_pp_num).zfill(2)
            pp_year_num = int(f"{self.map_pp_end_year}{pp_num_str}")

            target_df = valid_df.select(
                F.lit(self.map_pp_end_year).cast(DecimalType(4, 0)).alias("PP_END_YEAR"),
                F.lit(self.map_pp_num).cast(DecimalType(2, 0)).alias("PP_NUM"),
                F.lit(pp_year_num).cast(DecimalType(6, 0)).alias("PP_YEAR_NUM"),
                F.col("SSN"),
                F.col("NAME"),
                F.col("CURRENT_ACCT"),
                F.col("CURRENT_ORG"),
                F.col("FLSA_STATUS"),
                F.col("COMP_TIME_CUR_BAL"),
                F.col("COMP_TIME_YEAR_EARNED"),
                F.col("PP_END_DATE_CONV").alias("PP_END_DATE"),
                F.col("DAILY_DATE_EARNED_CONV").alias("DAILY_DATE_EARNED"),
                F.col("COMP_TIME_RATE"),
                F.col("COMP_TIME_HOURS"),
                F.col("COMP_TIME_UNDEF"),
            )

            row_count = self._db.write_jdbc(target_df, "COMP_TIME_DAILY_TBL")
            self.record_count = row_count

            metrics.src_success_rows = df.count()
            metrics.tgt_success_rows = row_count
            metrics.stop(success=True)

        except Exception as exc:
            metrics.record_error(2, str(exc))
            metrics.stop(success=False)
            raise

        finally:
            self._job_metrics.add_session(metrics)

    def _session_build_message_counters(self, source_file_path: str) -> None:
        """Session 3: Build counters and email message.

        Informatica mapping: m_COMPTIME_Build_Message_Counters
        Logic:
          - Read flat file, filter detail records (IS_NUMBER(SSN))
          - agg_ALL_RECORDS: COUNT(SSN) -> detail record count
          - Write to COUNTER_TBL
          - Build email subject/message with environment prefix
        """
        metrics = SessionMetrics(
            session_name="s_COMPTIME_Build_Message_Counters",
            mapping_name="m_COMPTIME_Build_Message_Counters",
        ).start()

        try:
            # Write counter record
            self._counter.write_counter(
                process_name="m_COMPTIME_Build_Message_Counters",
                description="Number of detail records from the COMP TIME file.",
                value=self.record_count,
                pp_end_year=str(self.map_pp_end_year),
                pp_num=str(self.map_pp_num),
            )
            metrics.tgt_success_rows = 1

            # Build email message (replaces exp_Build_Message)
            pp_num_str = str(self.map_pp_num).zfill(2)
            env_prefix = self._config.email.environment_prefix

            self.wf_subject = (
                f"{env_prefix}Comp Time File loaded successfully for "
                f"Pay Period: {self.map_pp_end_year}-{pp_num_str}"
            )
            self.wf_message = (
                f"Number of Detail Records from Comp Time file\t= "
                f"{self.record_count}"
            )

            metrics.src_success_rows = 1
            metrics.stop(success=True)

        except Exception as exc:
            metrics.record_error(3, str(exc))
            metrics.stop(success=False)
            raise

        finally:
            self._job_metrics.add_session(metrics)
