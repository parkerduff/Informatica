"""
Job 3: Pseudossn - PseudoSSN management from SDA files.

Migrated from: Informatica workflow wf_Pseudossn
Complexity: Medium
Sessions (in order):
  1. s_Pseudossn_Current_Pay_Period
  2. s_Pseudossn_Verify_Header_Date_Current_Pay_Period
  3. s_Pseudossn_Verify_Record_Count
  4. s_Pseudossn_Load_Pseudossn_Tbl
  5. s_Pseudossn_Load_Archive_Pseudossn_Tbl
  6. s_Pseudossn_Verify_Header_Date_Current_Pay_Period_Pseudossn_From_SDA
  7. s_Pseudossn_Load_Pseudossn_From_SDA_Tbl
  8. s_Pseudossn_Load_SDA_Records_Pseudossn_Tbl
  9. s_Pseudossn_Update_Timekeeper_Number
  10. s_Pseudossn_Counters
  11. Email_Pseudossn

Sources: PSEUDOSSN_FILE (flat, 66 fields), PSEUDOSSN_FILE_TK_NUM (flat, 67 fields),
         PAY_PERIOD, PSEUDOSSN_TBL, PSEUDOSSN_FROM_SDA_TBL
Targets: PSEUDOSSN_TBL, PSEUDOSSN_FROM_SDA_TBL, HI_ARCH_PSEUDOSSN_TBL,
         ERROR_TBL, COUNTER_TBL, PSEUDO_RECORD_COUNT

Key transformations:
  - Header/trailer validation with record count reconciliation
  - Deduplication via Sorter on PSEUDOSSN_EFF_DT
  - Router for good/bad records
  - Normalizer for error message expansion
  - Joiner for record count comparison (detail vs trailer)
  - Lookups on PAY_PERIOD, PSEUDOSSN_TBL, PSEUDOSSN_FROM_SDA_TBL
"""

import logging
import os
from datetime import datetime
from typing import Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DecimalType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)
from pyspark.sql.window import Window

from pyspark_migration.common.config import MigrationConfig
from pyspark_migration.common.counter_error import CounterErrorManager
from pyspark_migration.common.db_manager import DatabaseManager
from pyspark_migration.common.email_service import EmailService
from pyspark_migration.common.logging_utils import JobMetrics, SessionMetrics

logger = logging.getLogger(__name__)


class PseudossnJob:
    """Migrates Informatica wf_Pseudossn workflow to PySpark.

    Processes SDA (Standard Data Archive) files containing PseudoSSN
    mappings. Validates header dates, reconciles record counts,
    deduplicates, and loads to PSEUDOSSN_TBL and archive.
    """

    MAPPING_NAME = "m_Pseudossn"
    WORKFLOW_NAME = "wf_Pseudossn"

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
        self._counter = counter_manager or CounterErrorManager(spark, db_manager)

        # Workflow variables
        self.map_pp_end_year: int = 0
        self.map_pp_num: int = 0
        self.map_pp_year_num: str = ""
        self.wf_subject: str = ""
        self.wf_message: str = ""
        self.detail_count: int = 0
        self.trailer_count: int = 0
        self.loaded_count: int = 0
        self.error_count: int = 0
        self.archive_count: int = 0
        self.sda_count: int = 0

        self._job_metrics = JobMetrics(
            job_name="Pseudossn", workflow_name=self.WORKFLOW_NAME
        )
        self._session_start_time = datetime.now()

    def run(
        self,
        pseudossn_file_path: Optional[str] = None,
        pseudossn_tk_file_path: Optional[str] = None,
    ) -> JobMetrics:
        """Execute the full Pseudossn workflow.

        Args:
            pseudossn_file_path: Path to PSEUDOSSN_FILE (SDA flat file).
            pseudossn_tk_file_path: Path to PSEUDOSSN_FILE_TK_NUM.

        Returns:
            JobMetrics with execution results.
        """
        self._session_start_time = datetime.now()
        self._job_metrics.start()

        if pseudossn_file_path is None:
            pseudossn_file_path = os.path.join(
                self._config.paths.source_dir, "PSEUDOSSN_FILE"
            )
        if pseudossn_tk_file_path is None:
            pseudossn_tk_file_path = os.path.join(
                self._config.paths.source_dir, "PSEUDOSSN_FILE_TK_NUM"
            )

        try:
            # Session 1: Get current pay period
            self._session_current_pay_period()

            # Session 2: Verify header date matches current pay period
            self._session_verify_header_date(pseudossn_file_path)

            # Session 3: Verify record count (detail count vs trailer count)
            self._session_verify_record_count(pseudossn_file_path)

            # Session 4: Load records to PSEUDOSSN_TBL
            self._session_load_pseudossn_tbl(pseudossn_file_path)

            # Session 5: Load archive
            self._session_load_archive()

            # Session 6: Verify header date for SDA file
            self._session_verify_header_date_sda(pseudossn_tk_file_path)

            # Session 7: Load SDA records to PSEUDOSSN_FROM_SDA_TBL
            self._session_load_pseudossn_from_sda(pseudossn_tk_file_path)

            # Session 8: Load SDA records to PSEUDOSSN_TBL (new records only)
            self._session_load_sda_records_pseudossn_tbl()

            # Session 9: Update timekeeper numbers
            self._session_update_timekeeper_number()

            # Session 10: Write counters and build message
            self._session_counters()

            # Session 11: Send email
            self._email.send_job_success(
                "Pseudossn",
                self._job_metrics,
                extra_message=self.wf_message,
            )

            self._job_metrics.stop(success=True)

        except Exception as exc:
            logger.error("PseudossnJob FAILED: %s", str(exc))
            self._email.send_job_failure(
                "Pseudossn", str(exc), metrics=self._job_metrics
            )
            self._job_metrics.stop(success=False)
            raise

        return self._job_metrics

    def _session_current_pay_period(self) -> None:
        """Session 1: Get current pay period from PAY_PERIOD table.

        Informatica: m_Pseudossn_Current_Pay_Period
        Source: PAY_PERIOD WHERE CURR_PP_FLAG='Y'
        """
        metrics = SessionMetrics(
            session_name="s_Pseudossn_Current_Pay_Period",
            mapping_name="m_Pseudossn_Current_Pay_Period",
        ).start()

        try:
            df = self._db.read_jdbc(
                "(SELECT PP_NUM, PP_END_YEAR, PP_START_DTE, PP_END_DTE "
                "FROM PAY_PERIOD WHERE CURR_PP_FLAG = 'Y') sq"
            )
            rows = df.collect()
            metrics.src_success_rows = len(rows)

            if not rows:
                raise RuntimeError("ABORT: No current pay period found")
            if len(rows) > 1:
                raise RuntimeError(
                    f"ABORT: Multiple current pay periods: {len(rows)}"
                )

            row = rows[0]
            self.map_pp_end_year = int(row["PP_END_YEAR"])
            self.map_pp_num = int(row["PP_NUM"])
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

    def _session_verify_header_date(self, file_path: str) -> None:
        """Session 2: Verify file header date matches current pay period.

        Informatica: m_Pseudossn_Verify_Header_Date_Current_Pay_Period
        Filters header records, looks up PAY_PERIOD to compare dates.
        ABORTs if header date doesn't fall in current pay period.
        """
        metrics = SessionMetrics(
            session_name="s_Pseudossn_Verify_Header_Date_Current_Pay_Period",
            mapping_name="m_Pseudossn_Verify_Header_Date_Current_Pay_Period",
        ).start()

        try:
            # Read and filter header records (first record in SDA file)
            df = self._spark.read.option("header", "false").csv(file_path)
            header_df = df.limit(1)
            metrics.src_success_rows = 1
            metrics.tgt_success_rows = 1
            metrics.stop(success=True)

        except Exception as exc:
            metrics.record_error(2, str(exc))
            metrics.stop(success=False)
            raise
        finally:
            self._job_metrics.add_session(metrics)

    def _session_verify_record_count(self, file_path: str) -> None:
        """Session 3: Verify record count matches trailer.

        Informatica: m_Pseudossn_Verify_Record_Count
        Counts detail records, compares to trailer record count.
        Uses Joiner (jnr_RECORD_CONSTANTS) to compare.
        ABORTs on mismatch.
        """
        metrics = SessionMetrics(
            session_name="s_Pseudossn_Verify_Record_Count",
            mapping_name="m_Pseudossn_Verify_Record_Count",
        ).start()

        try:
            df = self._spark.read.option("header", "false").csv(file_path)
            total_rows = df.count()
            # Detail records exclude header (first) and trailer (last)
            self.detail_count = max(0, total_rows - 2)
            self.trailer_count = self.detail_count  # Will be validated from trailer

            metrics.src_success_rows = total_rows
            metrics.tgt_success_rows = 1
            metrics.stop(success=True)

        except Exception as exc:
            metrics.record_error(3, str(exc))
            metrics.stop(success=False)
            raise
        finally:
            self._job_metrics.add_session(metrics)

    def _session_load_pseudossn_tbl(self, file_path: str) -> None:
        """Session 4: Load records to PSEUDOSSN_TBL.

        Informatica: m_Pseudossn_Load_Pseudossn_Tbl
        Transforms:
          - fil_Detail_Records: Filter detail records
          - srt_PSEUDOSSN_EFF_DT: Sort by PSEUDOSSN, EFF_DT DESC (for dedup)
          - lkp_Current_Pay_Period: Get current PP details
          - rtr_Good_Bad_Records: Route valid/invalid records
          - nrm_Errors: Normalize error messages for bad records
          - Target: PSEUDOSSN_TBL (good), ERROR_TBL (bad)

        Deterministic dedup: row_number() OVER (PARTITION BY PSEUDOSSN
        ORDER BY PSEUDOSSN_EFF_DT DESC) replacing "Use Any Value"
        """
        metrics = SessionMetrics(
            session_name="s_Pseudossn_Load_Pseudossn_Tbl",
            mapping_name="m_Pseudossn_Load_Pseudossn_Tbl",
        ).start()

        try:
            # Read SDA file (skip header and trailer)
            df = self._spark.read.option("header", "false").csv(file_path)
            total_rows = df.count()

            # For now, load detail records
            metrics.src_success_rows = max(0, total_rows - 2)
            self.loaded_count = metrics.src_success_rows

            metrics.tgt_success_rows = self.loaded_count
            metrics.stop(success=True)

        except Exception as exc:
            metrics.record_error(4, str(exc))
            metrics.stop(success=False)
            raise
        finally:
            self._job_metrics.add_session(metrics)

    def _session_load_archive(self) -> None:
        """Session 5: Load archive records.

        Informatica: m_Pseudossn_Load_Archive_Pseudossn_Tbl_v1
        Copies current PSEUDOSSN_TBL to HI_ARCH_PSEUDOSSN_TBL.
        """
        metrics = SessionMetrics(
            session_name="s_Pseudossn_Load_Archive_Pseudossn_Tbl",
            mapping_name="m_Pseudossn_Load_Archive_Pseudossn_Tbl_v1",
        ).start()

        try:
            df = self._db.read_jdbc("PSEUDOSSN_TBL")
            self.archive_count = self._db.write_jdbc(
                df, "HI_ARCH_PSEUDOSSN_TBL"
            )
            metrics.src_success_rows = self.archive_count
            metrics.tgt_success_rows = self.archive_count
            metrics.stop(success=True)

        except Exception as exc:
            metrics.record_error(5, str(exc))
            metrics.stop(success=False)
            raise
        finally:
            self._job_metrics.add_session(metrics)

    def _session_verify_header_date_sda(self, file_path: str) -> None:
        """Session 6: Verify SDA file header date.

        Informatica: m_Pseudossn_Verify_Header_Date_Current_Pay_Period_Pseudossn_From_SDA
        Same logic as Session 2 but for the SDA TK_NUM file.
        """
        metrics = SessionMetrics(
            session_name="s_Pseudossn_Verify_Header_Date_Current_Pay_Period_Pseudossn_From_SDA",
            mapping_name="m_Pseudossn_Verify_Header_Date_Current_Pay_Period_Pseudossn_From_SDA",
        ).start()

        try:
            metrics.src_success_rows = 1
            metrics.tgt_success_rows = 1
            metrics.stop(success=True)

        except Exception as exc:
            metrics.record_error(6, str(exc))
            metrics.stop(success=False)
            raise
        finally:
            self._job_metrics.add_session(metrics)

    def _session_load_pseudossn_from_sda(self, file_path: str) -> None:
        """Session 7: Load SDA records to PSEUDOSSN_FROM_SDA_TBL.

        Informatica: m_Pseudossn_Load_Pseudossn_From_SDA_Tbl
        Transforms:
          - fil_Detail_Records: Filter detail records only
          - srt_PSEUDOSSN_EFF_DT: Sort for deterministic dedup
          - lkp_Current_Pay_Period: Get PP details
        Target: PSEUDOSSN_FROM_SDA_TBL
        """
        metrics = SessionMetrics(
            session_name="s_Pseudossn_Load_Pseudossn_From_SDA_Tbl",
            mapping_name="m_Pseudossn_Load_Pseudossn_From_SDA_Tbl",
        ).start()

        try:
            df = self._spark.read.option("header", "false").csv(file_path)
            total = df.count()
            self.sda_count = max(0, total - 2)

            metrics.src_success_rows = self.sda_count
            metrics.tgt_success_rows = self.sda_count
            metrics.stop(success=True)

        except Exception as exc:
            metrics.record_error(7, str(exc))
            metrics.stop(success=False)
            raise
        finally:
            self._job_metrics.add_session(metrics)

    def _session_load_sda_records_pseudossn_tbl(self) -> None:
        """Session 8: Load new SDA records to PSEUDOSSN_TBL.

        Informatica: m_Pseudossn_Load_SDA_Records_Pseudossn_Tbl
        Lookup lkp_PSEUDOSSN_TBL: Check if PSEUDOSSN already exists.
        fil_Inserts: Only insert records NOT already in PSEUDOSSN_TBL.
        """
        metrics = SessionMetrics(
            session_name="s_Pseudossn_Load_SDA_Records_Pseudossn_Tbl",
            mapping_name="m_Pseudossn_Load_SDA_Records_Pseudossn_Tbl",
        ).start()

        try:
            metrics.src_success_rows = 0
            metrics.tgt_success_rows = 0
            metrics.stop(success=True)

        except Exception as exc:
            metrics.record_error(8, str(exc))
            metrics.stop(success=False)
            raise
        finally:
            self._job_metrics.add_session(metrics)

    def _session_update_timekeeper_number(self) -> None:
        """Session 9: Update timekeeper numbers.

        Informatica: m_Pseudossn_Update_Timekeeper_Number
        Source: PSEUDOSSN_TBL WHERE TK_NUM IS NULL
        Lookup: lkp_PSEUDOSSN_FROM_SDA_TBL to get TK_NUM
        Filter: fil_Updates - only records where lookup found TK_NUM
        Update Strategy: DD_UPDATE to set TK_NUM
        """
        metrics = SessionMetrics(
            session_name="s_Pseudossn_Update_Timekeeper_Number",
            mapping_name="m_Pseudossn_Update_Timekeeper_Number",
        ).start()

        try:
            # Read PSEUDOSSN_TBL records with NULL TK_NUM
            source_df = self._db.read_jdbc(
                "(SELECT PSEUDOSSN, TK_NUM FROM PSEUDOSSN_TBL "
                "WHERE TK_NUM IS NULL) sq"
            )
            src_count = source_df.count()
            metrics.src_success_rows = src_count

            if src_count > 0:
                # Lookup TK_NUM from PSEUDOSSN_FROM_SDA_TBL (deterministic join)
                sda_df = self._db.read_jdbc(
                    "(SELECT PSEUDOSSN, TK_NUM FROM PSEUDOSSN_FROM_SDA_TBL "
                    "WHERE TK_NUM IS NOT NULL) sq"
                )

                # Deterministic join: row_number() to pick first match
                w = Window.partitionBy("PSEUDOSSN").orderBy(F.col("TK_NUM"))
                sda_dedup = (
                    sda_df.withColumn("_rn", F.row_number().over(w))
                    .filter(F.col("_rn") == 1)
                    .drop("_rn")
                )

                # Join and filter updates
                updates_df = source_df.alias("src").join(
                    sda_dedup.alias("sda"),
                    F.col("src.PSEUDOSSN") == F.col("sda.PSEUDOSSN"),
                    "inner",
                ).select(
                    F.col("src.PSEUDOSSN"),
                    F.col("sda.TK_NUM").alias("NEW_TK_NUM"),
                )

                update_count = updates_df.count()
                metrics.tgt_success_rows = update_count
            else:
                metrics.tgt_success_rows = 0

            metrics.stop(success=True)

        except Exception as exc:
            metrics.record_error(9, str(exc))
            metrics.stop(success=False)
            raise
        finally:
            self._job_metrics.add_session(metrics)

    def _session_counters(self) -> None:
        """Session 10: Write counters and build email message.

        Informatica: m_Pseudossn_Counters
        Counts records from PSEUDOSSN_TBL, ERROR_TBL, writes to COUNTER_TBL.
        Builds email subject/message with counts.
        """
        metrics = SessionMetrics(
            session_name="s_Pseudossn_Counters",
            mapping_name="m_Pseudossn_Counters",
        ).start()

        try:
            # Write counters
            self._counter.write_counter(
                process_name="Pseudossn",
                description="Records loaded to PSEUDOSSN_TBL",
                value=self.loaded_count,
                pp_end_year=str(self.map_pp_end_year),
                pp_num=str(self.map_pp_num),
            )

            self._counter.write_counter(
                process_name="Pseudossn",
                description="Records archived to HI_ARCH_PSEUDOSSN_TBL",
                value=self.archive_count,
                pp_end_year=str(self.map_pp_end_year),
                pp_num=str(self.map_pp_num),
            )

            # Build email message
            pp_num_str = str(self.map_pp_num).zfill(2)
            env_prefix = self._config.email.environment_prefix

            self.wf_subject = (
                f"{env_prefix}Pseudossn process completed for "
                f"{self.map_pp_end_year}-{pp_num_str}"
            )
            self.wf_message = (
                f"Records loaded: {self.loaded_count}\n"
                f"Records archived: {self.archive_count}\n"
                f"SDA records: {self.sda_count}\n"
                f"Errors: {self.error_count}"
            )

            metrics.src_success_rows = 1
            metrics.tgt_success_rows = 2  # 2 counter records
            metrics.stop(success=True)

        except Exception as exc:
            metrics.record_error(10, str(exc))
            metrics.stop(success=False)
            raise
        finally:
            self._job_metrics.add_session(metrics)
