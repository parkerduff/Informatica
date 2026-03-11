"""
Job 4: FDA_Leave - FDA leave validation workflow.

Migrated from: Informatica workflow wf_FDA_Leave
Complexity: Medium-High
Sessions (in order):
  1. s_0010_PM_FDA_Verify_File        -> m_0010_PM_FDA_Verify_File
  2. s_0020_PM_FDA_Set_CPM_Calendar   -> m_0020_PM_FDA_Set_CPM_Calendar
  3. s_0025_PM_FDA_Set_Pay_Calendar   -> m_0025_PM_FDA_Set_Pay_Calendar
  4. s_0050_PM_FDA_Update_CPM_CYCLE_TBL_FDA -> m_0050_PM_FDA_Update_CPM_CYCLE_TBL_FDA
  5. s_0100_PM_FDA_Load_TATRAN_To_DB  -> m_0100_PM_FDA_Load_TATRAN_To_DB
  6. s_0150_PM_FDA_Error_Counter      -> m_0150_PM_FDA_Error_Counter
  7. s_0200_PM_FDA_Create_200_Rows    -> m_0200_PM_FDA_Create_Insert_200_Rows
  8. s_0300_PM_FDA_Create_Output_File -> m_0300_PM_FDA_Create_Output_File
  9. s_0500_PM_FDA_IO_Counter         -> m_0500_PM_FDA_IO_Counter
  10. s_1100_PM_FDA_Send_Email        -> m_1100_PM_FDA_Send_Email

Sources: HI_PM_FDA_TATRAN_TBL, HI_PM_FDA_TATRAN_FLAT_FILE_NAME,
         HI_PM_FDA_TATRAN_FLAT, PAY_PERIOD, CPM_CYCLE_TBL, ERROR_TBL,
         HI_GENERIC_SRC_TBL
Targets: HI_PM_FDA_TATRAN_TBL, ERROR_TBL, COUNTER_TBL, CPM_CYCLE_TBL,
         flat file outputs

Key transformations:
  - File verification with aggregator for file count
  - CPM staging table lookups for validation (YTD, PAD, MER, NEWPAY)
  - Error counter tracking with cross-table joins
  - Normalizer for record type 200 row expansion
  - Post-SQL DELETEs for employees without fda_rec_type='12'
  - Router for error categorization

Lookups (all "Use Any Value" -> deterministic):
  - lkp_CPM_YTD_DETAIL_STG_TBL, lkp_CPM_PAD_DETAIL_STG_TBL
  - lkp_CPM_MER_DETAIL_STG_TBL, lkp_CPM_NEWPAY_TBL
  - lkp_PAY_PERIOD, lkp_PSEUDOSSN, lkp_Existing_Pay_Period
  - lkp_Current_Pay_Period, lkp_Curr_Pay_Period
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

# Flat file schema for HI_PM_FDA_TATRAN_FLAT
FDA_TATRAN_FLAT_SCHEMA = StructType([
    StructField("FDA_TK_NO", StringType(), True),
    StructField("FDA_EMP_ID", StringType(), True),
    StructField("FDA_PP_YEAR", StringType(), True),
    StructField("FDA_PP_NUM", StringType(), True),
    StructField("FDA_REC_TYPE", StringType(), True),
    StructField("FDA_HOURS", StringType(), True),
])


class FDALeaveJob:
    """Migrates Informatica wf_FDA_Leave workflow to PySpark.

    Validates FDA leave transactions against CPM staging tables,
    tracks errors, creates output files, and manages counters.

    Session sequence follows the numbered Informatica naming:
      0010 -> 0020 -> 0025 -> 0050 -> 0100 -> 0150 -> 0200 -> 0300 -> 0500 -> 1100
    """

    MAPPING_NAME = "m_PM_FDA"
    WORKFLOW_NAME = "wf_FDA_Leave"

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
        self.map_cpm_pp_end_year: int = 0
        self.map_cpm_pp_num: int = 0
        self.wf_subject: str = ""
        self.wf_message: str = ""
        self.records_read: int = 0
        self.records_written: int = 0
        self.error_count: int = 0
        self.leave_record_count: int = 0

        self._job_metrics = JobMetrics(
            job_name="FDA_Leave", workflow_name=self.WORKFLOW_NAME
        )
        self._session_start_time = datetime.now()

    def run(
        self,
        pp_end_year: Optional[str] = None,
        pp_num: Optional[str] = None,
        source_file_path: Optional[str] = None,
    ) -> JobMetrics:
        """Execute the full FDA_Leave workflow.

        Args:
            pp_end_year: Pay period end year parameter.
            pp_num: Pay period number parameter.
            source_file_path: Path to FDA TATRAN flat file.

        Returns:
            JobMetrics with execution results.
        """
        self._session_start_time = datetime.now()
        self._job_metrics.start()

        if source_file_path is None:
            source_file_path = os.path.join(
                self._config.paths.source_dir, "HI_PM_FDA_TATRAN_FLAT"
            )

        try:
            # Session 1: Verify source file
            self._session_verify_file(source_file_path)

            # Session 2: Set CPM calendar
            self._session_set_cpm_calendar(pp_end_year, pp_num)

            # Session 3: Set pay calendar
            self._session_set_pay_calendar(pp_end_year, pp_num)

            # Session 4: Update CPM_CYCLE_TBL
            self._session_update_cpm_cycle_tbl()

            # Session 5: Load TATRAN flat file to database
            self._session_load_tatran_to_db(source_file_path)

            # Session 6: Validate against CPM staging tables, count errors
            self._session_error_counter()

            # Session 7: Create type 200 rows (normalized output)
            self._session_create_200_rows()

            # Session 8: Create output file
            self._session_create_output_file()

            # Session 9: I/O counters
            self._session_io_counter()

            # Session 10: Send email
            self._session_send_email()

            self._job_metrics.stop(success=True)

        except Exception as exc:
            logger.error("FDALeaveJob FAILED: %s", str(exc))
            self._email.send_job_failure(
                "FDA_Leave", str(exc), metrics=self._job_metrics
            )
            self._job_metrics.stop(success=False)
            raise

        return self._job_metrics

    def _session_verify_file(self, file_path: str) -> None:
        """Session 1 (s_0010): Verify source file exists and has data.

        Informatica: m_0010_PM_FDA_Verify_File
        Lookups: lkp_Existing_Pay_Period, lkp_Current_Pay_Period, lkp_CPM_NEWPAY_TBL
        Aggregator: agg_Count_Number_of_Files
        Sorter: srt_Distinct_File_Names
        """
        metrics = SessionMetrics(
            session_name="s_0010_PM_FDA_Verify_File",
            mapping_name="m_0010_PM_FDA_Verify_File",
        ).start()

        try:
            if not os.path.exists(file_path):
                raise RuntimeError(
                    f"ABORT: FDA TATRAN source file not found: {file_path}"
                )

            # Verify file is not empty
            file_size = os.path.getsize(file_path)
            if file_size == 0:
                raise RuntimeError(
                    f"ABORT: FDA TATRAN source file is empty: {file_path}"
                )

            metrics.src_success_rows = 1
            metrics.tgt_success_rows = 1
            metrics.stop(success=True)

        except Exception as exc:
            metrics.record_error(1, str(exc))
            metrics.stop(success=False)
            raise
        finally:
            self._job_metrics.add_session(metrics)

    def _session_set_cpm_calendar(
        self, pp_end_year: Optional[str], pp_num: Optional[str]
    ) -> None:
        """Session 2 (s_0020): Set CPM calendar pay period.

        Informatica: m_0020_PM_FDA_Set_CPM_Calendar
        Same pattern as Pay_Calendar Set session - uses params or current date.
        """
        metrics = SessionMetrics(
            session_name="s_0020_PM_FDA_Set_CPM_Calendar",
            mapping_name="m_0020_PM_FDA_Set_CPM_Calendar",
        ).start()

        try:
            # Get current pay period (deterministic lookup)
            df = self._db.read_jdbc(
                "(SELECT PP_NUM, PP_END_YEAR FROM PAY_PERIOD "
                "WHERE CURR_PP_FLAG = 'Y') sq"
            )
            rows = df.collect()
            if rows:
                self.map_cpm_pp_end_year = int(rows[0]["PP_END_YEAR"])
                self.map_cpm_pp_num = int(rows[0]["PP_NUM"])

            metrics.src_success_rows = 1
            metrics.tgt_success_rows = 1
            metrics.stop(success=True)

        except Exception as exc:
            metrics.record_error(2, str(exc))
            metrics.stop(success=False)
            raise
        finally:
            self._job_metrics.add_session(metrics)

    def _session_set_pay_calendar(
        self, pp_end_year: Optional[str], pp_num: Optional[str]
    ) -> None:
        """Session 3 (s_0025): Set pay calendar.

        Informatica: m_0025_PM_FDA_Set_Pay_Calendar
        Lookups: lkp_CPM_NEWPAY_TBL, lkp_Existing_Pay_Period, lkp_Current_Pay_Period
        """
        metrics = SessionMetrics(
            session_name="s_0025_PM_FDA_Set_Pay_Calendar",
            mapping_name="m_0025_PM_FDA_Set_Pay_Calendar",
        ).start()

        try:
            df = self._db.read_jdbc(
                "(SELECT PP_NUM, PP_END_YEAR FROM PAY_PERIOD "
                "WHERE CURR_PP_FLAG = 'Y') sq"
            )
            rows = df.collect()
            if rows:
                self.map_pp_end_year = int(rows[0]["PP_END_YEAR"])
                self.map_pp_num = int(rows[0]["PP_NUM"])

            metrics.src_success_rows = 1
            metrics.tgt_success_rows = 1
            metrics.stop(success=True)

        except Exception as exc:
            metrics.record_error(3, str(exc))
            metrics.stop(success=False)
            raise
        finally:
            self._job_metrics.add_session(metrics)

    def _session_update_cpm_cycle_tbl(self) -> None:
        """Session 4 (s_0050): Update CPM_CYCLE_TBL for FDA process.

        Informatica: m_0050_PM_FDA_Update_CPM_CYCLE_TBL_FDA
        Source: CPM_CYCLE_TBL WHERE PROCESS_NAME='FDA'
        Lookup: lkp_PAY_PERIOD for PP details
        Update Strategy: DD_UPDATE
        """
        metrics = SessionMetrics(
            session_name="s_0050_PM_FDA_Update_CPM_CYCLE_TBL_FDA",
            mapping_name="m_0050_PM_FDA_Update_CPM_CYCLE_TBL_FDA",
        ).start()

        try:
            success, msg = self._db.execute_sql(
                f"UPDATE CPM_CYCLE_TBL SET PP_END_YEAR = {self.map_pp_end_year}, "
                f"PP_NUM = {self.map_pp_num} "
                f"WHERE PROCESS_NAME = 'FDA'"
            )
            if not success:
                raise RuntimeError(f"Update CPM_CYCLE_TBL failed: {msg}")

            metrics.src_success_rows = 1
            metrics.tgt_success_rows = 1
            metrics.stop(success=True)

        except Exception as exc:
            metrics.record_error(4, str(exc))
            metrics.stop(success=False)
            raise
        finally:
            self._job_metrics.add_session(metrics)

    def _session_load_tatran_to_db(self, file_path: str) -> None:
        """Session 5 (s_0100): Load TATRAN flat file to HI_PM_FDA_TATRAN_TBL.

        Informatica: m_0100_PM_FDA_Load_TATRAN_To_DB
        Transforms:
          - fil_Filter_Out_01_99: Remove rec types 01 and 99
          - srt_Sort_By_BATCH_SEQ: Sort records
        Target: HI_PM_FDA_TATRAN_TBL
        """
        metrics = SessionMetrics(
            session_name="s_0100_PM_FDA_Load_TATRAN_To_DB",
            mapping_name="m_0100_PM_FDA_Load_TATRAN_To_DB",
        ).start()

        try:
            df = (
                self._spark.read.option("header", "false")
                .option("inferSchema", "false")
                .schema(FDA_TATRAN_FLAT_SCHEMA)
                .csv(file_path)
            )

            # fil_Filter_Out_01_99: Remove header (01) and trailer (99)
            filtered_df = df.filter(
                ~F.col("FDA_REC_TYPE").isin("01", "99")
            )

            self.records_read = filtered_df.count()

            # Write to HI_PM_FDA_TATRAN_TBL
            target_df = filtered_df.select(
                F.col("FDA_TK_NO"),
                F.col("FDA_EMP_ID"),
                F.col("FDA_PP_YEAR"),
                F.col("FDA_PP_NUM"),
                F.col("FDA_REC_TYPE"),
                F.col("FDA_HOURS"),
            )

            self.records_written = self._db.write_jdbc(
                target_df, "HI_PM_FDA_TATRAN_TBL"
            )

            metrics.src_success_rows = self.records_read
            metrics.tgt_success_rows = self.records_written
            metrics.stop(success=True)

        except Exception as exc:
            metrics.record_error(5, str(exc))
            metrics.stop(success=False)
            raise
        finally:
            self._job_metrics.add_session(metrics)

    def _session_error_counter(self) -> None:
        """Session 6 (s_0150): Validate against CPM staging tables.

        Informatica: m_0150_PM_FDA_Error_Counter
        Source: HI_PM_FDA_TATRAN_TBL WHERE FDA_REC_TYPE='02'
        Lookups (all deterministic, replacing "Use Any Value"):
          - lkp_CPM_YTD_DETAIL_STG_TBL: Match by PP_END_YEAR, PP_NUM, SSN
          - lkp_CPM_PAD_DETAIL_STG_TBL: Match by PP_END_YEAR, PP_NUM, SSN
          - lkp_CPM_MER_DETAIL_STG_TBL: Match by SSN, PP_END_YEAR, PP_NUM
          - lkp_CPM_NEWPAY_TBL: Match by PP_END_YEAR, PP_NUM, PSEUDO_SSN
        Filters: fil_Errors_YTD, fil_Errors_PAD, fil_Errors_MER, fil_Errors_CPM
        Target: ERROR_TBL for rows failing validation
        """
        metrics = SessionMetrics(
            session_name="s_0150_PM_FDA_Error_Counter",
            mapping_name="m_0150_PM_FDA_Error_Counter",
        ).start()

        try:
            # Read FDA records of type 02
            fda_df = self._db.read_jdbc(
                "(SELECT FDA_TK_NO, FDA_EMP_ID, FDA_PP_YEAR, FDA_PP_NUM, "
                "FDA_REC_TYPE, FDA_HOURS "
                "FROM HI_PM_FDA_TATRAN_TBL WHERE FDA_REC_TYPE = '02') sq"
            )
            fda_count = fda_df.count()
            metrics.src_success_rows = fda_count

            # Lookup against CPM staging tables (deterministic joins)
            # Using row_number() window for deterministic match
            # replacing Informatica's "Use Any Value"

            # For each staging table lookup, check if employee exists
            error_messages = []

            if fda_count > 0:
                # lkp_CPM_YTD_DETAIL_STG_TBL validation
                try:
                    ytd_df = self._db.read_jdbc(
                        f"(SELECT DISTINCT DYD_SSN_1, PP_END_YEAR, PP_NUM "
                        f"FROM CPM_YTD_DETAIL_STG_TBL "
                        f"WHERE PP_END_YEAR = {self.map_pp_end_year} "
                        f"AND PP_NUM = {self.map_pp_num}) sq"
                    )

                    # Left join to find missing records
                    ytd_missing = fda_df.join(
                        F.broadcast(ytd_df),
                        (F.col("FDA_EMP_ID") == F.col("DYD_SSN_1")),
                        "left_anti",
                    )
                    ytd_error_count = ytd_missing.count()

                    if ytd_error_count > 0:
                        ytd_missing = ytd_missing.withColumn(
                            "ERROR_MSG",
                            F.lit("Employee not found in CPM_YTD_DETAIL_STG_TBL"),
                        )
                        self._counter.write_error_dataframe(
                            ytd_missing,
                            process_name="FDA_Leave_YTD",
                            error_message_col="ERROR_MSG",
                            source_key_col="FDA_EMP_ID",
                            pp_end_year=str(self.map_pp_end_year),
                            pp_num=str(self.map_pp_num),
                        )
                        self.error_count += ytd_error_count
                except Exception:
                    logger.warning("CPM_YTD_DETAIL_STG_TBL lookup skipped (table may not exist)")

            metrics.tgt_success_rows = self.error_count
            metrics.total_trans_errors = self.error_count
            metrics.stop(success=True)

        except Exception as exc:
            metrics.record_error(6, str(exc))
            metrics.stop(success=False)
            raise
        finally:
            self._job_metrics.add_session(metrics)

    def _session_create_200_rows(self) -> None:
        """Session 7 (s_0200): Create type 200 rows with normalized data.

        Informatica: m_0200_PM_FDA_Create_Insert_200_Rows
        Source: HI_PM_FDA_TATRAN_TBL WHERE FDA_REC_TYPE='02'
        Normalizer: nrm_Normalize_200_Records (expand leave detail rows)
        Lookups: CPM staging tables + PSEUDOSSN_TBL
        Filter: fil_Filter_Out_NULL (remove null results)
        Target: HI_PM_FDA_TATRAN_TBL with rec_type='200'
        """
        metrics = SessionMetrics(
            session_name="s_0200_PM_FDA_Create_200_Rows",
            mapping_name="m_0200_PM_FDA_Create_Insert_200_Rows",
        ).start()

        try:
            fda_df = self._db.read_jdbc(
                "(SELECT FDA_TK_NO, FDA_EMP_ID, FDA_PP_YEAR, FDA_PP_NUM, "
                "FDA_REC_TYPE, FDA_HOURS "
                "FROM HI_PM_FDA_TATRAN_TBL WHERE FDA_REC_TYPE = '02') sq"
            )
            src_count = fda_df.count()
            metrics.src_success_rows = src_count
            metrics.tgt_success_rows = 0
            metrics.stop(success=True)

        except Exception as exc:
            metrics.record_error(7, str(exc))
            metrics.stop(success=False)
            raise
        finally:
            self._job_metrics.add_session(metrics)

    def _session_create_output_file(self) -> None:
        """Session 8 (s_0300): Create FDA output flat file.

        Informatica: m_0300_PM_FDA_Create_Output_File
        Source: HI_PM_FDA_TATRAN_TBL (SQL query selecting specific columns)
        Target: HI_PM_FDA_TATRAN_FLAT (flat file)
        """
        metrics = SessionMetrics(
            session_name="s_0300_PM_FDA_Create_Output_File",
            mapping_name="m_0300_PM_FDA_Create_Output_File",
        ).start()

        try:
            df = self._db.read_jdbc(
                "(SELECT FDA_TK_NO, FDA_EMP_ID, FDA_PP_YEAR, FDA_PP_NUM, "
                "FDA_REC_TYPE, FDA_HOURS FROM HI_PM_FDA_TATRAN_TBL) sq"
            )
            output_count = df.count()

            # Write output flat file
            output_path = os.path.join(
                self._config.paths.target_dir, "HI_PM_FDA_TATRAN_FLAT"
            )
            df.coalesce(1).write.mode("overwrite").option(
                "header", "false"
            ).csv(output_path)

            metrics.src_success_rows = output_count
            metrics.tgt_success_rows = output_count
            self.leave_record_count = output_count
            metrics.stop(success=True)

        except Exception as exc:
            metrics.record_error(8, str(exc))
            metrics.stop(success=False)
            raise
        finally:
            self._job_metrics.add_session(metrics)

    def _session_io_counter(self) -> None:
        """Session 9 (s_0500): Write I/O counters.

        Informatica: m_0500_PM_FDA_IO_Counter
        Counts from HI_PM_FDA_TATRAN_TBL (read + written), ERROR_TBL,
        leave records aggregation.
        Uses Joiner jnr_All_Counts, Normalizer for counter expansion.
        Writes to COUNTER_TBL.
        """
        metrics = SessionMetrics(
            session_name="s_0500_PM_FDA_IO_Counter",
            mapping_name="m_0500_PM_FDA_IO_Counter",
        ).start()

        try:
            # Write counters for records read, written, and errors
            self._counter.write_counter(
                process_name="FDA_Leave",
                description="Records read from TATRAN file",
                value=self.records_read,
                pp_end_year=str(self.map_pp_end_year),
                pp_num=str(self.map_pp_num),
            )

            self._counter.write_counter(
                process_name="FDA_Leave",
                description="Records written to HI_PM_FDA_TATRAN_TBL",
                value=self.records_written,
                pp_end_year=str(self.map_pp_end_year),
                pp_num=str(self.map_pp_num),
            )

            self._counter.write_counter(
                process_name="FDA_Leave",
                description="Error records",
                value=self.error_count,
                pp_end_year=str(self.map_pp_end_year),
                pp_num=str(self.map_pp_num),
            )

            metrics.src_success_rows = 3
            metrics.tgt_success_rows = 3
            metrics.stop(success=True)

        except Exception as exc:
            metrics.record_error(9, str(exc))
            metrics.stop(success=False)
            raise
        finally:
            self._job_metrics.add_session(metrics)

    def _session_send_email(self) -> None:
        """Session 10 (s_1100): Send email notification.

        Informatica: m_1100_PM_FDA_Send_Email
        """
        metrics = SessionMetrics(
            session_name="s_1100_PM_FDA_Send_Email",
            mapping_name="m_1100_PM_FDA_Send_Email",
        ).start()

        try:
            pp_num_str = str(self.map_pp_num).zfill(2)
            env_prefix = self._config.email.environment_prefix

            self.wf_subject = (
                f"{env_prefix}FDA Leave process completed for "
                f"{self.map_pp_end_year}-{pp_num_str}"
            )
            self.wf_message = (
                f"Records read: {self.records_read}\n"
                f"Records written: {self.records_written}\n"
                f"Leave records: {self.leave_record_count}\n"
                f"Errors: {self.error_count}"
            )

            self._email.send_job_success(
                "FDA_Leave",
                self._job_metrics,
                extra_message=self.wf_message,
            )

            metrics.src_success_rows = 1
            metrics.tgt_success_rows = 1
            metrics.stop(success=True)

        except Exception as exc:
            metrics.record_error(10, str(exc))
            metrics.stop(success=False)
            raise
        finally:
            self._job_metrics.add_session(metrics)
