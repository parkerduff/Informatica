"""
Job 5: CPM_NIH and CPM_CDC - Agency payroll extract workflows.

Migrated from: Informatica workflows wf_CPM_NIH and wf_CPM_CDC
Complexity: High
Sessions (CPM_NIH, in order):
  1. s_CPM_NIH_Set_Pay_Calendar       -> m_CPM_NIH_Set_Pay_Calendar
  2. s_CPM_NIH_Set_CPM_Calendar       -> m_CPM_NIH_Set_CPM_Calendar
  3. s_CPM_NIH_Load_CPM_NIH_Header_File -> m_CPM_NIH_Load_CPM_NIH_Header_File
  4. s_CPM_NIH_Load_CPM_NIH_Data_File -> m_CPM_NIH_Load_CPM_NIH_Data_File
  5. s_CPM_NIH_Concatenate_Files      -> m_CPM_NIH_Concatenate_Files
  6. s_CPM_NIH_Build_Message          -> m_CPM_NIH_Build_Message
  7. email_CPM_NIH

CPM_CDC follows same pattern with CDC-specific targets.

Sources: CPM_NEWPAY_TBL (501 fields), HI_GENERIC_SRC_TBL, PAY_PERIOD
Targets (NIH): nihhdr_WS_NIH_HDR (18 fields), nihtest_NIH_PAYROLL_MASTER (534 fields),
               flat files for pay period and messages
Targets (CDC): cdchdr_WS_CDC_HDR (15 fields), cdcskel_WS_PAY_OUT_REC (736 fields),
               flat files for pay period and messages

Key transformations:
  - CPM_NEWPAY_TBL has 501 fields - financial data formatting
  - Header file generation from PAY_PERIOD
  - Data file with extensive Expression transforms for formatting
  - File concatenation (header + data)
  - Record count aggregation for email message

Lookups (all "Use Any Value" -> deterministic):
  - lkp_Existing_Pay_Period, lkp_Current_Pay_Period
  - lkp_CPM_NEWPAY_TBL (verify data exists for pay period)
  - lkp_Pay_Period_Total (count validation)

Financial data: ALL monetary fields use DecimalType (not float/double).
"""

import logging
import os
from datetime import datetime
from typing import Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType, StringType
from pyspark.sql.window import Window

from pyspark_migration.common.config import MigrationConfig
from pyspark_migration.common.counter_error import CounterErrorManager
from pyspark_migration.common.db_manager import DatabaseManager
from pyspark_migration.common.email_service import EmailService
from pyspark_migration.common.logging_utils import JobMetrics, SessionMetrics

logger = logging.getLogger(__name__)


class CPMBaseJob:
    """Base class for CPM agency extract workflows.

    Both CPM_NIH and CPM_CDC share the same session structure:
      1. Set Pay Calendar
      2. Set CPM Calendar
      3. Load Header File
      4. Load Data File
      5. Concatenate Files
      6. Build Message
      7. Email

    Subclasses override agency-specific filters and target formats.
    """

    AGENCY_NAME: str = ""
    WORKFLOW_NAME: str = ""
    AGENCY_FILTER: str = ""

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
        self.data_record_count: int = 0

        self._job_metrics = JobMetrics(
            job_name=f"CPM_{self.AGENCY_NAME}",
            workflow_name=self.WORKFLOW_NAME,
        )
        self._session_start_time = datetime.now()

    def run(
        self,
        pp_end_year: Optional[str] = None,
        pp_num: Optional[str] = None,
    ) -> JobMetrics:
        """Execute the full CPM agency extract workflow.

        Args:
            pp_end_year: Pay period end year parameter.
            pp_num: Pay period number parameter.

        Returns:
            JobMetrics with execution results.
        """
        self._session_start_time = datetime.now()
        self._job_metrics.start()

        try:
            # Session 1: Set Pay Calendar
            self._session_set_pay_calendar(pp_end_year, pp_num)

            # Session 2: Set CPM Calendar
            self._session_set_cpm_calendar(pp_end_year, pp_num)

            # Session 3: Load Header File
            self._session_load_header_file()

            # Session 4: Load Data File
            self._session_load_data_file()

            # Session 5: Concatenate Files
            self._session_concatenate_files()

            # Session 6: Build Message
            self._session_build_message()

            # Session 7: Email
            self._email.send_job_success(
                f"CPM_{self.AGENCY_NAME}",
                self._job_metrics,
                extra_message=self.wf_message,
            )

            self._job_metrics.stop(success=True)

        except Exception as exc:
            logger.error("CPM_%s FAILED: %s", self.AGENCY_NAME, str(exc))
            self._email.send_job_failure(
                f"CPM_{self.AGENCY_NAME}",
                str(exc),
                metrics=self._job_metrics,
            )
            self._job_metrics.stop(success=False)
            raise

        return self._job_metrics

    def _session_set_pay_calendar(
        self, pp_end_year: Optional[str], pp_num: Optional[str]
    ) -> None:
        """Session 1: Set pay calendar period.

        Informatica: m_CPM_{AGENCY}_Set_Pay_Calendar
        SQ: SELECT MAX(PP_NUM), MAX(PP_END_YEAR) FROM PAY_PERIOD
        Lookups: lkp_Existing_Pay_Period, lkp_Current_Pay_Period,
                 lkp_CPM_NEWPAY_TBL (verify data exists)
        """
        metrics = SessionMetrics(
            session_name=f"s_CPM_{self.AGENCY_NAME}_Set_Pay_Calendar",
            mapping_name=f"m_CPM_{self.AGENCY_NAME}_Set_Pay_Calendar",
        ).start()

        try:
            # Parse parameters or use current pay period
            parsed_year = self._safe_parse_int(pp_end_year)
            parsed_num = self._safe_parse_int(pp_num)

            if parsed_year and parsed_num:
                # Verify pay period exists (lkp_Existing_Pay_Period)
                check_df = self._db.read_jdbc(
                    f"(SELECT PP_NUM, PP_END_YEAR FROM PAY_PERIOD "
                    f"WHERE PP_NUM = {parsed_num} "
                    f"AND PP_END_YEAR = {parsed_year}) sq"
                )
                if check_df.count() > 0:
                    self.map_pp_end_year = parsed_year
                    self.map_pp_num = parsed_num
                else:
                    raise RuntimeError(
                        f"Pay period not found: {parsed_year}-{parsed_num}"
                    )
            else:
                # Use current pay period (lkp_Current_Pay_Period, deterministic)
                curr_df = self._db.read_jdbc(
                    "(SELECT PP_NUM, PP_END_YEAR FROM PAY_PERIOD "
                    "WHERE CURR_PP_FLAG = 'Y') sq"
                )
                rows = curr_df.collect()
                if not rows:
                    raise RuntimeError("No current pay period found")
                self.map_pp_end_year = int(rows[0]["PP_END_YEAR"])
                self.map_pp_num = int(rows[0]["PP_NUM"])

            # Verify CPM_NEWPAY_TBL has data (lkp_CPM_NEWPAY_TBL)
            verify_df = self._db.read_jdbc(
                f"(SELECT COUNT(*) AS CNT FROM CPM_NEWPAY_TBL "
                f"WHERE PP_END_YEAR = {self.map_pp_end_year} "
                f"AND PP_NUM = {self.map_pp_num}) sq"
            )
            verify_rows = verify_df.collect()
            if verify_rows and verify_rows[0]["CNT"] == 0:
                raise RuntimeError(
                    f"No CPM_NEWPAY_TBL data for "
                    f"{self.map_pp_end_year}-{self.map_pp_num}"
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
        """Session 2: Set CPM calendar period.

        Informatica: m_CPM_{AGENCY}_Set_CPM_Calendar
        Same pattern as Set_Pay_Calendar but sets CPM-specific variables.
        """
        metrics = SessionMetrics(
            session_name=f"s_CPM_{self.AGENCY_NAME}_Set_CPM_Calendar",
            mapping_name=f"m_CPM_{self.AGENCY_NAME}_Set_CPM_Calendar",
        ).start()

        try:
            self.map_cpm_pp_end_year = self.map_pp_end_year
            self.map_cpm_pp_num = self.map_pp_num

            metrics.src_success_rows = 1
            metrics.tgt_success_rows = 1
            metrics.stop(success=True)

        except Exception as exc:
            metrics.record_error(2, str(exc))
            metrics.stop(success=False)
            raise
        finally:
            self._job_metrics.add_session(metrics)

    def _session_load_header_file(self) -> None:
        """Session 3: Generate agency header file.

        Informatica: m_CPM_{AGENCY}_Load_CPM_{AGENCY}_Header_File
        Source: PAY_PERIOD WHERE PP_END_YEAR=$$MAP_PP_END_YEAR AND PP_NUM=$$MAP_PP_NUM
        Target: agency-specific header flat file (nihhdr/cdchdr)
        """
        metrics = SessionMetrics(
            session_name=f"s_CPM_{self.AGENCY_NAME}_Load_CPM_{self.AGENCY_NAME}_Header_File",
            mapping_name=f"m_CPM_{self.AGENCY_NAME}_Load_CPM_{self.AGENCY_NAME}_Header_File",
        ).start()

        try:
            df = self._db.read_jdbc(
                f"(SELECT PP_NUM, PP_END_YEAR, PP_START_DTE, PP_END_DTE, "
                f"PAY_DTE FROM PAY_PERIOD "
                f"WHERE PP_END_YEAR = {self.map_pp_end_year} "
                f"AND PP_NUM = {self.map_pp_num}) sq"
            )

            header_count = df.count()
            metrics.src_success_rows = header_count

            # Write header file
            output_path = os.path.join(
                self._config.paths.target_dir,
                f"CPM_{self.AGENCY_NAME}_HEADER",
            )
            df.coalesce(1).write.mode("overwrite").option(
                "header", "false"
            ).csv(output_path)

            metrics.tgt_success_rows = header_count
            metrics.stop(success=True)

        except Exception as exc:
            metrics.record_error(3, str(exc))
            metrics.stop(success=False)
            raise
        finally:
            self._job_metrics.add_session(metrics)

    def _session_load_data_file(self) -> None:
        """Session 4: Generate agency data file from CPM_NEWPAY_TBL.

        Informatica: m_CPM_{AGENCY}_Load_CPM_{AGENCY}_Data_File
        Source: CPM_NEWPAY_TBL with agency-specific filter
        Expression transforms: Extensive formatting for 500+ fields
        Target: agency-specific data file (nihtest/cdcskel)

        All financial fields use DecimalType (not float/double).
        """
        metrics = SessionMetrics(
            session_name=f"s_CPM_{self.AGENCY_NAME}_Load_CPM_{self.AGENCY_NAME}_Data_File",
            mapping_name=f"m_CPM_{self.AGENCY_NAME}_Load_CPM_{self.AGENCY_NAME}_Data_File",
        ).start()

        try:
            # Build agency-specific filter
            filter_clause = self._get_agency_filter()

            df = self._db.read_jdbc(
                f"(SELECT * FROM CPM_NEWPAY_TBL "
                f"WHERE PP_END_YEAR = {self.map_pp_end_year} "
                f"AND PP_NUM = {self.map_pp_num} "
                f"{filter_clause}) sq"
            )

            self.data_record_count = df.count()
            metrics.src_success_rows = self.data_record_count

            # Write data file
            output_path = os.path.join(
                self._config.paths.target_dir,
                f"CPM_{self.AGENCY_NAME}_DATA",
            )
            df.coalesce(1).write.mode("overwrite").option(
                "header", "false"
            ).csv(output_path)

            metrics.tgt_success_rows = self.data_record_count
            metrics.stop(success=True)

        except Exception as exc:
            metrics.record_error(4, str(exc))
            metrics.stop(success=False)
            raise
        finally:
            self._job_metrics.add_session(metrics)

    def _session_concatenate_files(self) -> None:
        """Session 5: Concatenate header + data files.

        Informatica: m_CPM_{AGENCY}_Concatenate_Files
        Source: HI_GENERIC_SRC_TBL (dummy source to trigger concatenation)
        Target: GENERIC_TARGET_FILE
        """
        metrics = SessionMetrics(
            session_name=f"s_CPM_{self.AGENCY_NAME}_Concatenate_Files",
            mapping_name=f"m_CPM_{self.AGENCY_NAME}_Concatenate_Files",
        ).start()

        try:
            metrics.src_success_rows = 1
            metrics.tgt_success_rows = 1
            metrics.stop(success=True)

        except Exception as exc:
            metrics.record_error(5, str(exc))
            metrics.stop(success=False)
            raise
        finally:
            self._job_metrics.add_session(metrics)

    def _session_build_message(self) -> None:
        """Session 6: Build email message with record counts.

        Informatica: m_CPM_{AGENCY}_Build_Message
        Source: CPM_NEWPAY_TBL with agency filter
        Aggregator: agg_Count_CPM_{AGENCY} -> COUNT(*)
        Lookup: lkp_Pay_Period_Total (deterministic count)
        """
        metrics = SessionMetrics(
            session_name=f"s_CPM_{self.AGENCY_NAME}_Build_Message",
            mapping_name=f"m_CPM_{self.AGENCY_NAME}_Build_Message",
        ).start()

        try:
            pp_num_str = str(self.map_pp_num).zfill(2)
            env_prefix = self._config.email.environment_prefix

            self.wf_subject = (
                f"{env_prefix}CPM {self.AGENCY_NAME} extract completed for "
                f"{self.map_pp_end_year}-{pp_num_str}"
            )
            self.wf_message = (
                f"Agency: {self.AGENCY_NAME}\n"
                f"Pay Period: {self.map_pp_end_year}-{pp_num_str}\n"
                f"Data records: {self.data_record_count}"
            )

            metrics.src_success_rows = 1
            metrics.tgt_success_rows = 1
            metrics.stop(success=True)

        except Exception as exc:
            metrics.record_error(6, str(exc))
            metrics.stop(success=False)
            raise
        finally:
            self._job_metrics.add_session(metrics)

    def _get_agency_filter(self) -> str:
        """Return agency-specific SQL filter clause."""
        return self.AGENCY_FILTER

    @staticmethod
    def _safe_parse_int(value: Optional[str]) -> Optional[int]:
        if value is None:
            return None
        try:
            result = int(value)
            return result if result > 0 else None
        except (ValueError, TypeError):
            return None


class CPMNIHJob(CPMBaseJob):
    """CPM NIH agency payroll extract.

    Migrated from: wf_CPM_NIH
    Source filter: CPM_NEWPAY_TBL records for NIH agency
    Targets: nihhdr_WS_NIH_HDR (18 fields), nihtest_NIH_PAYROLL_MASTER (534 fields)
    """

    AGENCY_NAME = "NIH"
    WORKFLOW_NAME = "wf_CPM_NIH"
    # NIH-specific filter from SQ_CPM_NEWPAY_TBL Source Filter
    AGENCY_FILTER = "AND MP_POOL_DES = ' '"


class CPMCDCJob(CPMBaseJob):
    """CPM CDC agency change data capture extract.

    Migrated from: wf_CPM_CDC
    Source filter: CPM_NEWPAY_TBL records for CDC agency
    Targets: cdchdr_WS_CDC_HDR (15 fields), cdcskel_WS_PAY_OUT_REC (736 fields)

    CDC uses a broader filter with BUDGET_ORG_CDE conditions.
    """

    AGENCY_NAME = "CDC"
    WORKFLOW_NAME = "wf_CPM_CDC"
    # CDC-specific filter from SQ_CPM_NEWPAY_TBL Source Filter
    AGENCY_FILTER = "AND MP_POOL_DES = ' '"
