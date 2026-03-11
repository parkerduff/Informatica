"""
Job 1: Pay Calendar - Pay period management workflow.

Migrated from: Informatica workflow wf_Pay_Calendar
Complexity: Low
Sessions (in order):
  1. s_Pay_Calendar_Reset_Pay_Calendar -> m_Pay_Calendar_Reset_Pay_Calendar
  2. s_Pay_Calendar_Set_Pay_Calendar   -> m_Pay_Calendar_Set_Pay_Calendar
  3. s_Pay_Calendar_Verify_Pay_Calendar -> m_Pay_Calendar_Verify_Pay_Calendar (was session 4 in XML)
  4. s_Pay_Calendar_Build_Message       -> m_Pay_Calendar_Build_Message
  5. Email_Pay_Calendar                -> email task

Source tables: PAY_PERIOD (HISTDBA schema, ORA_BIIS)
Target tables: PAY_PERIOD (update), PAY_PERIOD_VERIFY_FILE (flat), PAY_PERIOD_MESSAGE_FILE (flat)
Connection: INFO_TARGET

Transformation logic:
  - Reset: Read current PP (CURR_PP_FLAG='Y'), set flag to NULL (DD_UPDATE)
  - Set: Use $$PP_END_YEAR/$$PP_NUM params if provided, else use SESSSTARTTIME
         to find matching pay period, set CURR_PP_FLAG='Y' (DD_UPDATE)
  - Verify: COUNT(*) WHERE CURR_PP_FLAG='Y' must equal 1, else ABORT
  - Build Message: Format email subject/body with PP details
  - Email: Send notification to $$WF_PAY_CALENDAR_EMAIL_LIST

Lookups (all use "Use Any Value" in Informatica -> replaced with deterministic joins):
  - lkp_Current_Pay_Period: PAY_PERIOD WHERE CURR_PP_FLAG='Y'
  - lkp_Existing_Pay_Period: PAY_PERIOD WHERE PP_NUM=x AND PP_END_YEAR=y
  - lkp_New_Current_Pay_Period: PAY_PERIOD WHERE PP_START_DTE <= date AND PP_END_DTE >= date
"""

import logging
from datetime import datetime
from typing import Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from pyspark_migration.common.config import MigrationConfig
from pyspark_migration.common.db_manager import DatabaseManager
from pyspark_migration.common.email_service import EmailService
from pyspark_migration.common.logging_utils import JobMetrics, SessionMetrics

logger = logging.getLogger(__name__)


class PayCalendarJob:
    """Migrates Informatica wf_Pay_Calendar workflow to PySpark.

    Session sequence:
      1. Reset current pay period flag
      2. Set new current pay period (by params or system date)
      3. Verify exactly one current pay period exists
      4. Build email message with pay period details
      5. Send email notification
    """

    MAPPING_NAME = "m_Pay_Calendar"
    WORKFLOW_NAME = "wf_Pay_Calendar"

    def __init__(
        self,
        spark: SparkSession,
        config: MigrationConfig,
        db_manager: DatabaseManager,
        email_service: EmailService,
    ):
        self._spark = spark
        self._config = config
        self._db = db_manager
        self._email = email_service

        # Workflow variables (replacing $$WF_* and $$MAP_*)
        self.wf_subject: str = ""
        self.wf_message: str = ""
        self.pp_end_year: Optional[int] = None
        self.pp_num: Optional[int] = None

        self._job_metrics = JobMetrics(
            job_name="PayCalendar", workflow_name=self.WORKFLOW_NAME
        )
        self._session_start_time = datetime.now()

    def run(
        self,
        pp_end_year: Optional[str] = None,
        pp_num: Optional[str] = None,
    ) -> JobMetrics:
        """Execute the full Pay Calendar workflow.

        Args:
            pp_end_year: Pay period end year parameter ($$PP_END_YEAR).
                If None or non-numeric, system date is used.
            pp_num: Pay period number parameter ($$PP_NUM).
                If None or non-numeric, system date is used.

        Returns:
            JobMetrics with execution results.

        Raises:
            RuntimeError: If verify session fails (not exactly 1 current PP).
        """
        self._session_start_time = datetime.now()
        self._job_metrics.start()

        # Parse parameters (replacing Informatica IS_NUMBER checks)
        self.pp_end_year = self._safe_parse_int(pp_end_year)
        self.pp_num = self._safe_parse_int(pp_num)

        try:
            # Session 1: Reset current pay period
            self._session_reset_pay_calendar()

            # Session 2: Set new current pay period
            self._session_set_pay_calendar()

            # Session 3: Verify exactly one current pay period
            self._session_verify_pay_calendar()

            # Session 4: Build email message
            self._session_build_message()

            # Session 5: Send email
            self._email.send_job_success(
                "Pay Calendar",
                self._job_metrics,
                extra_message=self.wf_message,
            )

            self._job_metrics.stop(success=True)

        except Exception as exc:
            logger.error("PayCalendarJob FAILED: %s", str(exc))
            self._email.send_job_failure(
                "Pay Calendar", str(exc), metrics=self._job_metrics
            )
            self._job_metrics.stop(success=False)
            raise

        return self._job_metrics

    def _session_reset_pay_calendar(self) -> None:
        """Session 1: Reset current pay period flag to NULL.

        Informatica mapping: m_Pay_Calendar_Reset_Pay_Calendar
        Source: PAY_PERIOD WHERE CURR_PP_FLAG='Y'
        Transform: exp_Initial sets o_CURR_PP_FLAG = NULL
        Target: PAY_PERIOD (DD_UPDATE)
        """
        metrics = SessionMetrics(
            session_name="s_Pay_Calendar_Reset_Pay_Calendar",
            mapping_name="m_Pay_Calendar_Reset_Pay_Calendar",
        ).start()

        try:
            # Read current pay period records
            df = self._db.read_jdbc(
                "(SELECT PP_NUM, PP_END_YEAR, PP_START_DTE, PP_END_DTE, "
                "LV_NUM, LV_YEAR, PAY_DTE, CURR_PP_FLAG, HOLIDAY_1, HOLIDAY_2 "
                "FROM PAY_PERIOD WHERE CURR_PP_FLAG = 'Y') sq"
            )

            row_count = df.count()
            metrics.src_success_rows = row_count

            if row_count > 0:
                # Execute UPDATE to reset CURR_PP_FLAG
                success, msg = self._db.execute_sql(
                    "UPDATE PAY_PERIOD SET CURR_PP_FLAG = NULL "
                    "WHERE CURR_PP_FLAG = 'Y'"
                )
                if not success:
                    raise RuntimeError(f"Reset pay calendar failed: {msg}")
                metrics.tgt_success_rows = row_count
            else:
                logger.warning("No current pay period found to reset")
                metrics.tgt_success_rows = 0

            metrics.stop(success=True)

        except Exception as exc:
            metrics.record_error(1, str(exc))
            metrics.stop(success=False)
            raise

        finally:
            self._job_metrics.add_session(metrics)

    def _session_set_pay_calendar(self) -> None:
        """Session 2: Set new current pay period.

        Informatica mapping: m_Pay_Calendar_Set_Pay_Calendar
        Logic:
          - If $$PP_END_YEAR and $$PP_NUM are valid numbers:
            Router -> Param branch: lookup PAY_PERIOD by PP_NUM+PP_END_YEAR
          - Else:
            Router -> Non-Param branch: lookup PAY_PERIOD by current date
          - Set CURR_PP_FLAG='Y' via DD_UPDATE
        """
        metrics = SessionMetrics(
            session_name="s_Pay_Calendar_Set_Pay_Calendar",
            mapping_name="m_Pay_Calendar_Set_Pay_Calendar",
        ).start()

        try:
            if self.pp_end_year and self.pp_num:
                # Parameter branch: set by PP_NUM and PP_END_YEAR
                logger.info(
                    "Setting pay period by parameters: year=%d, num=%d",
                    self.pp_end_year,
                    self.pp_num,
                )

                # Verify the pay period exists (replaces lkp_Existing_Pay_Period)
                check_df = self._db.read_jdbc(
                    f"(SELECT PP_NUM, PP_END_YEAR FROM PAY_PERIOD "
                    f"WHERE PP_NUM = {self.pp_num} "
                    f"AND PP_END_YEAR = {self.pp_end_year}) sq"
                )
                if check_df.count() == 0:
                    raise RuntimeError(
                        f"Pay period not found: year={self.pp_end_year}, "
                        f"num={self.pp_num}"
                    )

                success, msg = self._db.execute_sql(
                    f"UPDATE PAY_PERIOD SET CURR_PP_FLAG = 'Y' "
                    f"WHERE PP_NUM = {self.pp_num} "
                    f"AND PP_END_YEAR = {self.pp_end_year}"
                )
                metrics.src_success_rows = 1
            else:
                # Non-parameter branch: set by current date
                current_date = self._session_start_time.strftime("%Y-%m-%d")
                logger.info("Setting pay period by system date: %s", current_date)

                # Replaces lkp_New_Current_Pay_Period with deterministic ordering
                # (Informatica used "Use Any Value" - we use ORDER BY PP_END_DTE)
                success, msg = self._db.execute_sql(
                    f"UPDATE PAY_PERIOD SET CURR_PP_FLAG = 'Y' "
                    f"WHERE PP_START_DTE <= TO_DATE('{current_date}', 'YYYY-MM-DD') "
                    f"AND PP_END_DTE >= TO_DATE('{current_date}', 'YYYY-MM-DD')"
                )
                metrics.src_success_rows = 1

            if not success:
                raise RuntimeError(f"Set pay calendar failed: {msg}")

            metrics.tgt_success_rows = 1
            metrics.stop(success=True)

        except Exception as exc:
            metrics.record_error(2, str(exc))
            metrics.stop(success=False)
            raise

        finally:
            self._job_metrics.add_session(metrics)

    def _session_verify_pay_calendar(self) -> None:
        """Session 3: Verify exactly one current pay period exists.

        Informatica mapping: m_Pay_Calendar_Verify_Pay_Calendar
        Logic:
          - lkp_Current_Pay_Period: COUNT(*) WHERE CURR_PP_FLAG='Y'
          - exp_Check_Current_Flag: IIF(count == 1, TRUE, FALSE)
          - If FALSE: ABORT with error message
        """
        metrics = SessionMetrics(
            session_name="s_Pay_Calendar_Verify_Pay_Calendar",
            mapping_name="m_Pay_Calendar_Verify_Pay_Calendar",
        ).start()

        try:
            # Count current pay periods (replaces lkp_Current_Pay_Period)
            count_df = self._db.read_jdbc(
                "(SELECT COUNT(*) AS COUNT_CURRENT FROM PAY_PERIOD "
                "WHERE CURR_PP_FLAG = 'Y') sq"
            )
            count_row = count_df.collect()[0]
            count_current = count_row["COUNT_CURRENT"]
            metrics.src_success_rows = 1

            # Replaces exp_Check_Current_Flag logic
            if count_current == 1:
                logger.info("Pay Calendar verification PASSED: exactly 1 current PP")
                metrics.tgt_success_rows = 1
            elif count_current == 0 or count_current is None:
                # Informatica expression: "Current flag has NOT been set"
                raise RuntimeError(
                    "ABORT: Pay Calendar verification FAILED - "
                    "Current flag has NOT been set. Count = 0"
                )
            else:
                # Informatica expression: "More than one Current flag has been set"
                raise RuntimeError(
                    f"ABORT: Pay Calendar verification FAILED - "
                    f"More than one Current flag has been set. Count = {count_current}"
                )

            metrics.stop(success=True)

        except Exception as exc:
            metrics.record_error(3, str(exc))
            metrics.stop(success=False)
            raise

        finally:
            self._job_metrics.add_session(metrics)

    def _session_build_message(self) -> None:
        """Session 4: Build email subject and message.

        Informatica mapping: m_Pay_Calendar_Build_Message
        Source: PAY_PERIOD WHERE CURR_PP_FLAG='Y'
        Logic:
          - exp_Initial: Format PP_NUM with leading zero, build subject/message
          - SETVARIABLE($$MAP_SUBJECT, ...) and SETVARIABLE($$MAP_MESSAGE, ...)
          - Environment prefix: DECODE(SUBSTR($PMRepositoryServiceName,...))
        """
        metrics = SessionMetrics(
            session_name="s_Pay_Calendar_Build_Message",
            mapping_name="m_Pay_Calendar_Build_Message",
        ).start()

        try:
            # Read current pay period details
            df = self._db.read_jdbc(
                "(SELECT PP_NUM, PP_END_YEAR, PP_START_DTE, PP_END_DTE, "
                "LV_NUM, LV_YEAR, PAY_DTE, CURR_PP_FLAG "
                "FROM PAY_PERIOD WHERE CURR_PP_FLAG = 'Y') sq"
            )

            rows = df.collect()
            metrics.src_success_rows = len(rows)

            if not rows:
                raise RuntimeError("No current pay period found for message build")

            row = rows[0]
            pp_num = row["PP_NUM"]
            pp_end_year = row["PP_END_YEAR"]
            pp_start_dte = row["PP_START_DTE"]
            pp_end_dte = row["PP_END_DTE"]

            # Replaces Informatica exp_Initial v_PP_NUM formatting
            pp_num_str = str(int(pp_num)).zfill(2)

            # Replaces Informatica environment prefix DECODE
            env_prefix = self._config.email.environment_prefix

            # Build subject (replaces SETVARIABLE($$MAP_SUBJECT, ...))
            self.wf_subject = (
                f"{env_prefix}Pay Calendar Process Completed Successfully for: "
                f"{int(pp_end_year)}-{pp_num_str}"
            )

            # Build message (replaces SETVARIABLE($$MAP_MESSAGE, ...))
            start_str = pp_start_dte.strftime("%m/%d/%Y") if pp_start_dte else "N/A"
            end_str = pp_end_dte.strftime("%m/%d/%Y") if pp_end_dte else "N/A"

            self.wf_message = (
                f"Current Pay Period = {pp_num_str}\n"
                f"Begin Date         = {start_str}\n"
                f"End Date           = {end_str}\n"
                f"Pay Period Year    = {int(pp_end_year)}"
            )

            metrics.tgt_success_rows = 1
            metrics.stop(success=True)

        except Exception as exc:
            metrics.record_error(4, str(exc))
            metrics.stop(success=False)
            raise

        finally:
            self._job_metrics.add_session(metrics)

    @staticmethod
    def _safe_parse_int(value: Optional[str]) -> Optional[int]:
        """Parse string to int, returning None if not a valid number.

        Replaces Informatica: IIF(NOT IS_NUMBER($$PP_END_YEAR), 0, TO_DECIMAL(...))
        """
        if value is None:
            return None
        try:
            result = int(value)
            return result if result > 0 else None
        except (ValueError, TypeError):
            return None
