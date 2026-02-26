"""
Job 1: wf_Pay_Calendar → PySpark Migration
Source: XML/Pay_Calendar

Informatica Workflow: wf_Pay_Calendar
Server: Prd_IS / Dom_Prd / ONDEMAND schedule
Repository: Prd_Repo_Srvc

Workflow Sequence (XML/Pay_Calendar lines 985-989):
    Start → s_Pay_Calendar_Reset_Pay_Calendar
          → s_Pay_Calendar_Set_Pay_Calendar
          → s_Pay_Calendar_Verify_Pay_Calendar
          → s_Pay_Calendar_Build_Message
          → Email_Pay_Calendar

Sessions and Mappings:
1. s_Pay_Calendar_Reset_Pay_Calendar → m_Pay_Calendar_Reset_Pay_Calendar
   - Source: PAY_PERIOD1 (Oracle, INFO_TARGET)
   - Transforms: exp_Initial, upd_Reset_Current_PP (DD_UPDATE)
   - Target: RESET_PAY_PERIOD (Oracle, INFO_TARGET)
   - Reject file: reset_pay_period1.bad

2. s_Pay_Calendar_Set_Pay_Calendar → m_Pay_Calendar_Set_Pay_Calendar
   - Source: PAY_PERIOD1 (Oracle, INFO_TARGET)
   - Transforms: rtr_Parameter_Non_Parameter (Router), upd_Set_Current_PP_Param,
     upd_Set_Current_PP_Non_Param, lkp_New_Current_Pay_Period, lkp_Existing_Pay_Period
   - Pre-session: $$PP_END_YEAR ← $$WF_PP_END_YEAR, $$PP_NUM ← $$WF_PP_NUM
   - Post-session success: $$WF_PP_END_YEAR ← $$PP_END_YEAR, $$WF_PP_NUM ← $$PP_NUM

3. s_Pay_Calendar_Verify_Pay_Calendar → m_Pay_Calendar_Verify_Pay_Calendar
   - Validates exactly one CURR_PP_FLAG='Y' row exists
   - ABORT() if 0 or >1

4. s_Pay_Calendar_Build_Message → m_Pay_Calendar_Build_Message
   - Builds $$WF_SUBJECT and $$WF_MESSAGE

5. Email_Pay_Calendar → sends email to $$WF_PAY_CALENDAR_EMAIL_LIST
"""

import logging
import os
from datetime import datetime
from typing import Optional

from pyspark.sql import SparkSession

from pyspark_migration.common.config import MigrationConfig
from pyspark_migration.common.db_manager import DatabaseManager
from pyspark_migration.common.email_service import EmailService
from pyspark_migration.common.logging_utils import SessionMetrics, JobMetrics

logger = logging.getLogger(__name__)

# Informatica mapping name constants (replaces $PMMappingName)
MAPPING_RESET = "m_Pay_Calendar_Reset_Pay_Calendar"
MAPPING_SET = "m_Pay_Calendar_Set_Pay_Calendar"
MAPPING_VERIFY = "m_Pay_Calendar_Verify_Pay_Calendar"
MAPPING_BUILD_MSG = "m_Pay_Calendar_Build_Message"
WORKFLOW_NAME = "wf_Pay_Calendar"


class PayCalendarJob:
    """PySpark implementation of wf_Pay_Calendar.
    
    Migrated from Informatica PowerCenter workflow.
    All sessions use INFO_TARGET Oracle connection (XML/Pay_Calendar lines 631-641).
    Default session config: Maximum Memory = 512MB, Enable Recovery = NO.
    """

    def __init__(
        self,
        spark: SparkSession,
        config: MigrationConfig,
        db_manager: DatabaseManager,
        email_service: EmailService,
    ):
        self.spark = spark
        self.config = config
        self.db = db_manager
        self.email = email_service
        self.job_metrics = JobMetrics(
            job_name="Job1_Pay_Calendar",
            workflow_name=WORKFLOW_NAME
        )
        # Workflow variables (replaces $$WF_* variables)
        self.wf_pp_end_year: Optional[str] = os.environ.get("WF_PP_END_YEAR", "")
        self.wf_pp_num: Optional[str] = os.environ.get("WF_PP_NUM", "")
        self.wf_subject: str = ""
        self.wf_message: str = ""
        self.wf_email_list = config.email.default_recipients

    def run(self) -> JobMetrics:
        """Execute the full wf_Pay_Calendar workflow.
        
        Workflow sequence:
        Start → Reset → Set → Verify → Build Message → Email
        Each step only runs if previous step Status = Succeeded.
        """
        self.job_metrics.mark_started()
        logger.info(f"Starting {WORKFLOW_NAME}")

        try:
            # Step 1: s_Pay_Calendar_Reset_Pay_Calendar
            self._reset_pay_calendar()

            # Step 2: s_Pay_Calendar_Set_Pay_Calendar
            self._set_pay_calendar()

            # Step 3: s_Pay_Calendar_Verify_Pay_Calendar
            self._verify_pay_calendar()

            # Step 4: s_Pay_Calendar_Build_Message
            self._build_message()

            # Step 5: Email_Pay_Calendar
            self._send_email()

            self.job_metrics.mark_succeeded()
            logger.info(f"{WORKFLOW_NAME} completed successfully")

        except Exception as e:
            self.job_metrics.mark_failed()
            logger.error(f"{WORKFLOW_NAME} failed: {e}")
            self.email.send_failure_email(
                job_name=WORKFLOW_NAME,
                session_name="wf_Pay_Calendar",
                error_message=str(e),
                recipients=self.wf_email_list
            )
            raise

        return self.job_metrics

    def _reset_pay_calendar(self) -> None:
        """Session: s_Pay_Calendar_Reset_Pay_Calendar
        Mapping: m_Pay_Calendar_Reset_Pay_Calendar
        
        Reads PAY_PERIOD1 source, applies exp_Initial, upd_Reset_Current_PP (DD_UPDATE).
        Resets all rows with CURR_PP_FLAG='Y' to NULL.
        
        Informatica artifacts:
        - Source: PAY_PERIOD1 via SQ_PAY_PERIOD_RESET (Source Qualifier)
        - Transform: exp_Initial (Expression)
        - Transform: upd_Reset_Current_PP (Update Strategy, DD_UPDATE)
        - Target: RESET_PAY_PERIOD via INFO_TARGET
        - Reject file: reset_pay_period1.bad
        """
        metrics = SessionMetrics(
            session_name="s_Pay_Calendar_Reset_Pay_Calendar",
            mapping_name=MAPPING_RESET
        )
        metrics.mark_started()
        logger.info("Step 1: Resetting current pay period flag")

        try:
            # Count rows before reset (for metrics)
            before_df = self.db.read_jdbc("PAY_PERIOD", predicate="CURR_PP_FLAG = 'Y'")
            metrics.src_success_rows = before_df.count()

            # Execute UPDATE (replaces upd_Reset_Current_PP DD_UPDATE)
            self.db.execute_sql(
                "UPDATE PAY_PERIOD SET CURR_PP_FLAG = NULL WHERE CURR_PP_FLAG = 'Y'",
                connection="target"
            )

            metrics.tgt_success_rows = metrics.src_success_rows
            metrics.mark_succeeded()
            logger.info(f"Reset {metrics.src_success_rows} rows")

        except Exception as e:
            metrics.mark_failed(error_code=-1, error_msg=str(e))
            logger.error(f"Reset failed: {e}")
            self.email.send_failure_email(
                job_name=WORKFLOW_NAME,
                session_name=metrics.session_name,
                error_message=str(e),
                recipients=self.wf_email_list
            )
            raise

        self.job_metrics.add_session(metrics)

    def _set_pay_calendar(self) -> None:
        """Session: s_Pay_Calendar_Set_Pay_Calendar
        Mapping: m_Pay_Calendar_Set_Pay_Calendar
        
        Uses rtr_Parameter_Non_Parameter router to determine path:
        - If parameters exist ($$PP_END_YEAR, $$PP_NUM set): use parameter path
        - If parameters empty: use system date path
        
        Informatica artifacts:
        - Pre-session vars: $$PP_END_YEAR ← $$WF_PP_END_YEAR, $$PP_NUM ← $$WF_PP_NUM
        - Transform: exp_Determine_Parameters_Exist (Expression)
        - Transform: rtr_Parameter_Non_Parameter (Router) → two branches
        - Transform: lkp_New_Current_Pay_Period (Lookup)
        - Transform: lkp_Existing_Pay_Period (Lookup)
        - Transform: upd_Set_Current_PP_Param (Update Strategy, DD_UPDATE)
        - Transform: upd_Set_Current_PP_Non_Param (Update Strategy, DD_UPDATE)
        - Post-session success: $$WF_PP_END_YEAR ← $$PP_END_YEAR, $$WF_PP_NUM ← $$PP_NUM
        """
        metrics = SessionMetrics(
            session_name="s_Pay_Calendar_Set_Pay_Calendar",
            mapping_name=MAPPING_SET
        )
        metrics.mark_started()

        # Pre-session variable assignment (XML/Pay_Calendar lines 807-808)
        pp_end_year = self.wf_pp_end_year
        pp_num = self.wf_pp_num

        logger.info(f"Step 2: Setting pay calendar (PP_END_YEAR={pp_end_year}, PP_NUM={pp_num})")

        try:
            # Router logic: rtr_Parameter_Non_Parameter
            if pp_end_year and pp_num:
                # Parameter path: upd_Set_Current_PP_Param
                logger.info("Using parameter-based path")
                self.db.execute_sql(
                    "UPDATE PAY_PERIOD SET CURR_PP_FLAG = 'Y' "
                    "WHERE PP_END_YEAR = :1 AND PP_NUM = :2",
                    connection="target",
                    params=[int(pp_end_year), int(pp_num)]
                )
            else:
                # Non-parameter path: upd_Set_Current_PP_Non_Param
                logger.info("Using system date-based path")
                self.db.execute_sql(
                    "UPDATE PAY_PERIOD SET CURR_PP_FLAG = 'Y' "
                    "WHERE PP_START_DTE <= SYSDATE AND PP_END_DTE >= SYSDATE",
                    connection="target"
                )

            # Read back updated values for post-session variable assignment
            curr_pp_df = self.db.read_jdbc(
                "PAY_PERIOD", predicate="CURR_PP_FLAG = 'Y'"
            )
            if curr_pp_df.count() == 1:
                row = curr_pp_df.first()
                self.wf_pp_end_year = str(row["PP_END_YEAR"])
                self.wf_pp_num = str(row["PP_NUM"])

            metrics.tgt_success_rows = 1
            metrics.mark_succeeded()

        except Exception as e:
            metrics.mark_failed(error_code=-1, error_msg=str(e))
            logger.error(f"Set pay calendar failed: {e}")
            self.email.send_failure_email(
                job_name=WORKFLOW_NAME,
                session_name=metrics.session_name,
                error_message=str(e),
                recipients=self.wf_email_list
            )
            raise

        self.job_metrics.add_session(metrics)

    def _verify_pay_calendar(self) -> None:
        """Session: s_Pay_Calendar_Verify_Pay_Calendar
        Mapping: m_Pay_Calendar_Verify_Pay_Calendar
        
        Validates exactly one CURR_PP_FLAG='Y' row exists.
        ABORT() if 0 or >1 (replaces Informatica ABORT() expression).
        
        Informatica original: ABORT('Expected 1 current pay period, found ' || count)
        PySpark: raise Exception(...)
        """
        metrics = SessionMetrics(
            session_name="s_Pay_Calendar_Verify_Pay_Calendar",
            mapping_name=MAPPING_VERIFY
        )
        metrics.mark_started()
        logger.info("Step 3: Verifying pay calendar")

        try:
            curr_pp_df = self.db.read_jdbc(
                "PAY_PERIOD", predicate="CURR_PP_FLAG = 'Y'"
            )
            count = curr_pp_df.count()
            metrics.src_success_rows = count

            # ABORT() replacement — must abort if count != 1
            if count != 1:
                error_msg = f"Expected exactly 1 current pay period, found {count}"
                raise Exception(error_msg)

            metrics.mark_succeeded()
            logger.info(f"Verification passed: {count} current pay period(s)")

        except Exception as e:
            metrics.mark_failed(error_code=-1, error_msg=str(e))
            logger.error(f"Verification failed: {e}")
            raise

        self.job_metrics.add_session(metrics)

    def _build_message(self) -> None:
        """Session: s_Pay_Calendar_Build_Message
        Mapping: m_Pay_Calendar_Build_Message
        
        Queries PAY_PERIOD for CURR_PP_FLAG='Y' record and builds
        $$WF_SUBJECT and $$WF_MESSAGE workflow variables.
        
        Uses SETVARIABLE pattern:
        - SETVARIABLE($$WF_SUBJECT, v_SUBJECT)
        - SETVARIABLE($$WF_MESSAGE, v_MESSAGE)
        """
        metrics = SessionMetrics(
            session_name="s_Pay_Calendar_Build_Message",
            mapping_name=MAPPING_BUILD_MSG
        )
        metrics.mark_started()
        logger.info("Step 4: Building notification message")

        try:
            curr_pp_df = self.db.read_jdbc(
                "PAY_PERIOD", predicate="CURR_PP_FLAG = 'Y'"
            )
            row = curr_pp_df.first()
            metrics.src_success_rows = 1

            pp_end_year = str(row["PP_END_YEAR"])
            pp_num = str(row["PP_NUM"]).zfill(2)
            pp_start = str(row["PP_START_DTE"]) if "PP_START_DTE" in curr_pp_df.columns else ""
            pp_end = str(row["PP_END_DTE"]) if "PP_END_DTE" in curr_pp_df.columns else ""

            # Build subject and message (replaces SETVARIABLE pattern)
            env_prefix = self.config.env_prefix
            self.wf_subject = (
                f"{env_prefix}Current Pay Period has been set to: "
                f"{pp_end_year}-{pp_num}"
            )
            self.wf_message = (
                f"The current pay period has been set to:\n"
                f"  Pay Period Year: {pp_end_year}\n"
                f"  Pay Period Number: {pp_num}\n"
                f"  Start Date: {pp_start}\n"
                f"  End Date: {pp_end}"
            )

            metrics.mark_succeeded()
            logger.info(f"Message built: {self.wf_subject}")

        except Exception as e:
            metrics.mark_failed(error_code=-1, error_msg=str(e))
            logger.error(f"Build message failed: {e}")
            raise

        self.job_metrics.add_session(metrics)

    def _send_email(self) -> None:
        """Task: Email_Pay_Calendar
        
        Sends email notification with workflow variables.
        Informatica attributes (XML/Pay_Calendar lines 589-593):
        - Email User Name = $$WF_PAY_CALENDAR_EMAIL_LIST
        - Email Subject = $$WF_SUBJECT
        - Email Text = $$WF_MESSAGE
        """
        logger.info("Step 5: Sending notification email")
        self.email.send_email(
            subject=self.wf_subject,
            body=self.wf_message,
            recipients=self.wf_email_list
        )
