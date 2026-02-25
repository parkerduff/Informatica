"""
Job 2: wf_COMPTIME → PySpark Migration
Source: XML/COMPTIME

Informatica Workflow: wf_COMPTIME
Server: Prd_IS / Dom_Prd / ONDEMAND schedule
Repository: Prd_Repo_Srvc

Workflow Sequence (XML/COMPTIME lines 959-962):
    Start → s_COMPTIME_Current_Pay_Period
          → s_COMPTIME_Load_COMP_TIME_DAILY_TBL
          → s_COMPTIME_Build_Message_Counters
          → email_COMPTIME_Complete

Key Transformations:
- fil_Detail: RECORD_TYPE_FLAG = 'D' (lines 98-104)
- exp_Initial: DECODE(TRUE, IS_NUMBER(SSN), 'D', 'NO') (lines 106-111)
- lkp_PAY_PERIOD: lookup CURR_PP_FLAG='Y' via INFO_TARGET (lines 335-376)
- exp_Convert: date conversions IIF(IS_DATE(...)) (lines 378-390)
- agg_ALL_RECORDS: COUNT(SSN) with Scope=All Input (lines 112-123)
- exp_Counters: compile counter values (lines 129-135)
- exp_Final: SESSSTARTTIME, $PMMappingName (lines 136-141)
- exp_Build_Message: SETVARIABLE for subject/message (lines 85-97)

DTM buffer size = 24000000 (line 944)
Post-session: file archival (lines 757-762)
"""

import logging
import os
import shutil
from datetime import datetime
from typing import Optional

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, regexp_replace, to_date, count, lit, current_timestamp
)

from pyspark_migration.common.config import MigrationConfig
from pyspark_migration.common.db_manager import DatabaseManager
from pyspark_migration.common.email_service import EmailService
from pyspark_migration.common.counter_error import CounterManager
from pyspark_migration.common.logging_utils import SessionMetrics, JobMetrics

logger = logging.getLogger(__name__)

WORKFLOW_NAME = "wf_COMPTIME"
MAPPING_LOAD = "m_COMPTIME_Load_COMP_TIME_DAILY_TBL"
MAPPING_CURRENT_PP = "m_COMPTIME_Current_Pay_Period"
MAPPING_BUILD_MSG = "m_COMPTIME_Build_Message_Counters"


class CompTimeJob:
    """PySpark implementation of wf_COMPTIME.
    
    Loads compensatory time data from flat file into COMP_TIME_DAILY_TBL.
    Uses INFO_TARGET Oracle connection for all DB operations.
    DTM buffer size = 24000000 → spark.sql.shuffle.partitions tuning.
    """

    def __init__(
        self,
        spark: SparkSession,
        config: MigrationConfig,
        db_manager: DatabaseManager,
        email_service: EmailService,
        counter_manager: CounterManager,
    ):
        self.spark = spark
        self.config = config
        self.db = db_manager
        self.email = email_service
        self.counters = counter_manager
        self.job_metrics = JobMetrics(
            job_name="Job2_COMPTIME",
            workflow_name=WORKFLOW_NAME
        )
        # Workflow variables (from /data/BIISINT/control/BIIS_parms.iparms)
        self.wf_pp_end_year = os.environ.get("WF_PP_END_YEAR", "")
        self.wf_pp_num = os.environ.get("WF_PP_NUM", "")
        self.wf_pp_year_num = os.environ.get("WF_PP_YEAR_NUM", "")
        self.wf_subject = ""
        self.wf_message = ""
        self.wf_email_list = config.email.default_recipients
        # Job start time (replaces SESSSTARTTIME)
        self.session_start_time = datetime.now()
        # Counters
        self.detail_record_count = 0
        self.lkp_pp_num: Optional[int] = None
        self.lkp_pp_end_year: Optional[int] = None

    def run(self) -> JobMetrics:
        """Execute the full wf_COMPTIME workflow."""
        self.job_metrics.mark_started()
        logger.info(f"Starting {WORKFLOW_NAME}")

        try:
            # Step 1: s_COMPTIME_Current_Pay_Period
            self._get_current_pay_period()

            # Step 2: s_COMPTIME_Load_COMP_TIME_DAILY_TBL
            self._load_comp_time_daily()

            # Step 3: s_COMPTIME_Build_Message_Counters
            self._build_message_counters()

            # Step 4: email_COMPTIME_Complete
            self._send_completion_email()

            self.job_metrics.mark_succeeded()
            logger.info(f"{WORKFLOW_NAME} completed successfully")

        except Exception as e:
            self.job_metrics.mark_failed()
            logger.error(f"{WORKFLOW_NAME} failed: {e}")
            self.email.send_failure_email(
                job_name=WORKFLOW_NAME,
                session_name="wf_COMPTIME",
                error_message=str(e),
                recipients=self.wf_email_list
            )
            raise

        return self.job_metrics

    def _get_current_pay_period(self) -> None:
        """Session: s_COMPTIME_Current_Pay_Period
        
        Retrieves current pay period from PAY_PERIOD table.
        Replaces lkp_PAY_PERIOD lookup (CURR_PP_FLAG='Y', cached, via INFO_TARGET).
        """
        metrics = SessionMetrics(
            session_name="s_COMPTIME_Current_Pay_Period",
            mapping_name=MAPPING_CURRENT_PP
        )
        metrics.mark_started()
        logger.info("Step 1: Getting current pay period")

        try:
            curr_pp_df = self.db.read_jdbc(
                "PAY_PERIOD", predicate="CURR_PP_FLAG = 'Y'"
            )
            row = curr_pp_df.first()
            if row is None:
                raise Exception("No current pay period found (CURR_PP_FLAG='Y')")

            self.lkp_pp_end_year = int(row["PP_END_YEAR"])
            self.lkp_pp_num = int(row["PP_NUM"])

            # Update workflow variables if not already set from parameter file
            if not self.wf_pp_end_year:
                self.wf_pp_end_year = str(self.lkp_pp_end_year)
            if not self.wf_pp_num:
                self.wf_pp_num = str(self.lkp_pp_num)

            metrics.src_success_rows = 1
            metrics.mark_succeeded()
            logger.info(f"Current pay period: {self.lkp_pp_end_year}-{self.lkp_pp_num}")

        except Exception as e:
            metrics.mark_failed(error_code=-1, error_msg=str(e))
            raise

        self.job_metrics.add_session(metrics)

    def _load_comp_time_daily(self) -> None:
        """Session: s_COMPTIME_Load_COMP_TIME_DAILY_TBL
        Mapping: m_COMPTIME_Load_COMP_TIME_DAILY_TBL
        
        Reads COMPTIME flat file, applies transformations, loads to COMP_TIME_DAILY_TBL.
        
        Transformation pipeline:
        1. Read flat file (SQ_U0287D01 Source Qualifier)
        2. fil_Detail: filter RECORD_TYPE_FLAG = 'D'
        3. exp_Initial: DECODE(TRUE, IS_NUMBER(SSN), 'D', 'NO')
        4. lkp_PAY_PERIOD: lookup current pay period
        5. exp_Convert: date conversions
        6. agg_ALL_RECORDS: COUNT(SSN)
        7. exp_Detail_Count: set CURR_PP_FLAG='Y'
        8. exp_Counters: compile counter values
        9. exp_Final: set RUN_DATE and PROCESS_NAME
        10. Write to COMP_TIME_DAILY_TBL and COUNTER_TBL
        
        Pre-session vars (lines 646-663):
        - $$MAP_PP_END_YEAR ← $$WF_PP_END_YEAR
        - $$MAP_PP_NUM ← $$WF_PP_NUM
        - $$MAP_PP_YEAR_NUM ← $$WF_PP_YEAR_NUM
        
        Post-session success command (lines 757-762):
        - mv source file to archive
        """
        metrics = SessionMetrics(
            session_name="s_COMPTIME_Load_COMP_TIME_DAILY_TBL",
            mapping_name=MAPPING_LOAD
        )
        metrics.mark_started()
        logger.info("Step 2: Loading COMP_TIME_DAILY_TBL")

        # Pre-session variable assignment
        map_pp_end_year = self.wf_pp_end_year
        map_pp_num = self.wf_pp_num
        map_pp_year_num = self.wf_pp_year_num

        try:
            # Read COMPTIME flat file (replaces SQ_U0287D01 Source Qualifier)
            comptime_filename = os.environ.get(
                "PARAM_COMPTIME_FILENAME", "u0827d01.txt"
            )
            input_path = os.path.join(
                self.config.paths.comptime_input_dir, comptime_filename
            )
            
            raw_df = self.spark.read.option("header", "false").csv(input_path)
            metrics.src_success_rows = raw_df.count()
            logger.info(f"Read {metrics.src_success_rows} rows from {input_path}")

            # fil_Detail: filter RECORD_TYPE_FLAG = 'D'
            # Informatica: Filter Condition = RECORD_TYPE_FLAG = 'D'
            detail_df = raw_df.filter(col("RECORD_TYPE_FLAG") == "D")

            # exp_Initial: DECODE(TRUE, IS_NUMBER(SSN), 'D', 'NO')
            # PySpark: when(col("SSN").rlike("^[0-9]+$"), "D").otherwise("NO")
            detail_df = detail_df.withColumn(
                "o_RECORD_TYPE_FLAG",
                when(col("SSN").rlike("^[0-9]+$"), "D").otherwise("NO")
            ).filter(col("o_RECORD_TYPE_FLAG") == "D")

            # exp_Convert: date conversions
            # IIF(IS_DATE(PP_END_DATE, 'YYYYMMDD'), TO_DATE(PP_END_DATE, 'YYYYMMDD'))
            if "PP_END_DATE" in detail_df.columns:
                detail_df = detail_df.withColumn(
                    "PP_END_DATE_CONVERTED",
                    when(
                        col("PP_END_DATE").rlike("^[0-9]{8}$"),
                        to_date(col("PP_END_DATE"), "yyyyMMdd")
                    )
                )

            # agg_ALL_RECORDS: COUNT(SSN) with Transformation Scope = All Input
            self.detail_record_count = detail_df.count()

            # Add metadata columns (replaces exp_Final)
            detail_df = detail_df.withColumn(
                "RUN_DATE", lit(self.session_start_time)
            ).withColumn(
                "PROCESS_NAME", lit(MAPPING_LOAD)
            )

            # Write to COMP_TIME_DAILY_TBL via INFO_TARGET
            self.db.write_jdbc(detail_df, "COMP_TIME_DAILY_TBL", mode="append")
            metrics.tgt_success_rows = self.detail_record_count

            # Write counters to COUNTER_TBL (replaces exp_Counters)
            self.counters.write_counter(
                process_name=MAPPING_LOAD,
                counter_description="Number of detail records from the COMP TIME file.",
                counter_value=float(self.detail_record_count),
                pp_end_year=self.lkp_pp_end_year,
                pp_num=self.lkp_pp_num,
                run_date=self.session_start_time
            )

            # Post-session success command: archive source file (lines 757-762)
            # mv $Param_Root_Directory/data/int/in/COMPTIME/$Param_COMPTIME_filename
            #    $Param_Root_Directory/data/archive/COMPTIME/u0827d01_P$$WF_PP_YEAR_NUM.txt
            archive_filename = f"u0827d01_P{map_pp_year_num}.txt"
            archive_path = os.path.join(
                self.config.paths.comptime_archive_dir, archive_filename
            )
            if os.path.exists(input_path):
                os.makedirs(self.config.paths.comptime_archive_dir, exist_ok=True)
                shutil.move(input_path, archive_path)
                logger.info(f"Archived: {input_path} → {archive_path}")

            metrics.mark_succeeded()
            logger.info(f"Loaded {self.detail_record_count} detail records")

        except Exception as e:
            metrics.mark_failed(error_code=-1, error_msg=str(e))
            raise

        self.job_metrics.add_session(metrics)

    def _build_message_counters(self) -> None:
        """Session: s_COMPTIME_Build_Message_Counters
        Mapping: m_COMPTIME_Build_Message_Counters
        
        Builds email subject and message from counters.
        
        Replaces exp_Build_Message transformation (XML/COMPTIME lines 85-97):
        - v_ENVIRONMENT = DECODE(SUBSTR($PMRepositoryServiceName, 1, 4), ...)
        - v_SUBJECT = v_ENVIRONMENT || 'Comp Time File loaded successfully...'
        - SETVARIABLE($$MAP_SUBJECT, v_SUBJECT)
        - v_MESSAGE = 'Number of Detail Records from Comp Time file = ' || COUNTER_1
        - SETVARIABLE($$MAP_MESSAGE, v_MESSAGE)
        
        Pre-session vars (lines 769-770):
        - $$MAP_SUBJECT ← $$WF_SUBJECT
        - $$MAP_MESSAGE ← $$WF_MESSAGE
        """
        metrics = SessionMetrics(
            session_name="s_COMPTIME_Build_Message_Counters",
            mapping_name=MAPPING_BUILD_MSG
        )
        metrics.mark_started()
        logger.info("Step 3: Building message and counters")

        try:
            pp_num_str = str(self.wf_pp_num).zfill(2) if self.wf_pp_num else "00"
            env_prefix = self.config.env_prefix

            # SETVARIABLE($$MAP_SUBJECT, v_SUBJECT)
            self.wf_subject = (
                f"{env_prefix}Comp Time File loaded successfully for "
                f"Pay Period: {self.wf_pp_end_year}-{pp_num_str}"
            )

            # SETVARIABLE($$MAP_MESSAGE, v_MESSAGE)
            self.wf_message = (
                f"Number of Detail Records from Comp Time file\t= {self.detail_record_count}"
            )

            metrics.mark_succeeded()
            logger.info(f"Subject: {self.wf_subject}")

        except Exception as e:
            metrics.mark_failed(error_code=-1, error_msg=str(e))
            raise

        self.job_metrics.add_session(metrics)

    def _send_completion_email(self) -> None:
        """Task: email_COMPTIME_Complete
        
        Sends completion email with subject and message.
        """
        logger.info("Step 4: Sending completion email")
        self.email.send_email(
            subject=self.wf_subject,
            body=self.wf_message,
            recipients=self.wf_email_list
        )
