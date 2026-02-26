"""
Job 6: wf_EHRP2BIIS_UPDATE → PySpark Migration
Source: XML/EHRP2BIIS_UPDATE

Informatica Workflow: wf_EHRP2BIIS_UPDATE
Server: Prd_IS / Dom_Prd / RUNFOREVER (recurring daily since 9/28/2018, lines 2604-2611)
Repository: Prd_Repo_Srvc

Source Join (SQ_PS_GVT_JOB, lines 1954-1957):
    SELECT * FROM PS_GVT_JOB, NWK_NEW_EHRP_ACTIONS_TBL
    WHERE NWK_NEW_EHRP_ACTIONS_TBL.EMPLID = PS_GVT_JOB.EMPLID
      AND NWK_NEW_EHRP_ACTIONS_TBL.EMPL_RCD = PS_GVT_JOB.EMPL_RCD
      AND NWK_NEW_EHRP_ACTIONS_TBL.EFFDT = PS_GVT_JOB.EFFDT
      AND NWK_NEW_EHRP_ACTIONS_TBL.EFFSEQ = PS_GVT_JOB.EFFSEQ
    ORDER BY PS_GVT_JOB.EFFDT

9 Lookup Transformations (all at Stage 4, lines 2621-2658):
    1. lkp_PS_GVT_EMPLOYMENT
    2. lkp_PS_GVT_PERS_NID
    3. lkp_PS_GVT_AWD_DATA
    4. lkp_PS_GVT_EE_DATA_TRK
    5. lkp_PS_HE_FILL_POS
    6. lkp_PS_GVT_CITIZENSHIP
    7. lkp_PS_GVT_PERS_DATA
    8. lkp_OLD_SEQUENCE_NUMBER
    9. lkp_PS_JPM_JP_ITEMS — DIFFERENT connection: INFO_NATE (lines 2752-2754)

3 Target Tables (written in parallel, lines 2558-2560):
    - NWK_ACTION_PRIMARY_TBL
    - NWK_ACTION_SECONDARY_TBL
    - EHRP_RECS_TRACKING_TBL

Memory: Maximum Memory = 2GB (lines 2659-2661)
Pre-load: ehrp2biis_preload KSH script (runs SQL*Plus step01)
RUNFOREVER: Requires PySpark Structured Streaming or scheduled micro-batch polling.
Enable Recovery = NO → needs explicit checkpointing for fault tolerance.
"""

import logging
import os
import subprocess
import time
from datetime import datetime
from typing import Optional, Dict

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, when, lit, broadcast, current_timestamp, coalesce
)

from pyspark_migration.common.config import MigrationConfig
from pyspark_migration.common.db_manager import DatabaseManager
from pyspark_migration.common.email_service import EmailService
from pyspark_migration.common.counter_error import CounterManager, ErrorManager
from pyspark_migration.common.logging_utils import SessionMetrics, JobMetrics

logger = logging.getLogger(__name__)

WORKFLOW_NAME = "wf_EHRP2BIIS_UPDATE"
MAPPING_NAME = "m_EHRP2BIIS_UPDATE"

# 9 lookup table configurations (all at Stage 4)
LOOKUP_TABLES = [
    {
        "name": "lkp_PS_GVT_EMPLOYMENT",
        "table": "PS_GVT_EMPLOYMENT",
        "join_keys": ["EMPLID", "EMPL_RCD"],
        "connection": "source",  # $Source / INFO_TARGET
    },
    {
        "name": "lkp_PS_GVT_PERS_NID",
        "table": "PS_GVT_PERS_NID",
        "join_keys": ["EMPLID"],
        "connection": "source",
    },
    {
        "name": "lkp_PS_GVT_AWD_DATA",
        "table": "PS_GVT_AWD_DATA",
        "join_keys": ["EMPLID", "EMPL_RCD", "EFFDT"],
        "connection": "source",
    },
    {
        "name": "lkp_PS_GVT_EE_DATA_TRK",
        "table": "PS_GVT_EE_DATA_TRK",
        "join_keys": ["EMPLID", "EMPL_RCD"],
        "connection": "source",
    },
    {
        "name": "lkp_PS_HE_FILL_POS",
        "table": "PS_HE_FILL_POS",
        "join_keys": ["POSITION_NBR"],
        "connection": "source",
    },
    {
        "name": "lkp_PS_GVT_CITIZENSHIP",
        "table": "PS_GVT_CITIZENSHIP",
        "join_keys": ["EMPLID"],
        "connection": "source",
    },
    {
        "name": "lkp_PS_GVT_PERS_DATA",
        "table": "PS_GVT_PERS_DATA",
        "join_keys": ["EMPLID"],
        "connection": "source",
    },
    {
        "name": "lkp_OLD_SEQUENCE_NUMBER",
        "table": "NWK_ACTION_PRIMARY_TBL",
        "join_keys": ["EMPLID", "EMPL_RCD", "EFFDT", "EFFSEQ"],
        "connection": "target",
    },
    {
        # DIFFERENT named connection: INFO_NATE (separate Oracle DB)
        # XML/EHRP2BIIS_UPDATE lines 2752-2754
        "name": "lkp_PS_JPM_JP_ITEMS",
        "table": "PS_JPM_JP_ITEMS",
        "join_keys": ["SETID", "JP_ITEM_ID"],
        "connection": "nate",  # INFO_NATE — different Oracle DB
    },
]


class EHRP2BIISUpdateJob:
    """PySpark implementation of wf_EHRP2BIIS_UPDATE.
    
    Most complex workflow in the migration:
    - RUNFOREVER scheduling pattern → micro-batch polling loop
    - 9 lookup transformations including cross-database (INFO_NATE)
    - 3 target tables written in parallel
    - Pre-load script (ehrp2biis_preload)
    - Maximum Memory = 2GB
    """

    def __init__(
        self,
        spark: SparkSession,
        config: MigrationConfig,
        db_manager: DatabaseManager,
        email_service: EmailService,
        counter_manager: CounterManager,
        error_manager: ErrorManager,
    ):
        self.spark = spark
        self.config = config
        self.db = db_manager
        self.email = email_service
        self.counters = counter_manager
        self.errors = error_manager
        self.job_metrics = JobMetrics(
            job_name="Job6_EHRP2BIIS_UPDATE",
            workflow_name=WORKFLOW_NAME
        )
        self.session_start_time = datetime.now()
        self.wf_email_list = config.email.default_recipients
        # Lookup DataFrames cache (replaces Informatica lookup cache)
        self._lookup_cache: Dict[str, DataFrame] = {}

    def run(self, run_forever: bool = False, poll_interval_seconds: int = 300) -> JobMetrics:
        """Execute the EHRP2BIIS_UPDATE workflow.
        
        Args:
            run_forever: If True, implements RUNFOREVER pattern with micro-batch polling.
                        If False, runs once (for testing/migration).
            poll_interval_seconds: Seconds between polls when run_forever=True (default 5 min).
        
        RUNFOREVER Pattern (XML/EHRP2BIIS_UPDATE lines 2604-2611):
        Informatica uses recurring daily schedule since 9/28/2018.
        PySpark replacement: scheduled micro-batch polling loop on NWK_NEW_EHRP_ACTIONS_TBL.
        """
        self.job_metrics.mark_started()
        logger.info(f"Starting {WORKFLOW_NAME} (run_forever={run_forever})")

        try:
            if run_forever:
                self._run_forever_loop(poll_interval_seconds)
            else:
                self._run_single_batch()

            self.job_metrics.mark_succeeded()

        except Exception as e:
            self.job_metrics.mark_failed()
            logger.error(f"{WORKFLOW_NAME} failed: {e}")
            self.email.send_failure_email(
                job_name=WORKFLOW_NAME,
                session_name=MAPPING_NAME,
                error_message=str(e),
                recipients=self.wf_email_list
            )
            raise

        return self.job_metrics

    def _run_forever_loop(self, poll_interval: int) -> None:
        """Implement RUNFOREVER pattern as micro-batch polling loop.
        
        Replaces Informatica RUNFOREVER schedule:
        - Polls NWK_NEW_EHRP_ACTIONS_TBL for new records
        - Processes new records in micro-batches
        - Sleeps between polls
        """
        logger.info(f"Starting RUNFOREVER loop (poll every {poll_interval}s)")

        while True:
            try:
                new_records = self.db.read_jdbc(
                    "NWK_NEW_EHRP_ACTIONS_TBL",
                    connection="source",
                    predicate="PROCESSED_FLAG IS NULL OR PROCESSED_FLAG = 'N'"
                )
                
                if new_records.count() > 0:
                    logger.info(f"Found {new_records.count()} new records to process")
                    self._run_single_batch()
                else:
                    logger.debug("No new records found, sleeping...")

            except Exception as e:
                logger.error(f"Batch processing error: {e}")
                self.email.send_failure_email(
                    job_name=WORKFLOW_NAME,
                    session_name=MAPPING_NAME,
                    error_message=str(e),
                    recipients=self.wf_email_list
                )

            time.sleep(poll_interval)

    def _run_single_batch(self) -> None:
        """Execute a single batch of EHRP2BIIS processing."""

        # Step 1: Run pre-load script (ehrp2biis_preload)
        self._run_preload_script()

        # Step 2: Read source data (SQ_PS_GVT_JOB join)
        source_df = self._read_source_join()

        # Step 3: Execute all 9 lookups
        enriched_df = self._execute_lookups(source_df)

        # Step 4: Write to 3 target tables in parallel
        self._write_targets(enriched_df)

        # Step 5: Send completion email
        self._send_completion_email()

    def _run_preload_script(self) -> None:
        """Execute ehrp2biis_preload script.
        
        Replaces KSH script that runs SQL*Plus step01.
        Original script: ehrp2biis_preload (lines 1-65)
        
        Reads credentials from:
        - /home/sa-biisint/.use1 (username for source DB)
        - /home/sa-biisint/.pw1 (password for source DB)
        
        Executes: step01.sql from /data/BIISINT/bin/EHRP2BIIS/
        
        Error detection (lines 54-60):
        - grep -i "ERROR" $logfile → send failure email
        """
        metrics = SessionMetrics(
            session_name="s_EHRP2BIIS_Preload",
            mapping_name="ehrp2biis_preload"
        )
        metrics.mark_started()
        logger.info("Running pre-load script (ehrp2biis_preload → step01.sql)")

        try:
            step01_path = os.path.join(
                self.config.paths.ehrp2biis_bin_dir, "step01.sql"
            )

            if os.path.exists(step01_path):
                with open(step01_path, "r") as f:
                    sql_content = f.read()

                # Execute SQL via oracledb (replaces SQL*Plus execution)
                self.db.execute_sql(sql_content, connection="source")
                logger.info("step01.sql executed successfully")
            else:
                logger.warning(f"step01.sql not found at {step01_path}, skipping")

            metrics.mark_succeeded()

        except Exception as e:
            # Error detection (replaces grep -i "ERROR" $logfile)
            error_msg = f"EHRP2BIIS Preload script did not complete successfully: {e}"
            metrics.mark_failed(error_code=-1, error_msg=error_msg)
            logger.error(error_msg)
            self.email.send_failure_email(
                job_name=WORKFLOW_NAME,
                session_name="ehrp2biis_preload",
                error_message=error_msg,
                recipients=self.wf_email_list
            )
            raise
        finally:
            self.job_metrics.add_session(metrics)

    def _read_source_join(self) -> DataFrame:
        """Read source data using SQ_PS_GVT_JOB source qualifier join.
        
        Replaces Informatica Source Qualifier (lines 1954-1957):
        SELECT * FROM PS_GVT_JOB, NWK_NEW_EHRP_ACTIONS_TBL
        WHERE NWK_NEW_EHRP_ACTIONS_TBL.EMPLID = PS_GVT_JOB.EMPLID
          AND NWK_NEW_EHRP_ACTIONS_TBL.EMPL_RCD = PS_GVT_JOB.EMPL_RCD
          AND NWK_NEW_EHRP_ACTIONS_TBL.EFFDT = PS_GVT_JOB.EFFDT
          AND NWK_NEW_EHRP_ACTIONS_TBL.EFFSEQ = PS_GVT_JOB.EFFSEQ
        ORDER BY PS_GVT_JOB.EFFDT
        """
        metrics = SessionMetrics(
            session_name="s_EHRP2BIIS_Read_Source",
            mapping_name=MAPPING_NAME
        )
        metrics.mark_started()
        logger.info("Reading source join (PS_GVT_JOB ⨝ NWK_NEW_EHRP_ACTIONS_TBL)")

        try:
            # Use pushdown query for efficient source join
            # Select gvt.* and only non-overlapping nwk columns to avoid
            # duplicate column names (EMPLID, EMPL_RCD, EFFDT, EFFSEQ exist in both)
            source_query = """
                (SELECT gvt.*,
                        nwk.ACTION, nwk.ACTION_REASON, nwk.ACTION_DT,
                        nwk.DEPTID AS NWK_DEPTID, nwk.JOBCODE AS NWK_JOBCODE,
                        nwk.POSITION_NBR AS NWK_POSITION_NBR,
                        nwk.GVT_PAR_NBR, nwk.GVT_NOA_CODE, nwk.GVT_LEG_AUTH_1,
                        nwk.PROCESSED_FLAG, nwk.CREATED_DATE, nwk.PROCESSED_DATE
                 FROM PS_GVT_JOB gvt
                 INNER JOIN NWK_NEW_EHRP_ACTIONS_TBL nwk
                     ON nwk.EMPLID = gvt.EMPLID
                    AND nwk.EMPL_RCD = gvt.EMPL_RCD
                    AND nwk.EFFDT = gvt.EFFDT
                    AND nwk.EFFSEQ = gvt.EFFSEQ
                 ORDER BY gvt.EFFDT) sq_ps_gvt_job
            """
            source_df = self.db.read_jdbc(
                source_query,
                connection="source"
            )
            metrics.src_success_rows = source_df.count()
            metrics.mark_succeeded()
            logger.info(f"Read {metrics.src_success_rows} joined source records")
            return source_df

        except Exception as e:
            metrics.mark_failed(error_code=-1, error_msg=str(e))
            raise
        finally:
            self.job_metrics.add_session(metrics)

    def _execute_lookups(self, source_df: DataFrame) -> DataFrame:
        """Execute all 9 lookup transformations.
        
        All lookups at Stage 4 (XML/EHRP2BIIS_UPDATE lines 2621-2658).
        Each lookup reads from a PeopleSoft table and enriches the source record.
        
        Informatica lookup cache = Auto → PySpark broadcast join.
        
        IMPORTANT: lkp_PS_JPM_JP_ITEMS uses INFO_NATE connection
        (separate Oracle DB, lines 2752-2754).
        """
        metrics = SessionMetrics(
            session_name="s_EHRP2BIIS_Lookups",
            mapping_name=MAPPING_NAME
        )
        metrics.mark_started()
        logger.info("Executing 9 lookup transformations")

        enriched_df = source_df

        try:
            for lookup_config in LOOKUP_TABLES:
                lkp_name = lookup_config["name"]
                table = lookup_config["table"]
                join_keys = lookup_config["join_keys"]
                connection = lookup_config["connection"]

                logger.info(f"  Lookup: {lkp_name} ({table} via {connection})")

                # Read lookup table (with caching for repeated use)
                if lkp_name not in self._lookup_cache:
                    lkp_df = self.db.read_jdbc(table, connection=connection)
                    # Use broadcast join (replaces Informatica Auto cache)
                    self._lookup_cache[lkp_name] = broadcast(lkp_df)

                lkp_df = self._lookup_cache[lkp_name]

                # Left outer join (lookup returns NULL if no match)
                # Deduplicate join keys to avoid ambiguity
                enriched_df = enriched_df.join(
                    lkp_df,
                    on=join_keys,
                    how="left_outer"
                )

            metrics.mark_succeeded()
            logger.info("All 9 lookups completed")

        except Exception as e:
            metrics.mark_failed(error_code=-1, error_msg=str(e))
            raise
        finally:
            self.job_metrics.add_session(metrics)

        return enriched_df

    def _write_targets(self, enriched_df: DataFrame) -> None:
        """Write to 3 target tables in parallel.
        
        Target tables (XML/EHRP2BIIS_UPDATE lines 2558-2560):
        1. NWK_ACTION_PRIMARY_TBL — primary action data
        2. NWK_ACTION_SECONDARY_TBL — secondary action data
        3. EHRP_RECS_TRACKING_TBL — tracking/audit records
        
        All via INFO_TARGET connection.
        """
        metrics = SessionMetrics(
            session_name="s_EHRP2BIIS_Write_Targets",
            mapping_name=MAPPING_NAME
        )
        metrics.mark_started()
        logger.info("Writing to 3 target tables")

        try:
            # Add metadata columns
            output_df = enriched_df.withColumn(
                "RUN_DATE", lit(self.session_start_time)
            ).withColumn(
                "PROCESS_NAME", lit(MAPPING_NAME)
            )

            # Cache the output DataFrame since it's written to 3 targets
            output_df.cache()
            total_rows = output_df.count()

            # Write to NWK_ACTION_PRIMARY_TBL
            self.db.write_jdbc(output_df, "NWK_ACTION_PRIMARY_TBL", mode="append")
            logger.info(f"  Written {total_rows} rows to NWK_ACTION_PRIMARY_TBL")

            # Write to NWK_ACTION_SECONDARY_TBL
            self.db.write_jdbc(output_df, "NWK_ACTION_SECONDARY_TBL", mode="append")
            logger.info(f"  Written {total_rows} rows to NWK_ACTION_SECONDARY_TBL")

            # Write to EHRP_RECS_TRACKING_TBL
            tracking_df = output_df.withColumn(
                "TRACKING_STATUS", lit("PROCESSED")
            ).withColumn(
                "PROCESSED_DATE", current_timestamp()
            )
            self.db.write_jdbc(tracking_df, "EHRP_RECS_TRACKING_TBL", mode="append")
            logger.info(f"  Written {total_rows} rows to EHRP_RECS_TRACKING_TBL")

            output_df.unpersist()

            metrics.tgt_success_rows = total_rows * 3  # 3 targets
            metrics.mark_succeeded()

        except Exception as e:
            metrics.mark_failed(error_code=-1, error_msg=str(e))
            raise
        finally:
            self.job_metrics.add_session(metrics)

    def _send_completion_email(self) -> None:
        """Send completion email for EHRP2BIIS processing."""
        env_prefix = self.config.env_prefix
        subject = f"{env_prefix}EHRP2BIIS Update completed successfully"
        message = (
            f"EHRP2BIIS Update processing completed at {datetime.now()}\n"
            f"Job metrics:\n{self.job_metrics.summary()}"
        )
        self.email.send_email(
            subject=subject,
            body=message,
            recipients=self.wf_email_list
        )
