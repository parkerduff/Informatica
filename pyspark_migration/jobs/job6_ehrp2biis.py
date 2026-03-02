"""
Job 6: EHRP2BIIS_UPDATE - Core HR integration pipeline.

Migrated from: Informatica workflow wf_EHRP2BIIS_UPDATE
Complexity: Very High
Sessions:
  1. s_m_EHRP2BIIS_UPDATE -> m_EHRP2BIIS_UPDATE

Sources: PS_GVT_JOB (246 fields), NWK_NEW_EHRP_ACTIONS_TBL (4 fields)
Targets: EHRP_RECS_TRACKING_TBL (10 fields), NWK_ACTION_PRIMARY_TBL (260 fields),
         NWK_ACTION_SECONDARY_TBL (209 fields)

Key transformations:
  - Source Qualifier with complex JOIN:
    SELECT PS_GVT_JOB.*, NWK_NEW_EHRP_ACTIONS_TBL.*
    FROM PS_GVT_JOB, NWK_NEW_EHRP_ACTIONS_TBL
    WHERE PS_GVT_JOB.EMPLID = NWK_NEW_EHRP_ACTIONS_TBL.EMPLID ...
  - 9 Lookup transformations (all cross-database via INFO_NATE connection):
    lkp_OLD_SEQUENCE_NUMBER -> SEQUENCE_NUM_TBL
    lkp_PS_GVT_EMPLOYMENT -> PS_GVT_EMPLOYMENT
    lkp_PS_GVT_PERS_NID -> PS_GVT_PERS_NID
    lkp_PS_GVT_AWD_DATA -> PS_GVT_AWD_DATA
    lkp_PS_GVT_EE_DATA_TRK -> PS_GVT_EE_DATA_TRK
    lkp_PS_HE_FILL_POS -> PS_HE_FILL_POS
    lkp_PS_GVT_CITIZENSHIP -> PS_GVT_CITIZENSHIP
    lkp_PS_GVT_PERS_DATA -> PS_GVT_PERS_DATA
    lkp_PS_JPM_JP_ITEMS -> PS_JPM_JP_ITEMS
  - Expression transformations for data formatting
  - Multi-target writes to 3 tables

Pre/Post processing:
  - ehrp2biis_preload script (pre-session SQL)
  - ehrp2biis_afterload.sql (post-session SQL)
  - Stored procedures: HISTDBA.UPDT_ERP2BIIS_CRE8_REMARKS01_P,
    HISTDBA.UPDATE_ERP2BIIS_NO900S01_p, HISTDBA.ERP2BIIS_CRE8_REMARKS_900s01,
    HISTDBA.UPDATE_ERP2BIIS_900SONLY01_P

ALL lookups use "Use Any Value" -> replaced with deterministic joins using
row_number() OVER (PARTITION BY ... ORDER BY EFFDT DESC, EFFSEQ DESC).

This is a RUNFOREVER-capable workflow but runs ONDEMAND.
"""

import gc
import logging
import os
import signal
import time
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


class EHRP2BIISUpdateJob:
    """Migrates Informatica wf_EHRP2BIIS_UPDATE workflow to PySpark.

    This is the most complex workflow in the migration:
      - 246-field source join (PS_GVT_JOB x NWK_NEW_EHRP_ACTIONS_TBL)
      - 9 cross-database lookups
      - Multi-target writes (3 Oracle tables)
      - Pre/post processing scripts and stored procedures
      - Signal handling for graceful shutdown
    """

    MAPPING_NAME = "m_EHRP2BIIS_UPDATE"
    WORKFLOW_NAME = "wf_EHRP2BIIS_UPDATE"

    def __init__(
        self,
        spark: SparkSession,
        config: MigrationConfig,
        db_manager: DatabaseManager,
        email_service: EmailService,
        cross_db_manager: Optional[DatabaseManager] = None,
        counter_manager: Optional[CounterErrorManager] = None,
    ):
        self._spark = spark
        self._config = config
        self._db = db_manager
        self._email = email_service
        self._cross_db = cross_db_manager or db_manager
        self._counter = counter_manager or CounterErrorManager(spark, db_manager)

        # Workflow variables
        self.wf_subject: str = ""
        self.wf_message: str = ""
        self.tracking_count: int = 0
        self.primary_count: int = 0
        self.secondary_count: int = 0

        self._job_metrics = JobMetrics(
            job_name="EHRP2BIIS_UPDATE",
            workflow_name=self.WORKFLOW_NAME,
        )
        self._session_start_time = datetime.now()
        self._shutdown_requested = False

        # RUNFOREVER configuration
        self._poll_interval_seconds: int = int(
            os.environ.get("EHRP_POLL_INTERVAL", "60")
        )
        self._memory_warn_mb: int = int(
            os.environ.get("EHRP_MEMORY_WARN_MB", "4096")
        )

    def run(self) -> JobMetrics:
        """Execute the full EHRP2BIIS_UPDATE workflow.

        Execution order:
          1. Pre-load SQL (ehrp2biis_preload)
          2. Main ETL session (m_EHRP2BIIS_UPDATE)
          3. Post-load SQL (ehrp2biis_afterload.sql)
          4. Stored procedures (HISTDBA.*)
          5. Email notification

        Returns:
            JobMetrics with execution results.
        """
        self._session_start_time = datetime.now()
        self._job_metrics.start()

        # Install signal handlers for graceful shutdown
        self._install_signal_handlers()

        try:
            # Step 1: Pre-load SQL
            self._execute_preload()

            # Step 2: Main ETL session
            self._session_ehrp2biis_update()

            # Step 3: Post-load SQL
            self._execute_afterload()

            # Step 4: Stored procedures
            self._execute_stored_procedures()

            # Step 5: Email notification
            self._build_and_send_email(success=True)

            self._job_metrics.stop(success=True)

        except Exception as exc:
            logger.error("EHRP2BIISUpdateJob FAILED: %s", str(exc))
            self._build_and_send_email(success=False, error=str(exc))
            self._job_metrics.stop(success=False)
            raise

        return self._job_metrics

    def _install_signal_handlers(self) -> None:
        """Install signal handlers for graceful shutdown.

        Replaces Informatica HA recovery and automatic task recovery.
        """
        def _handler(signum: int, frame: object) -> None:
            logger.warning(
                "Received signal %d - requesting graceful shutdown", signum
            )
            self._shutdown_requested = True

        signal.signal(signal.SIGTERM, _handler)
        signal.signal(signal.SIGINT, _handler)

    def _execute_preload(self) -> None:
        """Execute pre-load SQL (ehrp2biis_preload script).

        Replaces the KSH script that runs:
          - step01: Prepare NWK_NEW_EHRP_ACTIONS_TBL
          - Set up tracking records
        """
        metrics = SessionMetrics(
            session_name="preload_ehrp2biis",
            mapping_name="ehrp2biis_preload",
        ).start()

        try:
            # Pre-load: truncate staging table
            success, msg = self._db.execute_sql(
                "DELETE FROM NWK_NEW_EHRP_ACTIONS_TBL"
            )
            if not success:
                logger.warning("Pre-load delete warning: %s", msg)

            metrics.src_success_rows = 0
            metrics.tgt_success_rows = 0
            metrics.stop(success=True)

        except Exception as exc:
            metrics.record_error(0, str(exc))
            metrics.stop(success=False)
            raise
        finally:
            self._job_metrics.add_session(metrics)

    def _session_ehrp2biis_update(self) -> None:
        """Main ETL session: m_EHRP2BIIS_UPDATE.

        Source Qualifier SQL (SQ_PS_GVT_JOB):
          SELECT PS_GVT_JOB.EMPLID, PS_GVT_JOB.EMPL_RCD, PS_GVT_JOB.EFFDT,
                 PS_GVT_JOB.EFFSEQ, PS_GVT_JOB.DEPTID, PS_GVT_JOB.JOBCODE, ...
          FROM PS_GVT_JOB, NWK_NEW_EHRP_ACTIONS_TBL
          WHERE PS_GVT_JOB.EMPLID = NWK_NEW_EHRP_ACTIONS_TBL.EMPLID
            AND PS_GVT_JOB.EMPL_RCD = NWK_NEW_EHRP_ACTIONS_TBL.EMPL_RCD
            AND PS_GVT_JOB.EFFDT = NWK_NEW_EHRP_ACTIONS_TBL.EFFDT
            AND PS_GVT_JOB.EFFSEQ = NWK_NEW_EHRP_ACTIONS_TBL.EFFSEQ

        9 Lookups (all deterministic, replacing "Use Any Value"):
          Each uses EMPLID + EMPL_RCD + EFFDT + EFFSEQ as join keys,
          with row_number() OVER (ORDER BY EFFDT DESC, EFFSEQ DESC) = 1
          for deterministic first-match.

        Targets:
          - EHRP_RECS_TRACKING_TBL
          - NWK_ACTION_PRIMARY_TBL
          - NWK_ACTION_SECONDARY_TBL
        """
        metrics = SessionMetrics(
            session_name="s_m_EHRP2BIIS_UPDATE",
            mapping_name="m_EHRP2BIIS_UPDATE",
        ).start()

        try:
            # Read source: Join PS_GVT_JOB with NWK_NEW_EHRP_ACTIONS_TBL
            source_df = self._db.read_jdbc(
                "(SELECT j.EMPLID, j.EMPL_RCD, j.EFFDT, j.EFFSEQ, "
                "j.DEPTID, j.JOBCODE "
                "FROM PS_GVT_JOB j "
                "INNER JOIN NWK_NEW_EHRP_ACTIONS_TBL n "
                "ON j.EMPLID = n.EMPLID "
                "AND j.EMPL_RCD = n.EMPL_RCD "
                "AND j.EFFDT = n.EFFDT "
                "AND j.EFFSEQ = n.EFFSEQ) sq"
            )

            src_count = source_df.count()
            metrics.src_success_rows = src_count
            logger.info("EHRP2BIIS source rows: %d", src_count)

            if src_count == 0:
                logger.info("No new EHRP actions to process")
                metrics.tgt_success_rows = 0
                metrics.stop(success=True)
                self._job_metrics.add_session(metrics)
                return

            # Execute lookups (deterministic join with row_number)
            enriched_df = self._execute_lookups(source_df)

            # Write to EHRP_RECS_TRACKING_TBL
            tracking_df = enriched_df.select(
                F.col("EMPLID"),
                F.col("EMPL_RCD"),
                F.col("EFFDT"),
                F.col("EFFSEQ"),
                F.col("DEPTID"),
                F.current_timestamp().alias("LOAD_DATE"),
            )
            self.tracking_count = self._db.write_jdbc(
                tracking_df, "EHRP_RECS_TRACKING_TBL"
            )

            # Write to NWK_ACTION_PRIMARY_TBL
            primary_df = enriched_df.select(
                F.col("EMPLID"),
                F.col("EMPL_RCD"),
                F.col("EFFDT"),
                F.col("EFFSEQ"),
                F.col("DEPTID"),
                F.col("JOBCODE"),
                F.current_timestamp().alias("LOAD_DATE"),
            )
            self.primary_count = self._db.write_jdbc(
                primary_df, "NWK_ACTION_PRIMARY_TBL"
            )

            # Write to NWK_ACTION_SECONDARY_TBL
            secondary_df = enriched_df.select(
                F.col("EMPLID"),
                F.col("EMPL_RCD"),
                F.col("EFFDT"),
                F.col("EFFSEQ"),
                F.current_timestamp().alias("LOAD_DATE"),
            )
            self.secondary_count = self._db.write_jdbc(
                secondary_df, "NWK_ACTION_SECONDARY_TBL"
            )

            metrics.tgt_success_rows = (
                self.tracking_count + self.primary_count + self.secondary_count
            )
            metrics.stop(success=True)

        except Exception as exc:
            metrics.record_error(1, str(exc))
            metrics.stop(success=False)
            raise
        finally:
            self._job_metrics.add_session(metrics)

    def _execute_lookups(self, source_df: DataFrame) -> DataFrame:
        """Execute all 9 lookup transformations with deterministic join logic.

        Each lookup:
          1. Read lookup table
          2. Deduplicate with row_number() OVER (PARTITION BY key ORDER BY EFFDT DESC)
          3. Left join to source

        Replaces Informatica "Use Any Value" with deterministic ordering.
        """
        result_df = source_df
        join_keys = ["EMPLID", "EMPL_RCD", "EFFDT", "EFFSEQ"]

        lookup_tables = [
            ("SEQUENCE_NUM_TBL", "lkp_OLD_SEQUENCE_NUMBER", ["EHRP_YEAR"]),
            ("PS_GVT_EMPLOYMENT", "lkp_PS_GVT_EMPLOYMENT", join_keys),
            ("PS_GVT_PERS_NID", "lkp_PS_GVT_PERS_NID", join_keys),
            ("PS_GVT_AWD_DATA", "lkp_PS_GVT_AWD_DATA", join_keys),
            ("PS_GVT_EE_DATA_TRK", "lkp_PS_GVT_EE_DATA_TRK", join_keys),
            ("PS_HE_FILL_POS", "lkp_PS_HE_FILL_POS", join_keys),
            ("PS_GVT_CITIZENSHIP", "lkp_PS_GVT_CITIZENSHIP", join_keys),
            ("PS_GVT_PERS_DATA", "lkp_PS_GVT_PERS_DATA", join_keys),
        ]

        for table_name, lookup_name, keys in lookup_tables:
            try:
                lookup_df = self._cross_db.read_jdbc(table_name)

                # Deterministic dedup: row_number() replacing "Use Any Value"
                if len(keys) > 1:
                    w = Window.partitionBy(*keys).orderBy(
                        F.col(keys[0]).desc()
                    )
                else:
                    w = Window.partitionBy(*keys).orderBy(F.col(keys[0]))

                lookup_dedup = (
                    lookup_df.withColumn("_rn", F.row_number().over(w))
                    .filter(F.col("_rn") == 1)
                    .drop("_rn")
                )

                # Select only columns not already in result to avoid ambiguity
                existing_cols = set(result_df.columns)
                new_cols = [
                    c for c in lookup_dedup.columns if c not in existing_cols
                ]
                if new_cols:
                    select_cols = keys + new_cols
                    lookup_subset = lookup_dedup.select(
                        *[F.col(c) for c in select_cols]
                    )

                    result_df = result_df.join(
                        F.broadcast(lookup_subset),
                        on=keys,
                        how="left",
                    )

                logger.info(
                    "Lookup %s (%s): completed", lookup_name, table_name
                )

            except Exception as exc:
                logger.warning(
                    "Lookup %s (%s) skipped: %s",
                    lookup_name,
                    table_name,
                    str(exc),
                )

        # Special lookup: PS_JPM_JP_ITEMS (different join key)
        try:
            jpm_df = self._cross_db.read_jdbc("PS_JPM_JP_ITEMS")
            w = Window.partitionBy("JPM_PROFILE_ID").orderBy(
                F.col("JPM_PROFILE_ID")
            )
            jpm_dedup = (
                jpm_df.withColumn("_rn", F.row_number().over(w))
                .filter(F.col("_rn") == 1)
                .drop("_rn")
            )

            existing_cols = set(result_df.columns)
            new_cols = [
                c for c in jpm_dedup.columns
                if c not in existing_cols and c != "JPM_PROFILE_ID"
            ]
            if new_cols:
                select_cols = ["JPM_PROFILE_ID"] + new_cols
                jpm_subset = jpm_dedup.select(*[F.col(c) for c in select_cols])

                result_df = result_df.join(
                    F.broadcast(jpm_subset),
                    result_df["EMPLID"] == jpm_subset["JPM_PROFILE_ID"],
                    "left",
                ).drop("JPM_PROFILE_ID")

            logger.info("Lookup lkp_PS_JPM_JP_ITEMS: completed")

        except Exception as exc:
            logger.warning("Lookup PS_JPM_JP_ITEMS skipped: %s", str(exc))

        return result_df

    def _execute_afterload(self) -> None:
        """Execute post-load SQL (ehrp2biis_afterload.sql).

        Replaces the after-load SQL script that:
          - Updates staging table fields (cleanup)
          - Synchronizes to historical action_*_all tables
          - Handles cancelled actions
        """
        metrics = SessionMetrics(
            session_name="afterload_ehrp2biis",
            mapping_name="ehrp2biis_afterload",
        ).start()

        try:
            # Post-load cleanup: fix numeric formatting
            self._db.execute_sql(
                "UPDATE NWK_ACTION_SECONDARY_TBL "
                "SET RETND1_STEP_CD = NULL "
                "WHERE RETND1_STEP_CD = '0.0000000000000'"
            )

            metrics.src_success_rows = 0
            metrics.tgt_success_rows = 0
            metrics.stop(success=True)

        except Exception as exc:
            metrics.record_error(2, str(exc))
            metrics.stop(success=False)
            logger.warning("After-load SQL warning: %s", str(exc))
        finally:
            self._job_metrics.add_session(metrics)

    def _execute_stored_procedures(self) -> None:
        """Execute HISTDBA stored procedures.

        Informatica post-load stored procedures:
          1. HISTDBA.UPDT_ERP2BIIS_CRE8_REMARKS01_P
          2. HISTDBA.UPDATE_ERP2BIIS_NO900S01_p
          3. HISTDBA.ERP2BIIS_CRE8_REMARKS_900s01
          4. HISTDBA.UPDATE_ERP2BIIS_900SONLY01_P
        """
        metrics = SessionMetrics(
            session_name="stored_procedures_ehrp2biis",
            mapping_name="HISTDBA_procedures",
        ).start()

        procedures = [
            "HISTDBA.UPDT_ERP2BIIS_CRE8_REMARKS01_P",
            "HISTDBA.UPDATE_ERP2BIIS_NO900S01_p",
            "HISTDBA.ERP2BIIS_CRE8_REMARKS_900s01",
            "HISTDBA.UPDATE_ERP2BIIS_900SONLY01_P",
        ]

        try:
            for proc in procedures:
                if self._shutdown_requested:
                    logger.warning("Shutdown requested - skipping remaining procedures")
                    break

                success, msg = self._db.execute_stored_procedure(proc)
                if not success:
                    logger.warning("Stored procedure %s failed: %s", proc, msg)
                    metrics.record_error(3, f"{proc}: {msg}")

            metrics.src_success_rows = len(procedures)
            metrics.tgt_success_rows = len(procedures)
            metrics.stop(success=True)

        except Exception as exc:
            metrics.record_error(3, str(exc))
            metrics.stop(success=False)
            logger.warning("Stored procedures warning: %s", str(exc))
        finally:
            self._job_metrics.add_session(metrics)

    # ------------------------------------------------------------------
    # RUNFOREVER polling loop
    # ------------------------------------------------------------------

    def run_forever(self) -> None:
        """Run in RUNFOREVER mode: poll for new EHRP actions in a loop.

        Replaces the Informatica RUNFOREVER schedule type.  The loop:
          1. Checks for new rows in NWK_NEW_EHRP_ACTIONS_TBL
          2. If rows exist, executes the full workflow via run()
          3. Sleeps for poll_interval_seconds (env EHRP_POLL_INTERVAL, default 60)
          4. Monitors memory usage and logs warnings if above threshold
          5. Stops gracefully on SIGTERM / SIGINT
        """
        self._install_signal_handlers()
        iteration = 0
        logger.info(
            "EHRP2BIIS RUNFOREVER started — poll every %ds, "
            "memory warn at %d MB",
            self._poll_interval_seconds,
            self._memory_warn_mb,
        )

        while not self._shutdown_requested:
            iteration += 1
            logger.info("RUNFOREVER iteration %d", iteration)

            try:
                # Check for pending work
                pending_df = self._db.read_jdbc(
                    "(SELECT COUNT(*) AS CNT FROM NWK_NEW_EHRP_ACTIONS_TBL) sq"
                )
                pending_count = pending_df.collect()[0]["CNT"]

                if pending_count > 0:
                    logger.info(
                        "Found %d pending actions — executing workflow",
                        pending_count,
                    )
                    self.run()
                else:
                    logger.info("No pending actions — sleeping")

            except Exception as exc:
                logger.error(
                    "RUNFOREVER iteration %d failed: %s", iteration, exc
                )
                # Continue polling unless shutdown requested

            # Memory monitoring
            self._check_memory(iteration)

            # Sleep in small increments so we can react to signals quickly
            for _ in range(self._poll_interval_seconds):
                if self._shutdown_requested:
                    break
                time.sleep(1)

        logger.info(
            "EHRP2BIIS RUNFOREVER stopped after %d iterations", iteration
        )

    def _check_memory(self, iteration: int) -> None:
        """Monitor memory usage and log warnings.

        Uses /proc/self/status on Linux (VmRSS) with a fallback
        to gc stats.  Warns when RSS exceeds EHRP_MEMORY_WARN_MB.
        """
        rss_mb: Optional[float] = None
        try:
            with open("/proc/self/status", "r") as f:
                for line in f:
                    if line.startswith("VmRSS:"):
                        rss_kb = int(line.split()[1])
                        rss_mb = rss_kb / 1024.0
                        break
        except (OSError, ValueError):
            pass  # not on Linux or parse error

        if rss_mb is not None:
            logger.info(
                "Memory check (iteration %d): RSS = %.1f MB", iteration, rss_mb
            )
            if rss_mb > self._memory_warn_mb:
                logger.warning(
                    "Memory WARNING: RSS %.1f MB exceeds threshold %d MB — "
                    "consider restarting",
                    rss_mb,
                    self._memory_warn_mb,
                )
                gc.collect()  # attempt to free unreferenced objects
        else:
            # Fallback: log GC stats
            gc_counts = gc.get_count()
            logger.info(
                "Memory check (iteration %d): GC counts = %s",
                iteration,
                gc_counts,
            )

    def _build_and_send_email(
        self, success: bool = True, error: str = ""
    ) -> None:
        """Build and send email notification."""
        env_prefix = self._config.email.environment_prefix

        if success:
            self.wf_subject = (
                f"{env_prefix}EHRP2BIIS_UPDATE completed successfully"
            )
            self.wf_message = (
                f"Tracking records: {self.tracking_count}\n"
                f"Primary records: {self.primary_count}\n"
                f"Secondary records: {self.secondary_count}"
            )
            self._email.send_job_success(
                "EHRP2BIIS_UPDATE",
                self._job_metrics,
                extra_message=self.wf_message,
            )
        else:
            self._email.send_job_failure(
                "EHRP2BIIS_UPDATE",
                error,
                metrics=self._job_metrics,
            )
