"""
Job 3: m_Pseudossn_Load_Pseudossn_From_SDA_Tbl → PySpark Migration
Source: XML/Pseudossn

Informatica Mapping: m_Pseudossn_Load_Pseudossn_From_SDA_Tbl
Source: PSEUDOSSN_FROM_SDA_TBL (Oracle ORA_BIIS) + PSEUDOSSN_FILE_TK_NUM (flat file)
Target: PSEUDOSSN_TBL

Key Transformation — exp_Conversions (lines 3614-3641):
- Date parsing: SUBSTR(in_HIRE_DATE, 1, 2) || '/' || ... (MMDDYYYY → MM/DD/YYYY)
- Signed numeric parsing for UNIF_ALLOW_AMT (lines 3639-3641)
- upd_Update_TK_NUM Update Strategy (DD_UPDATE)
"""

import logging
from datetime import datetime
from typing import Optional

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, when, substring, concat, lit, to_date, trim, length
)

from pyspark_migration.common.config import MigrationConfig
from pyspark_migration.common.db_manager import DatabaseManager
from pyspark_migration.common.email_service import EmailService
from pyspark_migration.common.counter_error import CounterManager
from pyspark_migration.common.logging_utils import SessionMetrics, JobMetrics

logger = logging.getLogger(__name__)

MAPPING_NAME = "m_Pseudossn_Load_Pseudossn_From_SDA_Tbl"
WORKFLOW_NAME = "wf_Pseudossn"


class PseudossnJob:
    """PySpark implementation of m_Pseudossn_Load_Pseudossn_From_SDA_Tbl.
    
    Loads pseudo-SSN data from Oracle source table and flat file,
    applies date and numeric conversions, and writes to PSEUDOSSN_TBL.
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
            job_name="Job3_Pseudossn",
            workflow_name=WORKFLOW_NAME
        )
        self.session_start_time = datetime.now()

    def run(self) -> JobMetrics:
        """Execute the Pseudossn mapping."""
        self.job_metrics.mark_started()
        logger.info(f"Starting {WORKFLOW_NAME}")

        try:
            # Step 1: Read source data
            source_df = self._read_sources()

            # Step 2: Apply exp_Conversions transformation
            converted_df = self._apply_conversions(source_df)

            # Step 3: Write to PSEUDOSSN_TBL
            self._write_target(converted_df)

            # Step 4: Update TK_NUM (upd_Update_TK_NUM)
            self._update_tk_num()

            self.job_metrics.mark_succeeded()
            logger.info(f"{WORKFLOW_NAME} completed successfully")

        except Exception as e:
            self.job_metrics.mark_failed()
            logger.error(f"{WORKFLOW_NAME} failed: {e}")
            self.email.send_failure_email(
                job_name=WORKFLOW_NAME,
                session_name=MAPPING_NAME,
                error_message=str(e)
            )
            raise

        return self.job_metrics

    def _read_sources(self) -> DataFrame:
        """Read source data from PSEUDOSSN_FROM_SDA_TBL (Oracle) and flat file.
        
        Replaces Informatica Source Qualifier reading from:
        - PSEUDOSSN_FROM_SDA_TBL (Oracle ORA_BIIS)
        - PSEUDOSSN_FILE_TK_NUM (flat file)
        """
        metrics = SessionMetrics(
            session_name="s_Pseudossn_Read_Sources",
            mapping_name=MAPPING_NAME
        )
        metrics.mark_started()

        try:
            # Read Oracle source
            sda_df = self.db.read_jdbc("PSEUDOSSN_FROM_SDA_TBL", connection="target")
            metrics.src_success_rows = sda_df.count()

            # Read flat file if exists
            tk_num_path = f"{self.config.paths.root_directory}/data/int/in/PSEUDOSSN/pseudossn_file_tk_num.txt"
            try:
                tk_num_df = self.spark.read.option("header", "false").csv(tk_num_path)
                sda_df = sda_df.crossJoin(tk_num_df.limit(1))
            except Exception:
                logger.warning(f"TK_NUM flat file not found at {tk_num_path}, proceeding without")

            metrics.mark_succeeded()
            self.job_metrics.add_session(metrics)
            return sda_df

        except Exception as e:
            metrics.mark_failed(error_code=-1, error_msg=str(e))
            self.job_metrics.add_session(metrics)
            raise

    def _apply_conversions(self, df: DataFrame) -> DataFrame:
        """Apply exp_Conversions transformation (XML/Pseudossn lines 3614-3641).
        
        1. Date parsing: MMDDYYYY → MM/DD/YYYY
           Informatica: SUBSTR(in_HIRE_DATE, 1, 2) || '/' || SUBSTR(in_HIRE_DATE, 3, 2) || '/' || SUBSTR(in_HIRE_DATE, 5, 4)
           PySpark: to_date(col("in_HIRE_DATE"), "MMddyyyy")
        
        2. Signed numeric parsing for UNIF_ALLOW_AMT (lines 3639-3641):
           Informatica: DECODE(TRUE, IS_NUMBER(v) AND sign='+', TO_DECIMAL(v,2), ...)
           PySpark: when(sign == "+", amt.cast("decimal(10,2)"))...
        """
        metrics = SessionMetrics(
            session_name="s_Pseudossn_Conversions",
            mapping_name=MAPPING_NAME
        )
        metrics.mark_started()

        try:
            result_df = df

            # Date parsing: MMDDYYYY → date type
            if "HIRE_DATE" in result_df.columns:
                result_df = result_df.withColumn(
                    "HIRE_DATE_CONVERTED",
                    when(
                        (col("HIRE_DATE").isNotNull()) & (length(col("HIRE_DATE")) == 8),
                        to_date(col("HIRE_DATE"), "MMddyyyy")
                    )
                )

            if "EFFECTIVE_DATE" in result_df.columns:
                result_df = result_df.withColumn(
                    "EFFECTIVE_DATE_CONVERTED",
                    when(
                        (col("EFFECTIVE_DATE").isNotNull()) & (length(col("EFFECTIVE_DATE")) == 8),
                        to_date(col("EFFECTIVE_DATE"), "MMddyyyy")
                    )
                )

            # Signed numeric parsing for UNIF_ALLOW_AMT
            # Informatica: DECODE(TRUE, IS_NUMBER(v) AND sign='+', TO_DECIMAL(v,2),
            #              IS_NUMBER(v) AND sign='-', TO_DECIMAL(v,2)*-1, ...)
            if "UNIF_ALLOW_AMT" in result_df.columns:
                sign_col = substring(col("UNIF_ALLOW_AMT"), 6, 1)
                amt_str = concat(
                    substring(col("UNIF_ALLOW_AMT"), 1, 3),
                    lit("."),
                    substring(col("UNIF_ALLOW_AMT"), 4, 2)
                )
                result_df = result_df.withColumn(
                    "UNIF_ALLOW_AMT_CONVERTED",
                    when(sign_col == "+", amt_str.cast("decimal(10,2)"))
                    .when(sign_col == "-", (amt_str.cast("decimal(10,2)") * -1))
                    .otherwise(amt_str.cast("decimal(10,2)"))
                )

            metrics.src_success_rows = result_df.count()
            metrics.mark_succeeded()
            self.job_metrics.add_session(metrics)
            return result_df

        except Exception as e:
            metrics.mark_failed(error_code=-1, error_msg=str(e))
            self.job_metrics.add_session(metrics)
            raise

    def _write_target(self, df: DataFrame) -> None:
        """Write converted data to PSEUDOSSN_TBL."""
        metrics = SessionMetrics(
            session_name="s_Pseudossn_Write_Target",
            mapping_name=MAPPING_NAME
        )
        metrics.mark_started()

        try:
            # Add metadata columns (replaces exp_Final)
            output_df = df.withColumn(
                "RUN_DATE", lit(self.session_start_time)
            ).withColumn(
                "PROCESS_NAME", lit(MAPPING_NAME)
            )

            self.db.write_jdbc(output_df, "PSEUDOSSN_TBL", mode="append")
            metrics.tgt_success_rows = output_df.count()
            metrics.mark_succeeded()
            logger.info(f"Wrote {metrics.tgt_success_rows} rows to PSEUDOSSN_TBL")

        except Exception as e:
            metrics.mark_failed(error_code=-1, error_msg=str(e))
            raise
        finally:
            self.job_metrics.add_session(metrics)

    def _update_tk_num(self) -> None:
        """Execute upd_Update_TK_NUM Update Strategy (DD_UPDATE).
        
        Replaces Informatica Update Strategy transformation with DD_UPDATE flag.
        Uses JDBC UPDATE via INFO_TARGET connection.
        """
        metrics = SessionMetrics(
            session_name="s_Pseudossn_Update_TK_NUM",
            mapping_name=MAPPING_NAME
        )
        metrics.mark_started()

        try:
            # Read the TK_NUM value from the flat file or computed value
            # and update PSEUDOSSN_TBL accordingly
            self.db.execute_sql(
                "UPDATE PSEUDOSSN_TBL SET TK_NUM = "
                "(SELECT MAX(TK_NUM) FROM PSEUDOSSN_TBL) + 1 "
                "WHERE TK_NUM IS NULL",
                connection="target"
            )
            metrics.mark_succeeded()
            logger.info("Updated TK_NUM values")

        except Exception as e:
            metrics.mark_failed(error_code=-1, error_msg=str(e))
            raise
        finally:
            self.job_metrics.add_session(metrics)
