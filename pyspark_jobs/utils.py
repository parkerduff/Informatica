"""
Shared utilities for PySpark ETL jobs migrated from Informatica PowerCenter.

Provides:
- Oracle JDBC connection helpers
- Pay period lookup (used by every job)
- Error logging to ERROR_TBL
- Counter logging to COUNTER_TBL
- Performance metrics capture to MIGRATION_PERF_LOG
- Email notifications via smtplib
- Environment detection (Dev/Test/Prod prefix)
- Parameter file parsing
- Reject file writing
"""

import csv
import logging
import os
import smtplib
import time
from datetime import datetime
from email.mime.text import MIMEText
from typing import Any, Dict, List, Optional

from pyspark.sql import DataFrame, Row, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DecimalType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger("pyspark_etl")

# ---------------------------------------------------------------------------
# Environment detection
# ---------------------------------------------------------------------------
ENV_PREFIX: str = os.environ.get("ENV_PREFIX", "Prod: ")

# ---------------------------------------------------------------------------
# Oracle JDBC configuration
# ---------------------------------------------------------------------------
ORACLE_JDBC_DRIVER = "oracle.jdbc.OracleDriver"


def get_jdbc_url(db_name: str = "ORA_BIIS") -> str:
    """Return the JDBC URL for the given Oracle database name.

    The URL is read from the environment variable ``ORACLE_JDBC_URL_<db_name>``
    (e.g. ``ORACLE_JDBC_URL_ORA_BIIS``).  Falls back to a generic TNS-style
    URL when the variable is not set.
    """
    env_key = f"ORACLE_JDBC_URL_{db_name}"
    return os.environ.get(
        env_key,
        f"jdbc:oracle:thin:@//<host>:1521/{db_name}",
    )


def get_jdbc_properties(connection_name: str = "INFO_TARGET") -> Dict[str, str]:
    """Build JDBC connection properties from environment variables.

    Credentials are read from:
        ``ORACLE_USER_<connection_name>`` / ``ORACLE_PASSWORD_<connection_name>``

    Legacy credential files (``/home/sa-biisint/.use``, ``.pw``, etc.) are
    supported as a fallback when environment variables are absent.
    """
    user_env = f"ORACLE_USER_{connection_name}"
    pw_env = f"ORACLE_PASSWORD_{connection_name}"

    user = os.environ.get(user_env, "")
    password = os.environ.get(pw_env, "")

    # Fallback: legacy credential files
    if not user:
        for path in ["/home/sa-biisint/.use", "/home/sa-biisint/.use1"]:
            if os.path.isfile(path):
                with open(path, "r") as fh:
                    user = fh.read().strip()
                    break
    if not password:
        for path in ["/home/sa-biisint/.pw", "/home/sa-biisint/.pw1"]:
            if os.path.isfile(path):
                with open(path, "r") as fh:
                    password = fh.read().strip()
                    break

    return {
        "user": user,
        "password": password,
        "driver": ORACLE_JDBC_DRIVER,
    }


def get_spark_session(app_name: str, **extra_conf: str) -> SparkSession:
    """Create or retrieve a SparkSession with sensible defaults."""
    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.jars", os.environ.get("ORACLE_JDBC_JAR", "ojdbc8.jar"))
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
    )
    for key, value in extra_conf.items():
        builder = builder.config(key, value)
    return builder.getOrCreate()


# ---------------------------------------------------------------------------
# JDBC read / write helpers
# ---------------------------------------------------------------------------

def read_oracle_table(
    spark: SparkSession,
    table: str,
    jdbc_url: Optional[str] = None,
    connection_name: str = "INFO_TARGET",
    predicate: Optional[str] = None,
) -> DataFrame:
    """Read an Oracle table via JDBC and optionally apply a push-down filter."""
    url = jdbc_url or get_jdbc_url()
    props = get_jdbc_properties(connection_name)
    reader = spark.read.format("jdbc").options(
        url=url,
        dbtable=table,
        **props,
    )
    df = reader.load()
    if predicate:
        df = df.filter(predicate)
    return df


def write_oracle_table(
    df: DataFrame,
    table: str,
    mode: str = "append",
    jdbc_url: Optional[str] = None,
    connection_name: str = "INFO_TARGET",
) -> None:
    """Write a DataFrame to an Oracle table via JDBC."""
    url = jdbc_url or get_jdbc_url()
    props = get_jdbc_properties(connection_name)
    df.write.jdbc(url=url, table=table, mode=mode, properties=props)


def execute_jdbc_statement(
    sql: str,
    jdbc_url: Optional[str] = None,
    connection_name: str = "INFO_TARGET",
) -> None:
    """Execute a raw SQL statement against Oracle via JDBC (DDL / DML).

    Uses the ``jaydebeapi`` library when available, otherwise falls back to
    ``oracledb`` / ``cx_Oracle``.
    """
    url = jdbc_url or get_jdbc_url()
    props = get_jdbc_properties(connection_name)

    try:
        import oracledb  # noqa: F811

        dsn = url.replace("jdbc:oracle:thin:@//", "")
        with oracledb.connect(
            user=props["user"], password=props["password"], dsn=dsn
        ) as conn:
            with conn.cursor() as cur:
                cur.execute(sql)
            conn.commit()
    except ImportError:
        try:
            import jaydebeapi

            conn = jaydebeapi.connect(
                ORACLE_JDBC_DRIVER,
                url,
                [props["user"], props["password"]],
            )
            cur = conn.cursor()
            cur.execute(sql)
            conn.commit()
            cur.close()
            conn.close()
        except ImportError:
            raise RuntimeError(
                "Neither oracledb nor jaydebeapi is available. "
                "Install one to execute raw JDBC statements."
            )


# ---------------------------------------------------------------------------
# Pay Period lookup  (used by every single job)
# ---------------------------------------------------------------------------

def get_current_pay_period(
    spark: SparkSession,
    jdbc_url: Optional[str] = None,
    connection_name: str = "INFO_TARGET",
) -> Row:
    """Look up the current pay period (``CURR_PP_FLAG = 'Y'``) and return it
    as a ``Row`` with fields ``PP_NUM`` and ``PP_END_YEAR``.

    Raises ``RuntimeError`` if no current pay period is found.
    """
    df = read_oracle_table(
        spark,
        "PAY_PERIOD",
        jdbc_url=jdbc_url,
        connection_name=connection_name,
        predicate="CURR_PP_FLAG = 'Y'",
    )
    row = df.first()
    if row is None:
        raise RuntimeError("No current pay period found (CURR_PP_FLAG = 'Y').")
    return row


# ---------------------------------------------------------------------------
# ERROR_TBL writer
# ---------------------------------------------------------------------------

ERROR_TBL_SCHEMA = StructType(
    [
        StructField("PROCESS_NAME", StringType(), True),
        StructField("ERROR_MESSAGE", StringType(), True),
        StructField("SOURCE_KEY", StringType(), True),
        StructField("ERROR_DATE", TimestampType(), True),
        StructField("PP_END_YEAR", DecimalType(4, 0), True),
        StructField("PP_NUM", DecimalType(2, 0), True),
        StructField("CYCLE_ID", DecimalType(3, 0), True),
        StructField("ERROR_CODE", StringType(), True),
    ]
)


def write_errors(
    spark: SparkSession,
    errors: List[Dict[str, Any]],
    process_name: str,
    pp_end_year: int,
    pp_num: int,
    cycle_id: int = 0,
    jdbc_url: Optional[str] = None,
    connection_name: str = "INFO_TARGET",
) -> None:
    """Write error rows to ``ERROR_TBL``.

    Each element of *errors* must be a dict with at least
    ``ERROR_MESSAGE`` and ``SOURCE_KEY``.
    """
    if not errors:
        return
    now = datetime.now()
    rows = []
    for err in errors:
        rows.append(
            Row(
                PROCESS_NAME=process_name,
                ERROR_MESSAGE=str(err.get("ERROR_MESSAGE", ""))[:200],
                SOURCE_KEY=str(err.get("SOURCE_KEY", ""))[:50],
                ERROR_DATE=now,
                PP_END_YEAR=pp_end_year,
                PP_NUM=pp_num,
                CYCLE_ID=cycle_id,
                ERROR_CODE=str(err.get("ERROR_CODE", ""))[:50],
            )
        )
    df = spark.createDataFrame(rows, schema=ERROR_TBL_SCHEMA)
    write_oracle_table(df, "ERROR_TBL", jdbc_url=jdbc_url, connection_name=connection_name)
    logger.info("Wrote %d error(s) to ERROR_TBL for process %s", len(rows), process_name)


def write_error_df(
    df: DataFrame,
    process_name: str,
    pp_end_year: int,
    pp_num: int,
    cycle_id: int = 0,
    source_key_col: str = "SOURCE_KEY",
    error_message: str = "Validation error",
    jdbc_url: Optional[str] = None,
    connection_name: str = "INFO_TARGET",
) -> int:
    """Write error rows from a DataFrame to ``ERROR_TBL``.

    Returns the count of error rows written.
    """
    now = datetime.now()
    error_df = df.select(
        F.lit(process_name).cast(StringType()).alias("PROCESS_NAME"),
        F.lit(error_message).cast(StringType()).alias("ERROR_MESSAGE"),
        F.col(source_key_col).cast(StringType()).alias("SOURCE_KEY"),
        F.lit(now).cast(TimestampType()).alias("ERROR_DATE"),
        F.lit(pp_end_year).cast(DecimalType(4, 0)).alias("PP_END_YEAR"),
        F.lit(pp_num).cast(DecimalType(2, 0)).alias("PP_NUM"),
        F.lit(cycle_id).cast(DecimalType(3, 0)).alias("CYCLE_ID"),
        F.lit("").cast(StringType()).alias("ERROR_CODE"),
    )
    count = error_df.count()
    if count > 0:
        write_oracle_table(
            error_df, "ERROR_TBL", jdbc_url=jdbc_url, connection_name=connection_name
        )
        logger.info(
            "Wrote %d error row(s) to ERROR_TBL for process %s", count, process_name
        )
    return count


# ---------------------------------------------------------------------------
# COUNTER_TBL writer
# ---------------------------------------------------------------------------

COUNTER_TBL_SCHEMA = StructType(
    [
        StructField("RUN_DATE", TimestampType(), True),
        StructField("PROCESS_NAME", StringType(), True),
        StructField("COUNTER_DESCRIPTION", StringType(), True),
        StructField("COUNTER_VALUE", DecimalType(15, 0), True),
        StructField("PP_END_YEAR", DecimalType(4, 0), True),
        StructField("PP_NUM", DecimalType(2, 0), True),
        StructField("CYCLE_ID", DecimalType(1, 0), True),
    ]
)


def write_counter(
    spark: SparkSession,
    process_name: str,
    counter_description: str,
    counter_value: int,
    pp_end_year: int,
    pp_num: int,
    cycle_id: int = 0,
    run_date: Optional[datetime] = None,
    jdbc_url: Optional[str] = None,
    connection_name: str = "INFO_TARGET",
) -> None:
    """Write a single counter row to ``COUNTER_TBL``."""
    now = run_date or datetime.now()
    row = Row(
        RUN_DATE=now,
        PROCESS_NAME=process_name,
        COUNTER_DESCRIPTION=counter_description[:200],
        COUNTER_VALUE=counter_value,
        PP_END_YEAR=pp_end_year,
        PP_NUM=pp_num,
        CYCLE_ID=cycle_id,
    )
    df = spark.createDataFrame([row], schema=COUNTER_TBL_SCHEMA)
    write_oracle_table(df, "COUNTER_TBL", jdbc_url=jdbc_url, connection_name=connection_name)
    logger.info(
        "Wrote counter '%s' = %d to COUNTER_TBL for process %s",
        counter_description,
        counter_value,
        process_name,
    )


# ---------------------------------------------------------------------------
# Performance metrics capture  (MIGRATION_PERF_LOG)
# ---------------------------------------------------------------------------

PERF_LOG_SCHEMA = StructType(
    [
        StructField("JOB_NAME", StringType(), True),
        StructField("RUN_DATE", TimestampType(), True),
        StructField("START_TIME", TimestampType(), True),
        StructField("END_TIME", TimestampType(), True),
        StructField("DURATION_SECONDS", DecimalType(15, 3), True),
        StructField("SRC_ROWS", DecimalType(15, 0), True),
        StructField("TGT_ROWS", DecimalType(15, 0), True),
        StructField("ERROR_ROWS", DecimalType(15, 0), True),
        StructField("PLATFORM", StringType(), True),
    ]
)


class PerfTimer:
    """Context manager that captures start/end times and duration."""

    def __init__(self, job_name: str) -> None:
        self.job_name = job_name
        self.start_time: Optional[datetime] = None
        self.end_time: Optional[datetime] = None
        self.duration_seconds: float = 0.0
        self._t0: float = 0.0

    def __enter__(self) -> "PerfTimer":
        self.start_time = datetime.now()
        self._t0 = time.time()
        logger.info("Job %s started at %s", self.job_name, self.start_time)
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.end_time = datetime.now()
        self.duration_seconds = time.time() - self._t0
        logger.info(
            "Job %s finished at %s (%.2f s)",
            self.job_name,
            self.end_time,
            self.duration_seconds,
        )

    def log_to_table(
        self,
        spark: SparkSession,
        src_rows: int = 0,
        tgt_rows: int = 0,
        error_rows: int = 0,
        jdbc_url: Optional[str] = None,
        connection_name: str = "INFO_TARGET",
    ) -> None:
        """Persist performance metrics to MIGRATION_PERF_LOG table."""
        row = Row(
            JOB_NAME=self.job_name,
            RUN_DATE=self.start_time,
            START_TIME=self.start_time,
            END_TIME=self.end_time,
            DURATION_SECONDS=round(self.duration_seconds, 3),
            SRC_ROWS=src_rows,
            TGT_ROWS=tgt_rows,
            ERROR_ROWS=error_rows,
            PLATFORM="PySpark",
        )
        df = spark.createDataFrame([row], schema=PERF_LOG_SCHEMA)
        write_oracle_table(
            df, "MIGRATION_PERF_LOG", jdbc_url=jdbc_url, connection_name=connection_name
        )
        logger.info(
            "Performance: %s | src=%d tgt=%d err=%d | %.2fs",
            self.job_name,
            src_rows,
            tgt_rows,
            error_rows,
            self.duration_seconds,
        )

    def log_to_csv(
        self,
        filepath: str,
        src_rows: int = 0,
        tgt_rows: int = 0,
        error_rows: int = 0,
    ) -> None:
        """Append performance metrics to a local CSV file."""
        header = [
            "JOB_NAME",
            "RUN_DATE",
            "START_TIME",
            "END_TIME",
            "DURATION_SECONDS",
            "SRC_ROWS",
            "TGT_ROWS",
            "ERROR_ROWS",
            "PLATFORM",
        ]
        file_exists = os.path.isfile(filepath)
        with open(filepath, "a", newline="") as fh:
            writer = csv.writer(fh)
            if not file_exists:
                writer.writerow(header)
            writer.writerow(
                [
                    self.job_name,
                    self.start_time,
                    self.start_time,
                    self.end_time,
                    round(self.duration_seconds, 3),
                    src_rows,
                    tgt_rows,
                    error_rows,
                    "PySpark",
                ]
            )


# ---------------------------------------------------------------------------
# Email notifications
# ---------------------------------------------------------------------------

SMTP_HOST: str = os.environ.get("SMTP_HOST", "localhost")
SMTP_PORT: int = int(os.environ.get("SMTP_PORT", "25"))
EMAIL_FROM: str = os.environ.get("EMAIL_FROM", "biisint-pyspark@hhs.gov")

# Default recipients from the preload script
DEFAULT_RECIPIENTS: List[str] = [
    "peter.chen@hhs.gov",
    "nathan.knight@hhs.gov",
    "marvin.simon@hhs.gov",
]


def send_email(
    subject: str,
    body: str,
    recipients: Optional[List[str]] = None,
    sender: Optional[str] = None,
) -> None:
    """Send a plain-text email notification.

    Silently logs a warning (instead of raising) when the SMTP server is
    unreachable so that ETL failures don't mask the real error.
    """
    to_addrs = recipients or DEFAULT_RECIPIENTS
    from_addr = sender or EMAIL_FROM

    msg = MIMEText(body, "plain")
    msg["Subject"] = subject
    msg["From"] = from_addr
    msg["To"] = ", ".join(to_addrs)

    try:
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=30) as smtp:
            smtp.sendmail(from_addr, to_addrs, msg.as_string())
        logger.info("Email sent: '%s' -> %s", subject, to_addrs)
    except Exception:
        logger.warning(
            "Failed to send email '%s' to %s. SMTP host=%s:%d",
            subject,
            to_addrs,
            SMTP_HOST,
            SMTP_PORT,
            exc_info=True,
        )


# ---------------------------------------------------------------------------
# Parameter file parsing
# ---------------------------------------------------------------------------

def parse_parameter_file(
    filepath: str = "/data/BIISINT/control/BIIS_parms.iparms",
) -> Dict[str, str]:
    """Parse an Informatica-style parameter file and return a dict.

    Expected format (one per line)::

        $$WF_PP_END_YEAR=2024
        $$WF_PP_NUM=15
    """
    params: Dict[str, str] = {}
    if not os.path.isfile(filepath):
        logger.warning("Parameter file not found: %s", filepath)
        return params
    with open(filepath, "r") as fh:
        for line in fh:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if "=" in line:
                key, _, value = line.partition("=")
                params[key.strip()] = value.strip()
    logger.info("Loaded %d parameter(s) from %s", len(params), filepath)
    return params


def get_param(
    params: Dict[str, str],
    key: str,
    env_key: Optional[str] = None,
    default: str = "",
) -> str:
    """Retrieve a parameter value, checking environment variables first,
    then the parsed parameter dict, then the default.
    """
    env_name = env_key or key.replace("$$", "WF_")
    val = os.environ.get(env_name, "")
    if val:
        return val
    return params.get(key, default)


# ---------------------------------------------------------------------------
# Reject (bad) file writer
# ---------------------------------------------------------------------------

def write_reject_file(
    df: DataFrame,
    filepath: str,
    bad_file_dir: Optional[str] = None,
) -> None:
    """Write rejected / failed rows to a ``.bad`` file as CSV.

    If *bad_file_dir* is provided, the file is placed there; otherwise the
    *filepath* is used as-is.
    """
    if bad_file_dir:
        filepath = os.path.join(bad_file_dir, os.path.basename(filepath))

    os.makedirs(os.path.dirname(filepath) or ".", exist_ok=True)
    pdf = df.toPandas()
    pdf.to_csv(filepath, index=False)
    logger.info("Wrote %d rejected row(s) to %s", len(pdf), filepath)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def format_pp_num(pp_num: int) -> str:
    """Zero-pad a pay-period number to 2 digits (e.g. 3 -> '03')."""
    return str(pp_num).zfill(2)


def safe_is_number(value: Optional[str]) -> bool:
    """Return True if *value* is a numeric string."""
    if value is None:
        return False
    try:
        float(value)
        return True
    except (ValueError, TypeError):
        return False
