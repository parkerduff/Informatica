"""
Database connection helpers for the PySpark migration.

Replaces Informatica PowerCenter database connections:
  - ORA_BIISPRD_SRC : Source HR data (EHRP, PeopleSoft tables)
  - ORA_BIIS        : Target BIIS Oracle data warehouse
  - INFO_NATE       : Secondary source for PS_JPM_JP_ITEMS lookup

Credentials are read from environment variables instead of the legacy
file-based approach ($HOME/.use, $HOME/.pw, $HOME/.use1, $HOME/.pw1).
"""

import os
import logging

import cx_Oracle

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Environment-variable names for each connection
# ---------------------------------------------------------------------------
# Source connection (ORA_BIISPRD_SRC)
_SRC_USER_ENV = "BIISPRD_SRC_USER"
_SRC_PASS_ENV = "BIISPRD_SRC_PASSWORD"
_SRC_DSN_ENV = "BIISPRD_SRC_DSN"

# Target connection (ORA_BIIS)
_TGT_USER_ENV = "BIIS_USER"
_TGT_PASS_ENV = "BIIS_PASSWORD"
_TGT_DSN_ENV = "BIIS_DSN"

# Secondary source connection (INFO_NATE)
_NATE_USER_ENV = "INFO_NATE_USER"
_NATE_PASS_ENV = "INFO_NATE_PASSWORD"
_NATE_DSN_ENV = "INFO_NATE_DSN"

# Default JDBC batch size matching PowerCenter commit interval
DEFAULT_BATCH_SIZE = 10000


# ---------------------------------------------------------------------------
# JDBC helpers (used by PySpark jobs)
# ---------------------------------------------------------------------------

def get_src_jdbc_url() -> str:
    """Return the JDBC URL for ORA_BIISPRD_SRC."""
    dsn = os.environ[_SRC_DSN_ENV]
    return f"jdbc:oracle:thin:@{dsn}"


def get_tgt_jdbc_url() -> str:
    """Return the JDBC URL for ORA_BIIS."""
    dsn = os.environ[_TGT_DSN_ENV]
    return f"jdbc:oracle:thin:@{dsn}"


def get_nate_jdbc_url() -> str:
    """Return the JDBC URL for INFO_NATE."""
    dsn = os.environ[_NATE_DSN_ENV]
    return f"jdbc:oracle:thin:@{dsn}"


def get_src_jdbc_properties() -> dict:
    """Return JDBC connection properties for the source database."""
    return {
        "user": os.environ[_SRC_USER_ENV],
        "password": os.environ[_SRC_PASS_ENV],
        "driver": "oracle.jdbc.OracleDriver",
        "batchsize": str(DEFAULT_BATCH_SIZE),
    }


def get_tgt_jdbc_properties() -> dict:
    """Return JDBC connection properties for the target database."""
    return {
        "user": os.environ[_TGT_USER_ENV],
        "password": os.environ[_TGT_PASS_ENV],
        "driver": "oracle.jdbc.OracleDriver",
        "batchsize": str(DEFAULT_BATCH_SIZE),
    }


def get_nate_jdbc_properties() -> dict:
    """Return JDBC connection properties for the INFO_NATE database."""
    return {
        "user": os.environ[_NATE_USER_ENV],
        "password": os.environ[_NATE_PASS_ENV],
        "driver": "oracle.jdbc.OracleDriver",
        "batchsize": str(DEFAULT_BATCH_SIZE),
    }


# ---------------------------------------------------------------------------
# cx_Oracle helpers (used by pre/post-load Python steps)
# ---------------------------------------------------------------------------

def get_src_cx_connection() -> cx_Oracle.Connection:
    """Return a cx_Oracle connection to ORA_BIISPRD_SRC."""
    return cx_Oracle.connect(
        user=os.environ[_SRC_USER_ENV],
        password=os.environ[_SRC_PASS_ENV],
        dsn=os.environ[_SRC_DSN_ENV],
    )


def get_tgt_cx_connection() -> cx_Oracle.Connection:
    """Return a cx_Oracle connection to ORA_BIIS."""
    return cx_Oracle.connect(
        user=os.environ[_TGT_USER_ENV],
        password=os.environ[_TGT_PASS_ENV],
        dsn=os.environ[_TGT_DSN_ENV],
    )


def execute_sql(conn: cx_Oracle.Connection, sql: str) -> None:
    """Execute a single SQL statement and commit."""
    cursor = conn.cursor()
    try:
        logger.info("Executing SQL: %s", sql[:200])
        cursor.execute(sql)
        conn.commit()
        logger.info("SQL executed successfully, rows affected: %s", cursor.rowcount)
    finally:
        cursor.close()


def call_procedure(conn: cx_Oracle.Connection, proc_name: str, args: list = None) -> None:
    """Call an Oracle stored procedure by name."""
    cursor = conn.cursor()
    try:
        logger.info("Calling stored procedure: %s", proc_name)
        cursor.callproc(proc_name, args or [])
        conn.commit()
        logger.info("Stored procedure %s completed successfully", proc_name)
    finally:
        cursor.close()


def execute_query(conn: cx_Oracle.Connection, sql: str) -> list:
    """Execute a SELECT query and return all rows."""
    cursor = conn.cursor()
    try:
        cursor.execute(sql)
        return cursor.fetchall()
    finally:
        cursor.close()
