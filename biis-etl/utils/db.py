"""SQL Server connectivity helpers for both Spark (JDBC) and pyodbc paths."""
from __future__ import annotations

import contextlib
import logging
from typing import Any, Dict, Iterator, List, Optional, Sequence

from utils.secrets import Config, DbSecret, get_db_secret

logger = logging.getLogger(__name__)


def get_jdbc_url(config: Config) -> str:
    db = config.database
    return (
        f"jdbc:sqlserver://{db.host}:{db.port};"
        f"databaseName={db.name};encrypt=true;trustServerCertificate=true"
    )


def get_jdbc_properties(config: Config, secret: DbSecret) -> Dict[str, str]:
    return {
        "user": secret.user,
        "password": secret.password,
        "driver": config.database.driver,
    }


def get_odbc_connstr(config: Config, secret: DbSecret) -> str:
    db = config.database
    return (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER={db.host},{db.port};DATABASE={db.name};"
        f"UID={secret.user};PWD={secret.password};"
        "Encrypt=yes;TrustServerCertificate=yes"
    )


@contextlib.contextmanager
def pyodbc_connection(secret: DbSecret, config: Config, autocommit: bool = True) -> Iterator[Any]:
    """Context manager yielding a pyodbc connection to SQL Server."""
    import pyodbc  # lazy import; not needed for pure-Spark jobs

    conn = pyodbc.connect(get_odbc_connstr(config, secret), autocommit=autocommit)
    try:
        yield conn
    finally:
        conn.close()


def execute_sql(conn: Any, sql: str, params: Optional[Sequence[Any]] = None) -> Any:
    """Execute a single SQL statement with logging and error propagation."""
    cur = conn.cursor()
    try:
        if params is not None:
            cur.execute(sql, params)
        else:
            cur.execute(sql)
        return cur
    except Exception:
        logger.exception("SQL failed: %s", sql.strip().splitlines()[0] if sql.strip() else sql)
        raise


def read_table(
    spark: Any,
    table: str,
    config: Config,
    secret: Optional[DbSecret] = None,
    columns: Optional[List[str]] = None,
) -> Any:
    """Read a SQL Server table via JDBC with explicit column projection."""
    secret = secret or get_db_secret(config)
    schema = config.database.schema
    if columns:
        col_list = ", ".join(columns)
        dbtable = f"(SELECT {col_list} FROM {schema}.{table}) AS subq"
    else:
        dbtable = f"{schema}.{table}"
    return (
        spark.read.format("jdbc")
        .option("url", get_jdbc_url(config))
        .option("dbtable", dbtable)
        .options(**get_jdbc_properties(config, secret))
        .load()
    )


def write_table(
    df: Any,
    table: str,
    config: Config,
    secret: Optional[DbSecret] = None,
    mode: str = "append",
) -> None:
    """Write a DataFrame to a SQL Server table via JDBC."""
    secret = secret or get_db_secret(config)
    schema = config.database.schema
    (
        df.write.format("jdbc")
        .option("url", get_jdbc_url(config))
        .option("dbtable", f"{schema}.{table}")
        .options(**get_jdbc_properties(config, secret))
        .mode(mode)
        .save()
    )
    logger.info("Wrote %d rows to %s.%s (mode=%s)", df.count(), schema, table, mode)


def get_current_pay_period(
    spark: Any, config: Config, secret: Optional[DbSecret] = None
) -> Dict[str, Any]:
    """Return the current pay period (CURR_PP_FLAG = 'Y') as a dict.

    Mirrors the Informatica lookup used across COMPTIME / CPM / FDA workflows.
    """
    secret = secret or get_db_secret(config)
    df = read_table(
        spark, "PAY_PERIOD", config, secret,
        columns=["PP_NUM", "PP_END_YEAR", "PP_START_DTE", "PP_END_DTE",
                 "LV_NUM", "LV_YEAR", "PAY_DTE", "CURR_PP_FLAG"],
    ).filter("CURR_PP_FLAG = 'Y'")
    rows = df.collect()
    if len(rows) != 1:
        raise ValueError(
            f"Expected exactly 1 current pay period (CURR_PP_FLAG='Y'), found {len(rows)}"
        )
    r = rows[0]
    pp_num = int(r["PP_NUM"])
    pp_end_year = int(r["PP_END_YEAR"])
    return {
        "pp_num": pp_num,
        "pp_end_year": pp_end_year,
        "pp_start_dte": r["PP_START_DTE"],
        "pp_end_dte": r["PP_END_DTE"],
        "lv_num": r["LV_NUM"],
        "lv_year": r["LV_YEAR"],
        "pay_dte": r["PAY_DTE"],
        # PP_YEAR_NUM = year concatenated with zero-padded pay period number
        "pp_year_num": int(f"{pp_end_year}{pp_num:02d}"),
    }
