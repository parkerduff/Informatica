"""SQL Server connectivity helpers (JDBC for Spark, pyodbc for DML)."""
import contextlib
import logging
from typing import Iterator, List, Optional

logger = logging.getLogger(__name__)


def get_jdbc_url(config: dict) -> str:
    db = config["database"]
    return (
        f"jdbc:sqlserver://{db['host']}:{db['port']};"
        f"databaseName={db['name']};encrypt=true;trustServerCertificate=true"
    )


def get_jdbc_properties(config: dict, secret: dict) -> dict:
    return {
        "user": secret["username"],
        "password": secret["password"],
        "driver": config["database"]["driver"],
    }


def _odbc_connstr(secret: dict, config: dict, database: Optional[str] = None) -> str:
    db = config["database"]
    return (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER={db['host']},{db['port']};"
        f"DATABASE={database or db['name']};"
        f"UID={secret['username']};PWD={secret['password']};"
        "TrustServerCertificate=yes;"
    )


@contextlib.contextmanager
def pyodbc_connection(secret: dict, config: dict, database: Optional[str] = None,
                      autocommit: bool = True) -> Iterator:
    import pyodbc

    conn = pyodbc.connect(_odbc_connstr(secret, config, database), autocommit=autocommit)
    try:
        yield conn
    finally:
        conn.close()


def execute_sql(conn, sql: str, params=None):
    cursor = conn.cursor()
    try:
        logger.debug("Executing SQL: %s", sql.strip().splitlines()[0][:200])
        if params:
            cursor.execute(sql, params)
        else:
            cursor.execute(sql)
        return cursor
    except Exception:
        logger.exception("SQL execution failed: %s", sql[:500])
        raise


def get_current_pay_period(spark, config: dict, secret: dict) -> dict:
    df = read_table(spark, "PAY_PERIOD", config, secret)
    rows = df.filter(df.CURR_PP_FLAG == "Y").collect()
    if len(rows) != 1:
        raise ValueError(f"Expected exactly 1 current pay period, found {len(rows)}")
    row = rows[0].asDict()
    return {
        "pp_num": int(row["PP_NUM"]),
        "pp_end_year": int(row["PP_END_YEAR"]),
        "pp_start_dte": row.get("PP_START_DTE"),
        "pp_end_dte": row.get("PP_END_DTE"),
        "pay_dte": row.get("PAY_DTE"),
    }


def read_table(spark, table: str, config: dict, secret: dict,
               columns: Optional[List[str]] = None):
    schema = config["database"].get("schema", "dbo")
    df = (
        spark.read.format("jdbc")
        .option("url", get_jdbc_url(config))
        .option("dbtable", f"{schema}.{table}")
        .options(**get_jdbc_properties(config, secret))
        .load()
    )
    if columns:
        df = df.select(*columns)
    return df


def write_table(df, table: str, config: dict, secret: dict, mode: str = "append"):
    schema = config["database"].get("schema", "dbo")
    (
        df.write.format("jdbc")
        .option("url", get_jdbc_url(config))
        .option("dbtable", f"{schema}.{table}")
        .option("truncate", "true")
        .options(**get_jdbc_properties(config, secret))
        .mode(mode)
        .save()
    )
