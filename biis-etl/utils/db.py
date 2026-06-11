"""Database access layer.

Provides a single API used by every job, script and test, with two backends
selected by :class:`~utils.config.Config`:

* ``sqlite``     -> local file DB (the ``test`` environment).
* ``sqlserver``  -> SQL Server over JDBC (Spark) and pyodbc (raw connection).

Jobs only ever call :func:`read_table` / :func:`write_table` / :func:`execute`
so they are completely agnostic to the backend.
"""
from __future__ import annotations

import os
import sqlite3
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any, Iterable, List, Optional, Sequence

if TYPE_CHECKING:  # pragma: no cover
    from pyspark.sql import DataFrame, SparkSession
    from utils.config import Config

from utils.secrets import get_secret


# --------------------------------------------------------------------------- #
# Raw connections
# --------------------------------------------------------------------------- #
def get_connection(config: "Config"):
    """Return a DB-API connection for raw SQL (DDL, assertions, counts)."""
    if config.backend == "sqlite":
        os.makedirs(os.path.dirname(config.sqlite_path), exist_ok=True)
        conn = sqlite3.connect(config.sqlite_path)
        conn.execute("PRAGMA foreign_keys = ON")
        return conn
    if config.backend == "sqlserver":  # pragma: no cover - requires SQL Server
        import pyodbc

        user = get_secret("db_user", config)
        password = get_secret("db_password", config)
        db = config.database
        dsn = (
            "DRIVER={ODBC Driver 18 for SQL Server};"
            f"SERVER={db['host']},{db['port']};DATABASE={db['name']};"
            f"UID={user};PWD={password};TrustServerCertificate=yes"
        )
        return pyodbc.connect(dsn)
    raise ValueError(f"Unknown backend: {config.backend!r}")


@contextmanager
def connection(config: "Config"):
    conn = get_connection(config)
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #
def execute(config: "Config", sql: str, params: Optional[Sequence[Any]] = None) -> None:
    with connection(config) as conn:
        cur = conn.cursor()
        cur.execute(sql, params or [])


def executemany(config: "Config", sql: str, rows: Iterable[Sequence[Any]]) -> None:
    with connection(config) as conn:
        cur = conn.cursor()
        cur.executemany(sql, list(rows))


def fetch_all(config: "Config", sql: str, params: Optional[Sequence[Any]] = None) -> List[tuple]:
    with connection(config) as conn:
        cur = conn.cursor()
        cur.execute(sql, params or [])
        return list(cur.fetchall())


def count(config: "Config", table: str) -> int:
    rows = fetch_all(config, f"SELECT COUNT(*) FROM {table}")
    return int(rows[0][0])


def truncate(config: "Config", table: str) -> None:
    if config.backend == "sqlite":
        execute(config, f"DELETE FROM {table}")
    else:  # pragma: no cover
        execute(config, f"TRUNCATE TABLE {table}")


def table_columns(config: "Config", table: str) -> List[str]:
    if config.backend == "sqlite":
        rows = fetch_all(config, f"PRAGMA table_info({table})")
        return [r[1] for r in rows]
    # pragma: no cover
    rows = fetch_all(
        config,
        "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS "
        "WHERE TABLE_NAME = ? ORDER BY ORDINAL_POSITION",
        [table],
    )
    return [r[0] for r in rows]


# --------------------------------------------------------------------------- #
# Spark <-> DB
# --------------------------------------------------------------------------- #
def _jdbc_url(config: "Config") -> str:  # pragma: no cover - requires SQL Server
    db = config.database
    return (
        f"jdbc:sqlserver://{db['host']}:{db['port']};databaseName={db['name']};"
        "encrypt=true;trustServerCertificate=true"
    )


def _coerce(value, data_type):
    from pyspark.sql.types import DoubleType, LongType

    if value is None:
        return None
    if isinstance(data_type, LongType):
        return int(value)
    if isinstance(data_type, DoubleType):
        return float(value)
    return str(value)


def read_table(spark: "SparkSession", table: str, config: "Config") -> "DataFrame":
    """Read a full table into a Spark DataFrame using the registry schema."""
    if config.backend == "sqlite":
        from pyspark.sql.types import StringType, StructField, StructType

        from utils.schemas import TABLES, spark_schema

        with connection(config) as conn:
            cur = conn.cursor()
            cur.execute(f"SELECT * FROM {table}")
            db_cols = [d[0] for d in cur.description]
            raw_rows = cur.fetchall()

        if table in TABLES:
            schema = spark_schema(table)
        else:  # pragma: no cover
            schema = StructType([StructField(c, StringType(), True) for c in db_cols])

        idx = [db_cols.index(f.name) for f in schema.fields]
        rows = [
            tuple(_coerce(r[idx[j]], schema.fields[j].dataType) for j in range(len(schema.fields)))
            for r in raw_rows
        ]
        return spark.createDataFrame(rows, schema)
    # pragma: no cover
    user = get_secret("db_user", config)
    password = get_secret("db_password", config)
    return (
        spark.read.format("jdbc")
        .option("url", _jdbc_url(config))
        .option("dbtable", f"{config.database['schema']}.{table}")
        .option("user", user)
        .option("password", password)
        .load()
    )


def write_table(
    spark: "SparkSession",
    df: "DataFrame",
    table: str,
    config: "Config",
    mode: str = "append",
) -> None:
    """Persist a Spark DataFrame to ``table``.

    ``mode`` is ``append`` or ``overwrite``.  ``overwrite`` preserves the
    pre-created DDL (it deletes rows rather than dropping the table).
    """
    if config.backend == "sqlite":
        import pandas as pd

        pdf: "pd.DataFrame" = df.toPandas()
        with connection(config) as conn:
            if mode == "overwrite":
                conn.execute(f"DELETE FROM {table}")
            if not pdf.empty:
                pdf.to_sql(table, conn, if_exists="append", index=False)
        return
    # pragma: no cover
    user = get_secret("db_user", config)
    password = get_secret("db_password", config)
    (
        df.write.format("jdbc")
        .option("url", _jdbc_url(config))
        .option("dbtable", f"{config.database['schema']}.{table}")
        .option("user", user)
        .option("password", password)
        .mode("append" if mode == "append" else "overwrite")
        .option("truncate", "true")
        .save()
    )
