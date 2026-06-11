"""Database access helpers for the BIIS ETL migration.

Wraps pyodbc (direct SQL / DDL / update strategy) and PySpark JDBC reads/writes
against the Dockerised SQL Server defined in ``docker-compose.yml``.
"""
from __future__ import annotations

from typing import Any, Dict, List

from utils import config as cfg


def get_pyodbc_connection(config: Dict[str, Any], database: str | None = None):
    """Return a pyodbc connection to SQL Server.

    ``database='master'`` is used while creating the application database.
    """
    import pyodbc

    if database == "master":
        conn_str = cfg.get_master_connection_string(config)
    else:
        conn_str = cfg.get_pyodbc_connection_string(config)
    conn = pyodbc.connect(conn_str, autocommit=True)
    return conn


def get_jdbc_url(config: Dict[str, Any]) -> str:
    return cfg.get_jdbc_url(config)


def get_jdbc_properties(config: Dict[str, Any]) -> Dict[str, str]:
    return cfg.get_jdbc_properties(config)


def execute_sql(config: Dict[str, Any], sql: str, database: str | None = None) -> None:
    """Execute one or more (GO/semicolon separated) SQL statements via pyodbc."""
    conn = get_pyodbc_connection(config, database=database)
    try:
        cursor = conn.cursor()
        for statement in _split_sql(sql):
            stmt = statement.strip()
            if stmt:
                cursor.execute(stmt)
        cursor.close()
    finally:
        conn.close()


def execute_scalar(config: Dict[str, Any], sql: str) -> Any:
    """Execute a query and return the first column of the first row."""
    conn = get_pyodbc_connection(config)
    try:
        cursor = conn.cursor()
        cursor.execute(sql)
        row = cursor.fetchone()
        return row[0] if row else None
    finally:
        conn.close()


def fetch_all(config: Dict[str, Any], sql: str) -> List[tuple]:
    conn = get_pyodbc_connection(config)
    try:
        cursor = conn.cursor()
        cursor.execute(sql)
        columns = [c[0] for c in cursor.description]
        rows = [tuple(r) for r in cursor.fetchall()]
        return columns, rows
    finally:
        conn.close()


def spark_df_from_query(spark, config: Dict[str, Any], sql: str, columns: List[str] | None = None):
    """Run a query via pyodbc and materialise the result as a Spark DataFrame.

    This keeps the heavy lifting (filters, expressions, aggregations) in Spark
    while avoiding a hard dependency on the SQL Server JDBC driver jar for I/O.
    """
    conn = get_pyodbc_connection(config)
    try:
        cursor = conn.cursor()
        cursor.execute(sql)
        col_names = columns or [c[0] for c in cursor.description]
        rows = [tuple(r) for r in cursor.fetchall()]
    finally:
        conn.close()
    if not rows:
        from pyspark.sql.types import StringType, StructField, StructType

        schema = StructType([StructField(c, StringType(), True) for c in col_names])
        return spark.createDataFrame([], schema)
    return spark.createDataFrame(rows, schema=list(col_names))


def read_table(spark, config: Dict[str, Any], table_name: str):
    """Read a SQL Server table into a Spark DataFrame via JDBC."""
    return (
        spark.read.format("jdbc")
        .option("url", get_jdbc_url(config))
        .option("dbtable", table_name)
        .option("user", get_jdbc_properties(config)["user"])
        .option("password", get_jdbc_properties(config)["password"])
        .option("driver", get_jdbc_properties(config)["driver"])
        .load()
    )


def write_table(spark_df, config: Dict[str, Any], table_name: str, mode: str = "append") -> None:
    """Write a Spark DataFrame to a SQL Server table via JDBC."""
    (
        spark_df.write.format("jdbc")
        .option("url", get_jdbc_url(config))
        .option("dbtable", table_name)
        .option("user", get_jdbc_properties(config)["user"])
        .option("password", get_jdbc_properties(config)["password"])
        .option("driver", get_jdbc_properties(config)["driver"])
        .mode(mode)
        .save()
    )


def _split_sql(sql: str) -> List[str]:
    """Split a SQL script on ``GO`` batch separators (and fall back to ``;``)."""
    lines = sql.splitlines()
    batches: List[str] = []
    current: List[str] = []
    for line in lines:
        if line.strip().upper() == "GO":
            batches.append("\n".join(current))
            current = []
        else:
            current.append(line)
    if current:
        batches.append("\n".join(current))

    statements: List[str] = []
    for batch in batches:
        if "GO" in sql.upper().split("\n"):
            statements.append(batch)
        else:
            statements.extend(batch.split(";"))
    return [s for s in statements if s.strip()]
