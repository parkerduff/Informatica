"""Oracle access helpers for the BIIS ETL jobs.

Spark-based reads/writes go through JDBC; imperative SQL (PL/SQL procedures, DDL
and DML that cannot be expressed in Spark) goes through the ``oracledb`` driver.
"""
import logging
import re
from typing import List, Optional

import oracledb
from pyspark.sql import DataFrame, SparkSession

from config import connections

logger = logging.getLogger(__name__)


def _table_ref(table: str, schema: Optional[str]) -> str:
    return "%s.%s" % (schema, table) if schema else table


def read_oracle_table(spark: SparkSession, table: str, schema: Optional[str] = None,
                      where_clause: Optional[str] = None) -> DataFrame:
    """Read an Oracle table via JDBC into a Spark DataFrame.

    When ``where_clause`` is supplied the read is pushed down as a subquery.
    """
    ref = _table_ref(table, schema)
    if where_clause:
        dbtable = "(SELECT * FROM %s WHERE %s) t" % (ref, where_clause)
    else:
        dbtable = ref
    logger.info("Reading Oracle table %s", dbtable)
    return (
        spark.read.format("jdbc")
        .option("url", connections.ORACLE_JDBC_URL)
        .option("dbtable", dbtable)
        .option("user", connections.ORACLE_USER)
        .option("password", connections.ORACLE_PASSWORD)
        .option("driver", connections.ORACLE_JDBC_DRIVER)
        .load()
    )


def write_to_oracle(df: DataFrame, table: str, schema: Optional[str] = None,
                    mode: str = "append") -> None:
    """Write a DataFrame to an Oracle table via JDBC."""
    ref = _table_ref(table, schema)
    logger.info("Writing DataFrame to Oracle table %s (mode=%s)", ref, mode)
    (
        df.write.format("jdbc")
        .option("url", connections.ORACLE_JDBC_URL)
        .option("dbtable", ref)
        .option("user", connections.ORACLE_USER)
        .option("password", connections.ORACLE_PASSWORD)
        .option("driver", connections.ORACLE_JDBC_DRIVER)
        .mode(mode)
        .save()
    )


def _connect() -> "oracledb.Connection":
    return oracledb.connect(
        user=connections.ORACLE_USER,
        password=connections.ORACLE_PASSWORD,
        dsn=connections.ORACLE_DSN,
    )


def execute_sql(statements: List[str], connection_name: str = "default") -> None:
    """Execute SQL statements sequentially via an ``oracledb`` connection.

    Used for PL/SQL procedure calls, DDL and DML that cannot be expressed in
    Spark. ``connection_name`` is accepted for API symmetry / future routing.
    """
    conn = _connect()
    try:
        cursor = conn.cursor()
        for stmt in statements:
            stmt = stmt.strip()
            if not stmt:
                continue
            logger.info("Executing SQL: %s", stmt.splitlines()[0][:120])
            cursor.execute(stmt)
        conn.commit()
    finally:
        conn.close()


# SQL*Plus directive lines that oracledb cannot execute. These are only treated
# as directives at a statement boundary (empty buffer); the same keywords inside
# a statement -- e.g. the SET clause of an UPDATE -- are left untouched.
_DIRECTIVE = re.compile(
    r"^(SPOOL|SET|COL|COLUMN|PROMPT|REM|REMARK|WHENEVER|@@?)\b", re.IGNORECASE
)
# SQL*Plus EXEC / EXECUTE is shorthand for an anonymous PL/SQL block.
_EXEC = re.compile(r"^EXEC(UTE)?\s+(?P<call>.+)$", re.IGNORECASE | re.DOTALL)


def _finalize(block: str, statements: List[str]) -> None:
    block = block.strip().rstrip(";").strip()
    if not block:
        return
    m = _EXEC.match(block)
    if m:
        # oracledb can't run "EXEC proc"; rewrite to "BEGIN proc; END;".
        statements.append("BEGIN %s; END;" % m.group("call").strip())
    else:
        statements.append(block)


def _split_sql_script(script: str) -> List[str]:
    """Split a SQL script into individual executable statements.

    PL/SQL blocks are terminated by a line containing only ``/``; plain SQL
    statements are separated by semicolons. SQL*Plus-only directives (SPOOL, SET,
    PROMPT, ...) are dropped, and ``EXEC``/``EXECUTE`` shorthands are rewritten as
    anonymous PL/SQL blocks so oracledb can run them.
    """
    statements: List[str] = []
    buffer: List[str] = []
    for line in script.splitlines():
        stripped = line.strip()
        # Only skip directives when we're between statements; inside a statement
        # the same keyword (e.g. UPDATE ... SET) is a real part of the SQL.
        if not buffer and (not stripped or _DIRECTIVE.match(stripped)):
            continue
        if stripped == "/":
            _finalize("\n".join(buffer), statements)
            buffer = []
            continue
        buffer.append(line)
        if stripped.endswith(";"):
            _finalize("\n".join(buffer), statements)
            buffer = []
    _finalize("\n".join(buffer), statements)
    return statements


def execute_sql_file(file_path: str, connection_name: str = "default") -> None:
    """Execute a SQL script file, splitting on semicolons and slashes."""
    with open(file_path, "r") as fh:
        script = fh.read()
    statements = _split_sql_script(script)
    logger.info("Executing %d statements from %s", len(statements), file_path)
    execute_sql(statements, connection_name=connection_name)
