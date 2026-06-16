"""Oracle access helpers for both Spark JDBC and raw SQL (oracledb).

Two distinct needs:

* Spark DataFrame reads/writes use the JDBC datasource -- :func:`get_spark_jdbc_options`
  returns the options dict (URL, driver, user, password) for a logical connection.
* Multi-statement SQL / PL-SQL scripts (preload step01, afterload procedures)
  cannot be expressed as DataFrames, so :func:`execute_sql_script` runs them
  statement-by-statement through a regular ``oracledb`` cursor.
"""
from __future__ import annotations

import logging
import re
from typing import Dict, List, Optional

from pyspark_etl.config.connections import OracleConnection, get_connection

logger = logging.getLogger(__name__)


def get_spark_jdbc_options(connection_name: str, query: Optional[str] = None,
                           dbtable: Optional[str] = None) -> Dict[str, str]:
    """Return a ``spark.read.format('jdbc').options(**opts)`` dict.

    Pass either ``dbtable`` (``SCHEMA.TABLE``) or ``query`` (a SELECT); for
    writes, set ``dbtable`` on the writer instead.
    """
    conn: OracleConnection = get_connection(connection_name)
    opts = {
        "url": conn.jdbc_url,
        "user": conn.user,
        "password": conn.password,
        "driver": conn.driver,
        "fetchsize": "10000",
    }
    if dbtable:
        opts["dbtable"] = dbtable
    if query:
        opts["query"] = query
    return opts


def read_table(spark, connection_name: str, schema: str, table: str,
               columns: Optional[List[str]] = None):
    """Read an Oracle table (optionally projecting ``columns``) into a DataFrame."""
    col_list = ", ".join(columns) if columns else "*"
    query = f"SELECT {col_list} FROM {schema}.{table}"
    opts = get_spark_jdbc_options(connection_name, query=query)
    return spark.read.format("jdbc").options(**opts).load()


def write_table(df, connection_name: str, schema: str, table: str,
                mode: str = "append") -> None:
    """Write a DataFrame to an Oracle table via JDBC."""
    conn = get_connection(connection_name)
    (df.write.format("jdbc")
        .option("url", conn.jdbc_url)
        .option("user", conn.user)
        .option("password", conn.password)
        .option("driver", conn.driver)
        .option("dbtable", f"{schema}.{table}")
        .option("batchsize", "5000")
        .mode(mode)
        .save())
    logger.info("Wrote DataFrame to %s.%s (mode=%s)", schema, table, mode)


_SQLPLUS_DIRECTIVE = re.compile(
    r"^\s*(SPOOL|SET|PROMPT|COL|EXIT|QUIT|SHOW|WHENEVER|CONNECT|@)\b",
    re.IGNORECASE,
)
_EXEC_SHORTHAND = re.compile(r"^\s*EXEC(?:UTE)?\s+(?P<call>.+?);?\s*$", re.IGNORECASE)
_BLOCK_START = re.compile(r"^\s*(BEGIN|DECLARE)\b", re.IGNORECASE)


def _finalize(buffer: List[str], statements: List[str]) -> None:
    stmt = "\n".join(buffer).strip().rstrip(";").strip()
    if stmt:
        statements.append(stmt)


def _split_sql_statements(script: str) -> List[str]:
    """Split a SQL*Plus-style script into oracledb-executable statements.

    * Strips SQL*Plus-only directives (SPOOL, SET, PROMPT, COL, EXIT, ...).
    * Converts ``EXEC proc[(args)];`` shorthand into a ``BEGIN proc; END;`` block.
    * Treats a lone ``/`` as a statement terminator and keeps multi-line
      ``BEGIN``/``DECLARE`` PL/SQL blocks intact (``;`` inside them does not split).
    """
    statements: List[str] = []
    buffer: List[str] = []
    in_block = False

    for line in script.splitlines():
        if not in_block and (
            _SQLPLUS_DIRECTIVE.match(line)
            or not line.strip()
            or (not buffer and line.lstrip().startswith("--"))
        ):
            continue

        if line.strip() == "/":
            _finalize(buffer, statements)
            buffer = []
            in_block = False
            continue

        exec_match = _EXEC_SHORTHAND.match(line)
        if exec_match and not buffer:
            statements.append(f"BEGIN {exec_match.group('call').strip()}; END;")
            continue

        if not buffer and _BLOCK_START.match(line):
            in_block = True

        buffer.append(line)

        if in_block:
            if re.search(r"\bEND\s*;\s*$", line, re.IGNORECASE):
                block = "\n".join(buffer).strip()
                if block:
                    statements.append(block)
                buffer = []
                in_block = False
        elif line.rstrip().endswith(";"):
            _finalize(buffer, statements)
            buffer = []

    _finalize(buffer, statements)
    return statements


def execute_statements(connection_name: str, statements: List[str]) -> List[str]:
    """Execute a list of SQL/PL-SQL statements in one transaction.

    Returns the executed statements (for logging). Rolls back and re-raises on
    the first failure so partial loads are not committed.
    """
    import oracledb  # imported lazily; not needed for Spark-only jobs

    conn = get_connection(connection_name)
    executed: List[str] = []
    db = oracledb.connect(user=conn.user, password=conn.password, dsn=conn.dsn())
    try:
        cur = db.cursor()
        for stmt in statements:
            logger.info("Executing: %s", stmt.splitlines()[0][:120])
            cur.execute(stmt)
            executed.append(stmt)
        db.commit()
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()
    return executed


def execute_sql_script(connection_name: str, script_path: str) -> List[str]:
    """Read a ``.sql`` file and execute its statements via :func:`execute_statements`."""
    with open(script_path) as fh:
        script = fh.read()
    statements = _split_sql_statements(script)
    return execute_statements(connection_name, statements)
