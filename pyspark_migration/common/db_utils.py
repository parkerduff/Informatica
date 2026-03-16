"""
Database Utilities

JDBC read/write helpers and stored procedure executor.
Replaces Informatica Source Qualifier / Target connections and sqlplus calls
from ehrp2biis_preload, actstage_load, and ehrp2biis_afterload.sql.
"""

import logging

import cx_Oracle

from pyspark_migration.config.settings import (
    JDBC_CONNECTIONS,
    ORACLE_DSN,
    SCHEMAS,
)

logger = logging.getLogger(__name__)


def _get_jdbc_properties(connection_name):
    """Return JDBC connection properties dict for spark.read/write.jdbc()."""
    conn = JDBC_CONNECTIONS[connection_name]
    return {
        "user": conn["user"],
        "password": conn["password"],
        "driver": conn["driver"],
    }


def read_oracle_table(spark, table_name, schema=None, connection_name="ORA_BIIS"):
    """
    Read an Oracle table into a Spark DataFrame via JDBC.

    Replaces Informatica Source Qualifier transformations.

    Parameters
    ----------
    spark : SparkSession
    table_name : str
        Table name (e.g. 'PAY_PERIOD').
    schema : str, optional
        Oracle schema/owner (e.g. 'HISTDBA'). If provided, qualifies table.
    connection_name : str
        Key in JDBC_CONNECTIONS ('ORA_BIISPRD_SRC' or 'ORA_BIIS').

    Returns
    -------
    DataFrame
    """
    conn = JDBC_CONNECTIONS[connection_name]
    qualified = f"{schema}.{table_name}" if schema else table_name
    logger.info("Reading table %s via %s", qualified, connection_name)

    return (
        spark.read.format("jdbc")
        .option("url", conn["url"])
        .option("dbtable", qualified)
        .option("user", conn["user"])
        .option("password", conn["password"])
        .option("driver", conn["driver"])
        .option("fetchsize", "10000")
        .load()
    )


def read_oracle_query(spark, query, connection_name="ORA_BIIS"):
    """
    Execute a SQL query and return results as a DataFrame.

    Parameters
    ----------
    spark : SparkSession
    query : str
        SQL SELECT statement wrapped in parentheses for JDBC.
    connection_name : str

    Returns
    -------
    DataFrame
    """
    conn = JDBC_CONNECTIONS[connection_name]
    logger.info("Executing query via %s: %s", connection_name, query[:200])

    return (
        spark.read.format("jdbc")
        .option("url", conn["url"])
        .option("dbtable", f"({query}) tmp")
        .option("user", conn["user"])
        .option("password", conn["password"])
        .option("driver", conn["driver"])
        .option("fetchsize", "10000")
        .load()
    )


def write_oracle_table(df, table_name, schema=None, connection_name="ORA_BIIS",
                        mode="append"):
    """
    Write a DataFrame to an Oracle table via JDBC.

    Replaces Informatica Target transformations.

    Parameters
    ----------
    df : DataFrame
    table_name : str
    schema : str, optional
    connection_name : str
    mode : str
        Spark write mode ('append', 'overwrite', 'ignore', 'error').
    """
    conn = JDBC_CONNECTIONS[connection_name]
    qualified = f"{schema}.{table_name}" if schema else table_name
    logger.info("Writing to table %s via %s (mode=%s)", qualified, connection_name, mode)

    (
        df.write.format("jdbc")
        .option("url", conn["url"])
        .option("dbtable", qualified)
        .option("user", conn["user"])
        .option("password", conn["password"])
        .option("driver", conn["driver"])
        .option("batchsize", "10000")
        .mode(mode)
        .save()
    )


def _get_cx_oracle_connection(connection_name="ORA_BIIS"):
    """
    Create a cx_Oracle connection for DDL/DML that cannot be expressed
    as DataFrame operations.

    Used by execute_sql() and truncate_table().
    """
    conn_cfg = JDBC_CONNECTIONS[connection_name]
    dsn = ORACLE_DSN[connection_name]
    connection = cx_Oracle.connect(
        user=conn_cfg["user"],
        password=conn_cfg["password"],
        dsn=dsn,
    )
    return connection


def execute_sql(connection_name, sql_statement, params=None):
    """
    Execute raw SQL (INSERT/UPDATE/DELETE/EXEC) via cx_Oracle.

    This is critical for the 8+ stored procedures in ehrp2biis_afterload.sql
    (lines 59-74) that cannot be expressed as DataFrame operations.

    Parameters
    ----------
    connection_name : str
        Key in JDBC_CONNECTIONS.
    sql_statement : str
        SQL statement to execute. For stored procedures use:
        'BEGIN schema.procedure_name; END;'
    params : dict, optional
        Bind parameters for the SQL statement.

    Returns
    -------
    int
        Number of rows affected (for DML), or 0 for DDL/procedures.
    """
    logger.info("Executing SQL on %s: %s", connection_name, sql_statement[:200])
    connection = _get_cx_oracle_connection(connection_name)
    cursor = connection.cursor()
    try:
        if params:
            cursor.execute(sql_statement, params)
        else:
            cursor.execute(sql_statement)
        connection.commit()
        rowcount = cursor.rowcount if cursor.rowcount >= 0 else 0
        logger.info("SQL executed successfully. Rows affected: %d", rowcount)
        return rowcount
    except Exception:
        connection.rollback()
        logger.exception("SQL execution failed: %s", sql_statement[:200])
        raise
    finally:
        cursor.close()
        connection.close()


def execute_procedure(connection_name, procedure_name):
    """
    Execute an Oracle stored procedure.

    Replaces EXEC statements from ehrp2biis_afterload.sql lines 59-74:
        EXEC HISTDBA.UPDT_ERP2BIIS_CRE8_REMARKS01_P
        EXEC HISTDBA.UPDATE_ERP2BIIS_NO900S01_p
        etc.

    Parameters
    ----------
    connection_name : str
    procedure_name : str
        Fully qualified procedure name (e.g. 'HISTDBA.UPDT_ERP2BIIS_CRE8_REMARKS01_P').
    """
    sql = f"BEGIN {procedure_name}; END;"
    logger.info("Executing procedure: %s", procedure_name)
    return execute_sql(connection_name, sql)


def execute_procedure_with_args(connection_name, procedure_name, args=None):
    """
    Execute an Oracle stored procedure with arguments.

    Parameters
    ----------
    connection_name : str
    procedure_name : str
    args : list, optional
        Positional arguments to the procedure.
    """
    if args:
        placeholders = ", ".join([f":{i}" for i in range(len(args))])
        sql = f"BEGIN {procedure_name}({placeholders}); END;"
        params = {str(i): v for i, v in enumerate(args)}
    else:
        sql = f"BEGIN {procedure_name}; END;"
        params = None
    logger.info("Executing procedure: %s with args: %s", procedure_name, args)
    return execute_sql(connection_name, sql, params)


def truncate_table(connection_name, table_name):
    """
    Execute TRUNCATE TABLE.

    Replaces: truncate table nknight.nwk_new_ehrp_actions_tbl
    from ehrp2biis_afterload.sql line 286.

    Parameters
    ----------
    connection_name : str
    table_name : str
        Fully qualified table name.
    """
    sql = f"TRUNCATE TABLE {table_name}"
    logger.info("Truncating table: %s", table_name)
    connection = _get_cx_oracle_connection(connection_name)
    cursor = connection.cursor()
    try:
        cursor.execute(sql)
        logger.info("Table %s truncated successfully", table_name)
    except Exception:
        logger.exception("Failed to truncate table %s", table_name)
        raise
    finally:
        cursor.close()
        connection.close()


def compile_procedure(connection_name, procedure_name):
    """
    Compile an Oracle stored procedure.

    Replaces: ALTER PROCEDURE update_sequence_number_tbl_p COMPILE
    from ehrp2biis_afterload.sql line 44.

    Parameters
    ----------
    connection_name : str
    procedure_name : str
    """
    sql = f"ALTER PROCEDURE {procedure_name} COMPILE"
    logger.info("Compiling procedure: %s", procedure_name)
    return execute_sql(connection_name, sql)
