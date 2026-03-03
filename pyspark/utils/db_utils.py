"""
Database utility functions for BIISINT PySpark ETL pipelines.

Replaces:
  - sqlplus -s '/nolog' connect $loin2/$ps2 patterns from KSH scripts
  - Informatica PowerCenter Source Qualifier / Target connections
  - Oracle JDBC read/write operations
"""

from pyspark.sql import DataFrame, SparkSession

from pyspark.utils.config import DatabaseConfig, EHRPSourceConfig


def read_table(
    spark: SparkSession,
    db_config: DatabaseConfig,
    table_name: str,
    query: str = "",
) -> DataFrame:
    """Read a table or execute a query via JDBC.

    Args:
        spark: Active SparkSession.
        db_config: Database connection configuration.
        table_name: Fully-qualified table name (e.g. "HISTDBA.PAY_PERIOD").
        query: Optional SQL query override. When provided, ``table_name``
            is ignored and the query is used as a JDBC sub-query.

    Returns:
        A Spark DataFrame with the query results.
    """
    reader = spark.read.format("jdbc").options(
        url=db_config.jdbc_url,
        driver=db_config.driver,
        user=db_config.user,
        password=db_config.password,
    )

    if query:
        reader = reader.option("dbtable", f"({query}) tmp")
    else:
        reader = reader.option("dbtable", table_name)

    return reader.load()


def read_ehrp_table(
    spark: SparkSession,
    ehrp_config: EHRPSourceConfig,
    table_name: str,
    query: str = "",
) -> DataFrame:
    """Read from the EHRP source database.

    Args:
        spark: Active SparkSession.
        ehrp_config: EHRP source database configuration.
        table_name: Table name in the EHRP schema.
        query: Optional SQL query override.

    Returns:
        A Spark DataFrame with the query results.
    """
    reader = spark.read.format("jdbc").options(
        url=ehrp_config.jdbc_url,
        driver=ehrp_config.driver,
        user=ehrp_config.user,
        password=ehrp_config.password,
    )

    if query:
        reader = reader.option("dbtable", f"({query}) tmp")
    else:
        reader = reader.option("dbtable", table_name)

    return reader.load()


def write_table(
    df: DataFrame,
    db_config: DatabaseConfig,
    table_name: str,
    mode: str = "append",
) -> None:
    """Write a DataFrame to an Oracle table via JDBC.

    Args:
        df: Spark DataFrame to write.
        db_config: Database connection configuration.
        table_name: Target table name.
        mode: Spark write mode ("append", "overwrite", "ignore", "error").
    """
    df.write.format("jdbc").options(
        url=db_config.jdbc_url,
        driver=db_config.driver,
        user=db_config.user,
        password=db_config.password,
        dbtable=table_name,
    ).mode(mode).save()


def execute_sql(
    spark: SparkSession,
    db_config: DatabaseConfig,
    sql_statements: list,
) -> None:
    """Execute arbitrary SQL statements against the database.

    This is used for DDL/DML that cannot be expressed as DataFrame
    operations (e.g. stored procedure calls, TRUNCATE, UPDATE).

    Args:
        spark: Active SparkSession.
        db_config: Database connection configuration.
        sql_statements: List of SQL strings to execute sequentially.
    """
    # Use the JDBC driver directly for non-query statements
    spark._jvm.java.lang.Class.forName(db_config.driver)
    connection = spark._jvm.java.sql.DriverManager.getConnection(
        db_config.jdbc_url, db_config.user, db_config.password
    )
    try:
        connection.setAutoCommit(False)
        statement = connection.createStatement()
        for sql in sql_statements:
            statement.execute(sql)
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()


def call_stored_procedure(
    spark: SparkSession,
    db_config: DatabaseConfig,
    procedure_name: str,
    params: str = "",
) -> None:
    """Call an Oracle stored procedure.

    Args:
        spark: Active SparkSession.
        db_config: Database connection configuration.
        procedure_name: Fully-qualified procedure name
            (e.g. "HISTDBA.UPDATE_ERP2BIIS_NO900S01_p").
        params: Optional parameter string for the procedure call.
    """
    if params:
        call_sql = f"BEGIN {procedure_name}({params}); END;"
    else:
        call_sql = f"BEGIN {procedure_name}; END;"
    execute_sql(spark, db_config, [call_sql])
