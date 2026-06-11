"""Shared SparkSession factory for all BIIS jobs."""
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")

MSSQL_JDBC = "com.microsoft.sqlserver:mssql-jdbc:12.4.2.jre11"


def get_spark(app_name: str):
    from pyspark.sql import SparkSession

    return (
        SparkSession.builder.appName(app_name)
        .config("spark.jars.packages", MSSQL_JDBC)
        .config("spark.sql.session.timeZone", "UTC")
        .master("local[*]")
        .getOrCreate()
    )
