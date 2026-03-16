"""
SparkSession Factory

Creates and configures SparkSession with Oracle JDBC driver support.
Replaces Informatica PowerCenter integration service configuration.
"""

from pyspark.sql import SparkSession

from pyspark_migration.config.settings import SPARK_CONFIG, get_environment


def get_spark_session(app_name, environment=None):
    """
    Create a configured SparkSession with Oracle JDBC driver.

    Parameters
    ----------
    app_name : str
        Name for the Spark application (e.g. 'biis_pay_calendar').
    environment : str, optional
        Environment name ('dev', 'test', 'prod'). Defaults to config value.

    Returns
    -------
    SparkSession
        Configured SparkSession instance.
    """
    if environment is None:
        environment = get_environment()

    env_prefix = {"dev": "DEV", "test": "TEST", "prod": "PROD"}.get(
        environment, "DEV"
    )

    builder = (
        SparkSession.builder.appName(f"{env_prefix}_{app_name}")
        .config("spark.driver.memory", SPARK_CONFIG["driver_memory"])
        .config("spark.executor.memory", SPARK_CONFIG["executor_memory"])
        .config("spark.executor.cores", SPARK_CONFIG["executor_cores"])
        .config("spark.jars", SPARK_CONFIG["oracle_jdbc_jar"])
        .config("spark.driver.extraClassPath", SPARK_CONFIG["oracle_jdbc_jar"])
        .config(
            "spark.executor.extraClassPath", SPARK_CONFIG["oracle_jdbc_jar"]
        )
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
        .config("spark.sql.session.timeZone", "America/New_York")
    )

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
