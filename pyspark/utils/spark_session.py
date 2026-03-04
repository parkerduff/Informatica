"""
PySpark session factory for BIISINT ETL pipelines.

Replaces:
  - Informatica PowerCenter session/workflow runtime
  - INFA_HOME / LD_LIBRARY_PATH environment setup from KSH scripts
"""

from pyspark.sql import SparkSession

from pyspark.utils.config import AppConfig


def create_spark_session(config: AppConfig, app_name_suffix: str = "") -> SparkSession:
    """Create a configured SparkSession for BIISINT ETL jobs.

    Args:
        config: Application configuration.
        app_name_suffix: Optional suffix appended to the Spark app name
            (e.g. "Pay_Calendar", "EHRP2BIIS").

    Returns:
        A configured SparkSession instance.
    """
    full_app_name = config.spark.app_name
    if app_name_suffix:
        full_app_name = f"{full_app_name}_{app_name_suffix}"

    builder = (
        SparkSession.builder
        .appName(full_app_name)
        .master(config.spark.master)
        .config("spark.executor.memory", config.spark.executor_memory)
        .config("spark.driver.memory", config.spark.driver_memory)
        .config("spark.jars", config.spark.oracle_jdbc_jar)
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
    )

    return builder.getOrCreate()
