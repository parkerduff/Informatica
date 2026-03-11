"""
SparkSession factory with tuned defaults.

Replaces Informatica session-level settings:
  - DTM buffer size (Auto) -> spark.sql.shuffle.partitions
  - Maximum Memory -> spark.executor.memory / spark.driver.memory
  - High Precision -> DecimalType enforcement (in job code)
  - Session on Grid -> Spark parallelism via executor count
"""

import logging
from typing import Optional

from pyspark.sql import SparkSession

from pyspark_migration.common.config import SparkConfig

logger = logging.getLogger(__name__)


def create_spark_session(
    config: Optional[SparkConfig] = None,
    app_name_suffix: str = "",
) -> SparkSession:
    """Create a configured SparkSession with Oracle JDBC driver.

    Args:
        config: SparkConfig dataclass. Uses defaults if None.
        app_name_suffix: Optional suffix appended to the app name
            (e.g., '_pay_calendar').

    Returns:
        Configured SparkSession instance.
    """
    if config is None:
        config = SparkConfig()

    app_name = config.app_name
    if app_name_suffix:
        app_name = f"{app_name}_{app_name_suffix}"

    builder = (
        SparkSession.builder.appName(app_name)
        .master(config.master)
        .config("spark.executor.memory", config.executor_memory)
        .config("spark.driver.memory", config.driver_memory)
        .config(
            "spark.sql.shuffle.partitions", str(config.shuffle_partitions)
        )
        .config(
            "spark.sql.autoBroadcastJoinThreshold",
            str(config.broadcast_threshold),
        )
        .config(
            "spark.sql.adaptive.enabled", str(config.adaptive_enabled).lower()
        )
        .config("spark.jars", config.jdbc_driver_path)
        .config("spark.driver.extraClassPath", config.jdbc_driver_path)
        .config("spark.executor.extraClassPath", config.jdbc_driver_path)
    )

    if config.checkpoint_dir:
        builder = builder.config(
            "spark.sql.streaming.checkpointLocation", config.checkpoint_dir
        )

    spark = builder.getOrCreate()

    if config.checkpoint_dir:
        spark.sparkContext.setCheckpointDir(config.checkpoint_dir)

    logger.info(
        "SparkSession created: app=%s, master=%s, executorMem=%s, "
        "driverMem=%s, shufflePartitions=%d",
        app_name,
        config.master,
        config.executor_memory,
        config.driver_memory,
        config.shuffle_partitions,
    )

    return spark
