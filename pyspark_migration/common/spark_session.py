"""
Spark session factory for PySpark migration.

Replaces Informatica PowerCenter Integration Service configuration:
- Prd_IS (production), Test_IS (test/dev)
- default_session_config settings
- DTM buffer size, Maximum Memory, Enable Recovery
"""

import logging
from pyspark.sql import SparkSession

from pyspark_migration.common.config import SparkConfig

logger = logging.getLogger(__name__)


def create_spark_session(config: SparkConfig, job_name: str = "") -> SparkSession:
    """Create a configured Spark session.
    
    Replaces Informatica Integration Service and session configuration:
    - Maximum Memory Allowed For Auto Memory Attributes = 512MB (default) / 2GB (EHRP2BIIS)
    - DTM buffer size = Auto / 24000000 (COMPTIME)
    - Enable Recovery = NO → checkpoint_enabled = False
    - Cache LOOKUP() function = YES → broadcast join threshold
    """
    app_name = f"{config.app_name}_{job_name}" if job_name else config.app_name

    builder = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.executor.memory", config.executor_memory)
        .config("spark.driver.memory", config.driver_memory)
        .config("spark.sql.shuffle.partitions", str(config.shuffle_partitions))
        .config("spark.sql.autoBroadcastJoinThreshold", str(config.broadcast_threshold))
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
    )

    if config.checkpoint_enabled:
        builder = builder.config("spark.checkpoint.dir", config.checkpoint_dir)

    spark = builder.getOrCreate()

    if config.checkpoint_enabled:
        spark.sparkContext.setCheckpointDir(config.checkpoint_dir)
        logger.info(f"Checkpointing enabled: {config.checkpoint_dir}")

    logger.info(
        f"Spark session created: {app_name} "
        f"(executor_memory={config.executor_memory}, "
        f"shuffle_partitions={config.shuffle_partitions})"
    )
    return spark
