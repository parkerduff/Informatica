"""SparkSession factory shared by all jobs."""
from __future__ import annotations

import os
from functools import lru_cache


@lru_cache(maxsize=1)
def get_spark(app_name: str = "biis_etl"):
    """Build (or reuse) a SparkSession.

    The Oracle JDBC driver jar is added when ``ORACLE_JDBC_JAR`` points at it so
    DataFrame reads/writes work out of the box.
    """
    from pyspark.sql import SparkSession

    builder = SparkSession.builder.appName(app_name)
    jar = os.environ.get("ORACLE_JDBC_JAR")
    if jar:
        builder = builder.config("spark.jars", jar)
    return builder.getOrCreate()
