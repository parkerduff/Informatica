"""SparkSession factory shared by jobs, scripts and tests."""
from __future__ import annotations

from typing import Any, Optional

_SESSION: Optional[Any] = None


def get_spark(app_name: str = "biis-etl", jdbc: bool = True) -> Any:
    """Return a (cached) local SparkSession.

    When ``jdbc`` is set and the mssql-jdbc jar is not already on the classpath
    (e.g. when launched via ``python`` instead of ``spark-submit``) the driver
    is pulled from Maven so JDBC reads/writes work in any entry point.
    """
    global _SESSION
    if _SESSION is not None:
        return _SESSION

    from pyspark.sql import SparkSession

    builder = SparkSession.builder.appName(app_name).master(
        "local[2]"
    ).config("spark.sql.shuffle.partitions", "4").config(
        "spark.ui.enabled", "false"
    )
    if jdbc:
        builder = builder.config(
            "spark.jars.packages", "com.microsoft.sqlserver:mssql-jdbc:12.4.2.jre11"
        )
    _SESSION = builder.getOrCreate()
    _SESSION.sparkContext.setLogLevel("WARN")
    return _SESSION
