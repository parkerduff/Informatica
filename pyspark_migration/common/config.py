"""
Configuration management for PySpark migration.

Replaces Informatica artifacts:
  - BIIS_parms.iparms (parameter file) -> dataclass configs loaded from env vars
  - INFO_TARGET, $Source, $Target connections -> OracleConnectionConfig
  - Session-level DTM buffer, commit interval -> SparkConfig
  - Email task recipients -> EmailConfig
  - $PMBadFileDir, $PMSourceFileDir -> PathConfig

All credentials are loaded from environment variables - never hardcoded.
"""

import os
from dataclasses import dataclass, field
from typing import Optional


@dataclass
class OracleConnectionConfig:
    """Oracle JDBC connection configuration.

    Replaces Informatica connection objects: INFO_TARGET, $Source, $Target,
    INFO_NATE, BIISPRD.
    """

    host: str = field(default_factory=lambda: os.environ.get("ORACLE_HOST", "localhost"))
    port: int = field(default_factory=lambda: int(os.environ.get("ORACLE_PORT", "1521")))
    service_name: str = field(
        default_factory=lambda: os.environ.get("ORACLE_SERVICE_NAME", "XEPDB1")
    )
    username: str = field(
        default_factory=lambda: os.environ.get("ORACLE_USERNAME", "biis_user")
    )
    password: str = field(
        default_factory=lambda: os.environ.get("ORACLE_PASSWORD", "")
    )

    @property
    def jdbc_url(self) -> str:
        """JDBC URL for Spark JDBC reads/writes."""
        return f"jdbc:oracle:thin:@{self.host}:{self.port}/{self.service_name}"

    @property
    def thin_url(self) -> str:
        """python-oracledb thin connection string."""
        return f"{self.host}:{self.port}/{self.service_name}"


@dataclass
class SparkConfig:
    """Spark session configuration.

    Replaces Informatica session-level settings:
      - DTM buffer size (Auto) -> shuffle_partitions
      - Maximum Memory -> executor_memory, driver_memory
      - Commit Interval (10000) -> jdbc_batch_size
      - High Precision -> use DecimalType (enforced in code)
    """

    app_name: str = "informatica_pyspark_migration"
    master: str = field(
        default_factory=lambda: os.environ.get("SPARK_MASTER", "local[*]")
    )
    executor_memory: str = field(
        default_factory=lambda: os.environ.get("SPARK_EXECUTOR_MEMORY", "2g")
    )
    driver_memory: str = field(
        default_factory=lambda: os.environ.get("SPARK_DRIVER_MEMORY", "2g")
    )
    shuffle_partitions: int = field(
        default_factory=lambda: int(os.environ.get("SPARK_SHUFFLE_PARTITIONS", "200"))
    )
    broadcast_threshold: int = field(
        default_factory=lambda: int(
            os.environ.get("SPARK_BROADCAST_THRESHOLD", "10485760")
        )
    )
    jdbc_batch_size: int = field(
        default_factory=lambda: int(os.environ.get("JDBC_BATCH_SIZE", "10000"))
    )
    checkpoint_dir: Optional[str] = field(
        default_factory=lambda: os.environ.get("SPARK_CHECKPOINT_DIR")
    )
    adaptive_enabled: bool = True
    jdbc_driver_path: str = field(
        default_factory=lambda: os.environ.get(
            "ORACLE_JDBC_DRIVER_PATH", "/opt/oracle/ojdbc11.jar"
        )
    )


@dataclass
class EmailConfig:
    """Email notification configuration.

    Replaces Informatica email tasks: $$WF_*_EMAIL_LIST, environment prefix
    detection from $PMRepositoryServiceName.
    """

    smtp_host: str = field(
        default_factory=lambda: os.environ.get("SMTP_HOST", "localhost")
    )
    smtp_port: int = field(
        default_factory=lambda: int(os.environ.get("SMTP_PORT", "25"))
    )
    sender: str = field(
        default_factory=lambda: os.environ.get(
            "EMAIL_SENDER", "biis-etl@hhs.gov"
        )
    )
    default_recipients: str = field(
        default_factory=lambda: os.environ.get(
            "EMAIL_RECIPIENTS",
            "peter.chen@hhs.gov,nathan.knight@hhs.gov,"
            "marvin.simon@hhs.gov,mariappan.muthiah@hhs.gov",
        )
    )
    environment: str = field(
        default_factory=lambda: os.environ.get("ENVIRONMENT", "Dev")
    )
    enabled: bool = field(
        default_factory=lambda: os.environ.get("EMAIL_ENABLED", "false").lower()
        == "true"
    )

    @property
    def environment_prefix(self) -> str:
        """Environment prefix for email subjects.

        Replaces Informatica: DECODE(SUBSTR($PMRepositoryServiceName, 1, 4),
            'Dev_', 'Dev: ', 'Test', 'Test: ', '')
        """
        env = self.environment.lower()
        if env.startswith("dev"):
            return "Dev: "
        elif env.startswith("test"):
            return "Test: "
        elif env.startswith("prod"):
            return ""
        return f"{self.environment}: "


@dataclass
class PathConfig:
    """File path configuration.

    Replaces Informatica path variables:
      - $PMBadFileDir -> reject_dir
      - $PMSourceFileDir -> source_dir
      - $PMTargetFileDir -> target_dir
    """

    reject_dir: str = field(
        default_factory=lambda: os.environ.get("PM_BAD_FILE_DIR", "/tmp/bad_files")
    )
    source_dir: str = field(
        default_factory=lambda: os.environ.get("PM_SOURCE_FILE_DIR", "/tmp/source_files")
    )
    target_dir: str = field(
        default_factory=lambda: os.environ.get("PM_TARGET_FILE_DIR", "/tmp/target_files")
    )


@dataclass
class MigrationConfig:
    """Top-level migration configuration aggregating all sub-configs."""

    oracle: OracleConnectionConfig = field(default_factory=OracleConnectionConfig)
    oracle_source: Optional[OracleConnectionConfig] = None
    oracle_cross_db: Optional[OracleConnectionConfig] = None
    spark: SparkConfig = field(default_factory=SparkConfig)
    email: EmailConfig = field(default_factory=EmailConfig)
    paths: PathConfig = field(default_factory=PathConfig)
    stop_on_errors: int = field(
        default_factory=lambda: int(os.environ.get("STOP_ON_ERRORS", "0"))
    )
    error_threshold_pct: float = field(
        default_factory=lambda: float(
            os.environ.get("ERROR_THRESHOLD_PCT", "0.05")
        )
    )
    completeness_threshold: float = field(
        default_factory=lambda: float(
            os.environ.get("COMPLETENESS_THRESHOLD", "0.95")
        )
    )
    sla_seconds: int = field(
        default_factory=lambda: int(os.environ.get("SLA_SECONDS", "3600"))
    )

    def __post_init__(self) -> None:
        if self.oracle_source is None:
            source_host = os.environ.get("ORACLE_SOURCE_HOST")
            if source_host:
                self.oracle_source = OracleConnectionConfig(
                    host=source_host,
                    port=int(os.environ.get("ORACLE_SOURCE_PORT", "1521")),
                    service_name=os.environ.get(
                        "ORACLE_SOURCE_SERVICE_NAME", "XEPDB1"
                    ),
                    username=os.environ.get("ORACLE_SOURCE_USERNAME", "biis_user"),
                    password=os.environ.get("ORACLE_SOURCE_PASSWORD", ""),
                )
            else:
                self.oracle_source = self.oracle

        if self.oracle_cross_db is None:
            cross_host = os.environ.get("ORACLE_CROSS_DB_HOST")
            if cross_host:
                self.oracle_cross_db = OracleConnectionConfig(
                    host=cross_host,
                    port=int(os.environ.get("ORACLE_CROSS_DB_PORT", "1521")),
                    service_name=os.environ.get(
                        "ORACLE_CROSS_DB_SERVICE_NAME", "XEPDB1"
                    ),
                    username=os.environ.get("ORACLE_CROSS_DB_USERNAME", "biis_user"),
                    password=os.environ.get("ORACLE_CROSS_DB_PASSWORD", ""),
                )
            else:
                self.oracle_cross_db = self.oracle
