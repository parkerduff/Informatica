"""
Configuration management for BIISINT PySpark ETL pipelines.

Replaces:
  - /home/sa-biisint/bin/SETENV
  - /home/sa-biisint/.use1, .pw1 credential files
  - /data/BIISINT/control/BIIS_parms.iparms parameter file

All environment-specific settings are managed here. Credentials are read
from environment variables or secure credential files at runtime.
"""

import os
from dataclasses import dataclass, field
from typing import Optional


@dataclass
class DatabaseConfig:
    """Oracle JDBC connection configuration."""

    host: str = os.getenv("BIIS_DB_HOST", "localhost")
    port: str = os.getenv("BIIS_DB_PORT", "1521")
    service_name: str = os.getenv("BIIS_DB_SERVICE", "BIISPRD")
    user: str = os.getenv("BIIS_DB_USER", "")
    password: str = os.getenv("BIIS_DB_PASSWORD", "")
    driver: str = "oracle.jdbc.driver.OracleDriver"

    @property
    def jdbc_url(self) -> str:
        return f"jdbc:oracle:thin:@{self.host}:{self.port}/{self.service_name}"

    @property
    def connection_properties(self) -> dict:
        return {
            "user": self.user,
            "password": self.password,
            "driver": self.driver,
        }


@dataclass
class EHRPSourceConfig:
    """EHRP source database configuration."""

    host: str = os.getenv("EHRP_DB_HOST", "localhost")
    port: str = os.getenv("EHRP_DB_PORT", "1521")
    service_name: str = os.getenv("EHRP_DB_SERVICE", "EHRPSRC")
    user: str = os.getenv("EHRP_DB_USER", "")
    password: str = os.getenv("EHRP_DB_PASSWORD", "")
    driver: str = "oracle.jdbc.driver.OracleDriver"

    @property
    def jdbc_url(self) -> str:
        return f"jdbc:oracle:thin:@{self.host}:{self.port}/{self.service_name}"

    @property
    def connection_properties(self) -> dict:
        return {
            "user": self.user,
            "password": self.password,
            "driver": self.driver,
        }


@dataclass
class PathConfig:
    """File system path configuration."""

    home_dir: str = os.getenv("BIIS_HOME", "/home/sa-biisint")
    data_root: str = os.getenv("BIIS_DATA_ROOT", "/data/BIISINT")

    @property
    def bin_dir(self) -> str:
        return os.path.join(self.data_root, "bin")

    @property
    def ehrp2biis_dir(self) -> str:
        return os.path.join(self.bin_dir, "EHRP2BIIS")

    @property
    def log_dir(self) -> str:
        return os.path.join(self.home_dir, "data", "int", "log")

    @property
    def cpm_output_dir(self) -> str:
        return os.path.join(self.data_root, "data", "int", "out", "CPM")

    @property
    def les_output_dir(self) -> str:
        return os.path.join(self.data_root, "data", "int", "out", "LES")

    @property
    def comptime_input_dir(self) -> str:
        return os.path.join(self.data_root, "data", "int", "in", "COMPTIME")

    @property
    def les_input_dir(self) -> str:
        return os.path.join(self.data_root, "data", "int", "in", "LES")

    @property
    def cpm_input_dir(self) -> str:
        return os.path.join(self.data_root, "data", "int", "in", "CPM")

    @property
    def pseudossn_input_dir(self) -> str:
        return os.path.join(self.data_root, "data", "int", "in", "Pseudossn")


@dataclass
class SFTPConfig:
    """SFTP transfer configuration."""

    host: str = os.getenv("SFTP_HOST", "m1csv301.hhs.gov")
    user: str = os.getenv("SFTP_USER", "sa-cdirect")
    key_file: str = os.getenv("SFTP_KEY_FILE", "")
    password: str = os.getenv("SFTP_PASSWORD", "")

    # Agency-specific outbound directories on the SFTP server
    nih_outbound: str = "/opt/app/jail/sa-nihbiisu/outbound"
    oig_outbound: str = "/opt/app/jail/sa-oig/outbound"
    fda_outbound: str = "/opt/app/jail/sa-fdausr2/outbound"
    cdc_outbound: str = "/opt/app/jail/sa-cdcusr/outbound"
    afps_outbound: str = "/opt/app/jail/sa-afpsusr/outbound"


@dataclass
class EmailConfig:
    """Email notification configuration."""

    smtp_host: str = os.getenv("SMTP_HOST", "localhost")
    smtp_port: int = int(os.getenv("SMTP_PORT", "25"))
    from_address: str = os.getenv("EMAIL_FROM", "biisint@hhs.gov")
    default_recipients: list = field(default_factory=lambda: [
        "peter.chen@hhs.gov",
        "nathan.knight@hhs.gov",
        "marvin.simon@hhs.gov",
        "mariappan.muthiah@hhs.gov",
    ])
    transfer_recipients: list = field(default_factory=lambda: [
        "mariappan.muthiah@hhs.gov",
        "nathan.knight@hhs.gov",
        "karen.williams@hhs.gov",
        "marvin.simon@hhs.gov",
        "Robin.Cunningham@hhs.gov",
        "minh.tran@hhs.gov",
    ])


@dataclass
class SparkConfig:
    """PySpark session configuration."""

    app_name: str = "BIISINT_ETL"
    master: str = os.getenv("SPARK_MASTER", "local[*]")
    oracle_jdbc_jar: str = os.getenv(
        "ORACLE_JDBC_JAR", "/opt/spark/jars/ojdbc8.jar"
    )
    executor_memory: str = os.getenv("SPARK_EXECUTOR_MEMORY", "4g")
    driver_memory: str = os.getenv("SPARK_DRIVER_MEMORY", "2g")


@dataclass
class PayCalendarParams:
    """Pay Calendar workflow parameters."""

    pp_num: Optional[int] = None
    pp_end_year: Optional[int] = None

    def __post_init__(self):
        env_pp_num = os.getenv("PP_NUM")
        env_pp_end_year = os.getenv("PP_END_YEAR")
        if env_pp_num and env_pp_num.isdigit():
            self.pp_num = int(env_pp_num)
        if env_pp_end_year and env_pp_end_year.isdigit():
            self.pp_end_year = int(env_pp_end_year)

    @property
    def params_exist(self) -> bool:
        return self.pp_num is not None and self.pp_end_year is not None


class AppConfig:
    """Central configuration for the BIISINT application."""

    def __init__(self):
        self.db = DatabaseConfig()
        self.ehrp_source = EHRPSourceConfig()
        self.paths = PathConfig()
        self.sftp = SFTPConfig()
        self.email = EmailConfig()
        self.spark = SparkConfig()
        self.pay_calendar_params = PayCalendarParams()

    @property
    def environment(self) -> str:
        """Detect environment from BIIS_ENV variable."""
        env = os.getenv("BIIS_ENV", "Prod").lower()
        if env.startswith("dev"):
            return "Dev"
        elif env.startswith("test"):
            return "Test"
        return "Prod"
