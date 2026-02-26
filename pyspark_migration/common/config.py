"""
Configuration management for PySpark migration.
Replaces Informatica parameter files and session configs.

Informatica Artifacts Replaced:
- /data/BIISINT/control/BIIS_parms.iparms (parameter file)
- $PMRepositoryServiceName environment detection
- INFO_TARGET, $Source, INFO_NATE Oracle connections
- Credential files: /home/sa-biisint/.use, .pw, .use1, .pw1
"""

import os
from dataclasses import dataclass, field
from typing import Optional


@dataclass
class OracleConnectionConfig:
    """Oracle database connection configuration.
    
    Replaces Informatica connection objects:
    - INFO_TARGET: target database (ORA_BIIS)
    - $Source: source database (ORA_BIISPRD_SRC)
    - INFO_NATE: separate Oracle DB for lkp_PS_JPM_JP_ITEMS
    """
    host: str
    port: int
    service_name: str
    username: str
    password: str

    @property
    def jdbc_url(self) -> str:
        return f"jdbc:oracle:thin:@{self.host}:{self.port}/{self.service_name}"

    @property
    def dsn(self) -> str:
        return f"{self.host}:{self.port}/{self.service_name}"


@dataclass
class SparkConfig:
    """Spark session configuration.
    
    Replaces Informatica session configs:
    - Maximum Memory (512MB default, 2GB for EHRP2BIIS)
    - DTM buffer size (Auto or 24000000 for COMPTIME)
    - Enable Recovery = NO
    """
    app_name: str = "BIIS_ETL_Migration"
    executor_memory: str = "2g"
    driver_memory: str = "1g"
    shuffle_partitions: int = 200
    broadcast_threshold: int = 10485760  # 10MB
    checkpoint_enabled: bool = False
    checkpoint_dir: str = "/tmp/spark_checkpoints"


@dataclass
class EmailConfig:
    """Email notification configuration.
    
    Replaces Informatica email tasks and $$WF_*_EMAIL_LIST variables.
    Original recipients: peter.chen@hhs.gov, nathan.knight@hhs.gov, marvin.simon@hhs.gov
    """
    smtp_host: str = "localhost"
    smtp_port: int = 25
    from_address: str = "biis-etl@hhs.gov"
    default_recipients: list = field(default_factory=lambda: [
        "peter.chen@hhs.gov",
        "nathan.knight@hhs.gov",
        "marvin.simon@hhs.gov"
    ])


@dataclass
class PathConfig:
    """File path configuration.
    
    Replaces Informatica $Param_Root_Directory and file paths.
    """
    root_directory: str = "/data/BIISINT"
    control_dir: str = "/data/BIISINT/control"
    comptime_input_dir: str = "/data/BIISINT/data/int/in/COMPTIME"
    comptime_archive_dir: str = "/data/BIISINT/data/archive/COMPTIME"
    cpm_output_dir: str = "/data/BIISINT/data/int/out/CPM"
    log_dir: str = "/home/sa-biisint/data/int/log"
    ehrp2biis_bin_dir: str = "/data/BIISINT/bin/EHRP2BIIS"
    bad_file_dir: str = "/data/BIISINT/data/int/bad"
    parameter_file: str = "/data/BIISINT/control/BIIS_parms.iparms"


@dataclass
class MigrationConfig:
    """Master configuration for the entire migration.
    
    Consolidates all Informatica configuration into a single object.
    """
    environment: str = "Prod"
    info_target: Optional[OracleConnectionConfig] = None
    source_db: Optional[OracleConnectionConfig] = None
    info_nate: Optional[OracleConnectionConfig] = None
    spark: SparkConfig = field(default_factory=SparkConfig)
    email: EmailConfig = field(default_factory=EmailConfig)
    paths: PathConfig = field(default_factory=PathConfig)

    @property
    def env_prefix(self) -> str:
        """Replace DECODE(SUBSTR($PMRepositoryServiceName, 1, 4), ...) environment detection.
        
        Informatica original (XML/COMPTIME lines 91-95):
        DECODE(SUBSTR($PMRepositoryServiceName, 1, 4),
               'Dev_', 'Dev: ', 'Test', 'Test: ', 'Prod', 'Prod: ')
        """
        prefix_map = {"Dev": "Dev: ", "Test": "Test: ", "Prod": "Prod: "}
        return prefix_map.get(self.environment, "")


def load_config_from_env() -> MigrationConfig:
    """Load configuration from environment variables.
    
    Replaces Informatica credential files and parameter files:
    - /home/sa-biisint/.use  → DB_TARGET_USERNAME
    - /home/sa-biisint/.pw   → DB_TARGET_PASSWORD
    - /home/sa-biisint/.use1 → DB_SOURCE_USERNAME
    - /home/sa-biisint/.pw1  → DB_SOURCE_PASSWORD
    """
    config = MigrationConfig(
        environment=os.environ.get("ENVIRONMENT", "Prod"),
        info_target=OracleConnectionConfig(
            host=os.environ.get("DB_TARGET_HOST", "localhost"),
            port=int(os.environ.get("DB_TARGET_PORT", "1521")),
            service_name=os.environ.get("DB_TARGET_SERVICE", "ORA_BIIS"),
            username=os.environ.get("DB_TARGET_USERNAME", ""),
            password=os.environ.get("DB_TARGET_PASSWORD", ""),
        ),
        source_db=OracleConnectionConfig(
            host=os.environ.get("DB_SOURCE_HOST", "localhost"),
            port=int(os.environ.get("DB_SOURCE_PORT", "1521")),
            service_name=os.environ.get("DB_SOURCE_SERVICE", "ORA_BIISPRD_SRC"),
            username=os.environ.get("DB_SOURCE_USERNAME", ""),
            password=os.environ.get("DB_SOURCE_PASSWORD", ""),
        ),
        info_nate=OracleConnectionConfig(
            host=os.environ.get("DB_NATE_HOST", "localhost"),
            port=int(os.environ.get("DB_NATE_PORT", "1521")),
            service_name=os.environ.get("DB_NATE_SERVICE", "INFO_NATE"),
            username=os.environ.get("DB_NATE_USERNAME", ""),
            password=os.environ.get("DB_NATE_PASSWORD", ""),
        ),
    )

    # Load pay period parameters from environment or parameter file
    pp_end_year = os.environ.get("WF_PP_END_YEAR", "")
    pp_num = os.environ.get("WF_PP_NUM", "")
    pp_year_num = os.environ.get("WF_PP_YEAR_NUM", "")

    # Try to load from parameter file if not in environment
    if not pp_end_year and os.path.exists(config.paths.parameter_file):
        params = parse_parameter_file(config.paths.parameter_file)
        pp_end_year = params.get("PP_END_YEAR", "")
        pp_num = params.get("PP_NUM", "")
        pp_year_num = params.get("PP_YEAR_NUM", "")

    os.environ["WF_PP_END_YEAR"] = pp_end_year
    os.environ["WF_PP_NUM"] = pp_num
    os.environ["WF_PP_YEAR_NUM"] = pp_year_num

    return config


def parse_parameter_file(filepath: str) -> dict:
    """Parse Informatica parameter file (/data/BIISINT/control/BIIS_parms.iparms).
    
    Informatica .iparms format: KEY=VALUE pairs, one per line.
    Used by wf_Pay_Calendar and wf_COMPTIME.
    """
    params = {}
    if not os.path.exists(filepath):
        return params
    with open(filepath, "r") as f:
        for line in f:
            line = line.strip()
            if line and "=" in line and not line.startswith("#"):
                key, value = line.split("=", 1)
                params[key.strip()] = value.strip()
    return params
