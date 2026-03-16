"""
BIIS ETL Configuration Settings

Centralized configuration for database connections, file paths, and email recipients.
Replaces Informatica connection objects and shell script environment variables.
"""

import os


def get_environment():
    """Determine current environment from ENV variable or default to 'dev'."""
    return os.getenv("BIIS_ENVIRONMENT", "dev").lower()


# ---------------------------------------------------------------------------
# Oracle JDBC connection configurations
# Replaces Informatica connection objects: ORA_BIISPRD_SRC, ORA_BIIS
# ---------------------------------------------------------------------------

JDBC_CONNECTIONS = {
    "ORA_BIISPRD_SRC": {
        "url": os.getenv(
            "ORA_BIISPRD_SRC_URL",
            "jdbc:oracle:thin:@//biisprd-src-host:1521/BIISPRD",
        ),
        "user": os.getenv("ORA_BIISPRD_SRC_USER", ""),
        "password": os.getenv("ORA_BIISPRD_SRC_PASSWORD", ""),
        "driver": "oracle.jdbc.driver.OracleDriver",
    },
    "ORA_BIIS": {
        "url": os.getenv(
            "ORA_BIIS_URL",
            "jdbc:oracle:thin:@//biis-host:1521/BIIS",
        ),
        "user": os.getenv("ORA_BIIS_USER", ""),
        "password": os.getenv("ORA_BIIS_PASSWORD", ""),
        "driver": "oracle.jdbc.driver.OracleDriver",
    },
}

# ---------------------------------------------------------------------------
# cx_Oracle / jaydebeapi connection strings (for stored procedure execution)
# Replaces sqlplus calls from ehrp2biis_preload and actstage_load
# ---------------------------------------------------------------------------

ORACLE_DSN = {
    "ORA_BIISPRD_SRC": os.getenv(
        "ORA_BIISPRD_SRC_DSN", "biisprd-src-host:1521/BIISPRD"
    ),
    "ORA_BIIS": os.getenv("ORA_BIIS_DSN", "biis-host:1521/BIIS"),
}

# ---------------------------------------------------------------------------
# File paths
# Replaces shell variables from ehrp2biis_preload, actstage_load, transfers
# ---------------------------------------------------------------------------

DATA_ROOT = os.getenv("BIIS_DATA_ROOT", "/data/BIISINT")
HOME_DIR = os.getenv("BIIS_HOME", "/home/sa-biisint")

FILE_PATHS = {
    "data_root": DATA_ROOT,
    "home_dir": HOME_DIR,
    "ehrp2biis_bin": os.path.join(DATA_ROOT, "bin/EHRP2BIIS"),
    "log_dir": os.path.join(HOME_DIR, "data/int/log"),
    "cpm_output_dir": os.path.join(DATA_ROOT, "data/int/out/CPM"),
    "les_output_dir": os.path.join(DATA_ROOT, "data/int/out/LES"),
    "control_dir": os.path.join(DATA_ROOT, "control"),
    "comptime_input": os.getenv(
        "COMPTIME_INPUT_FILE",
        os.path.join(DATA_ROOT, "data/int/in/U0287D01"),
    ),
    "pseudossn_input": os.getenv(
        "PSEUDOSSN_INPUT_FILE",
        os.path.join(DATA_ROOT, "data/int/in/PSEUDOSSN_FROM_SDA_TBL.dat"),
    ),
    "les_input_dir": os.getenv(
        "LES_INPUT_DIR",
        os.path.join(DATA_ROOT, "data/int/in/LES"),
    ),
    "cpm_ytd_input": os.getenv(
        "CPM_YTD_INPUT",
        os.path.join(DATA_ROOT, "data/int/in/CPM/PC_DOEYTD_RDF.TXT"),
    ),
    "cpm_mer_input": os.getenv(
        "CPM_MER_INPUT",
        os.path.join(DATA_ROOT, "data/int/in/CPM/PC_DOEMER_RDF.TXT"),
    ),
}

# ---------------------------------------------------------------------------
# SFTP transfer configuration
# Replaces ksh transfer scripts (nih_cpm_transfer, oig_transfer, etc.)
# ---------------------------------------------------------------------------

SFTP_CONFIG = {
    "host": os.getenv("SFTP_HOST", "m1csv301.hhs.gov"),
    "username": os.getenv("SFTP_USER", "sa-cdirect"),
    "port": int(os.getenv("SFTP_PORT", "22")),
    "key_file": os.getenv("SFTP_KEY_FILE", ""),
}

TRANSFER_DESTINATIONS = {
    "nih": "/opt/app/jail/sa-nihbiisu/outbound",
    "oig": "/opt/app/jail/sa-oig/outbound",
    "fda": "/opt/app/jail/sa-fdausr2/outbound",
    "cdc": "/opt/app/jail/sa-cdcbiis/outbound",
    "afps": "/opt/app/jail/sa-afps/outbound",
}

# ---------------------------------------------------------------------------
# Email notification configuration
# Replaces mailx calls from ehrp2biis_preload line 8
# ---------------------------------------------------------------------------

EMAIL_CONFIG = {
    "smtp_host": os.getenv("SMTP_HOST", "localhost"),
    "smtp_port": int(os.getenv("SMTP_PORT", "25")),
    "from_address": os.getenv("EMAIL_FROM", "biis-etl@hhs.gov"),
    "default_recipients": os.getenv(
        "EMAIL_RECIPIENTS",
        "peter.chen@hhs.gov nathan.knight@hhs.gov marvin.simon@hhs.gov",
    ).split(),
    "transfer_recipients": os.getenv(
        "TRANSFER_EMAIL_RECIPIENTS",
        "mariappan.muthiah@hhs.gov nathan.knight@hhs.gov "
        "karen.williams@hhs.gov marvin.simon@hhs.gov "
        "Robin.Cunningham@hhs.gov minh.tran@hhs.gov",
    ).split(),
}

# ---------------------------------------------------------------------------
# Spark configuration
# ---------------------------------------------------------------------------

SPARK_CONFIG = {
    "oracle_jdbc_jar": os.getenv(
        "ORACLE_JDBC_JAR",
        "/opt/spark/jars/ojdbc8.jar",
    ),
    "driver_memory": os.getenv("SPARK_DRIVER_MEMORY", "4g"),
    "executor_memory": os.getenv("SPARK_EXECUTOR_MEMORY", "4g"),
    "executor_cores": int(os.getenv("SPARK_EXECUTOR_CORES", "2")),
}

# ---------------------------------------------------------------------------
# Database schemas
# ---------------------------------------------------------------------------

SCHEMAS = {
    "histdba": "HISTDBA",
    "nknight": "NKNIGHT",
    "ehrp": "EHRP",
}
