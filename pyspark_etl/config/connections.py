"""Connection configuration for the BIIS PySpark ETL jobs.

Values are loaded from environment variables (a local ``.env`` file is loaded
automatically if present). Oracle access is split across the schemas used by the
original Informatica mappings:

- NKNIGHT          -- staging schema for intermediate transformation
- EHRP             -- source PeopleSoft schema
- HISTDBA          -- BIIS production schema
- INFO_TARGET_DEV  -- development target schema
"""
import os

from dotenv import load_dotenv

load_dotenv()


def _env(name: str, default: str = "") -> str:
    return os.environ.get(name, default)


# --- Oracle (Spark JDBC + oracledb) -----------------------------------------
ORACLE_JDBC_URL = _env("ORACLE_JDBC_URL")
ORACLE_USER = _env("ORACLE_USER")
ORACLE_PASSWORD = _env("ORACLE_PASSWORD")

# Schema names used to qualify table references.
ORACLE_SCHEMAS = {
    "staging": _env("ORACLE_SCHEMA_NKNIGHT", "NKNIGHT"),
    "source": _env("ORACLE_SCHEMA_EHRP", "EHRP"),
    "production": _env("ORACLE_SCHEMA_HISTDBA", "HISTDBA"),
    "target_dev": _env("ORACLE_SCHEMA_INFO_TARGET_DEV", "INFO_TARGET_DEV"),
}

# Convenience aliases.
NKNIGHT = ORACLE_SCHEMAS["staging"]
EHRP = ORACLE_SCHEMAS["source"]
HISTDBA = ORACLE_SCHEMAS["production"]
INFO_TARGET_DEV = ORACLE_SCHEMAS["target_dev"]

# Spark JDBC driver class for Oracle.
ORACLE_JDBC_DRIVER = _env("ORACLE_JDBC_DRIVER", "oracle.jdbc.OracleDriver")

# --- File system locations ---------------------------------------------------
FILE_INPUT_DIR = _env("FILE_INPUT_DIR", "/data/BIISINT/data/int/in/")
FILE_OUTPUT_DIR = _env("FILE_OUTPUT_DIR", "/data/BIISINT/data/int/out/CPM/")
LOG_DIR = _env("LOG_DIR", "/home/sa-biisint/data/int/log")

# --- SFTP --------------------------------------------------------------------
SFTP_HOST = _env("SFTP_HOST", "m1csv301.hhs.gov")
SFTP_USER = _env("SFTP_USER", "sa-cdirect")
SFTP_PASSWORD = _env("SFTP_PASSWORD")
SFTP_PORT = int(_env("SFTP_PORT", "22"))


def jdbc_properties() -> dict:
    """Connection properties for ``spark.read.jdbc`` / ``DataFrame.write.jdbc``."""
    return {
        "user": ORACLE_USER,
        "password": ORACLE_PASSWORD,
        "driver": ORACLE_JDBC_DRIVER,
    }


def qualified(schema_key: str, table: str) -> str:
    """Return a ``SCHEMA.TABLE`` reference for the given logical schema key."""
    schema = ORACLE_SCHEMAS.get(schema_key, schema_key)
    return "%s.%s" % (schema, table) if schema else table
