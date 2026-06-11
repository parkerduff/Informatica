"""Configuration loading for the BIIS ETL migration.

Loads the YAML config and exposes connection strings for both pyodbc
(used for DDL / direct SQL) and JDBC (used by PySpark ``DataFrameReader.jdbc``).
"""
from __future__ import annotations

import os
from typing import Any, Dict

import yaml

DEFAULT_CONFIG_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "config")


def load_config(env: str = "test", config_dir: str | None = None) -> Dict[str, Any]:
    """Load ``<config_dir>/<env>.yaml`` and return it as a dict."""
    config_dir = config_dir or DEFAULT_CONFIG_DIR
    path = os.path.join(config_dir, f"{env}.yaml")
    if not os.path.exists(path):
        raise FileNotFoundError(f"Config file not found: {path}")
    with open(path, "r", encoding="utf-8") as fh:
        return yaml.safe_load(fh)


def get_db_config(config: Dict[str, Any]) -> Dict[str, Any]:
    return config["database"]


def get_pyodbc_connection_string(config: Dict[str, Any]) -> str:
    """Build a pyodbc connection string for SQL Server.

    ``master`` is targeted when the configured database does not yet exist
    (e.g. while running the DDL that creates it).
    """
    db = get_db_config(config)
    trust = "yes" if db.get("trust_cert") else "no"
    return (
        f"DRIVER={db['driver']};"
        f"SERVER={db['host']},{db['port']};"
        f"DATABASE={db['name']};"
        f"UID={db['user']};"
        f"PWD={db['password']};"
        f"TrustServerCertificate={trust};"
        f"Encrypt={trust};"
    )


def get_master_connection_string(config: Dict[str, Any]) -> str:
    """pyodbc connection string pointed at the ``master`` database."""
    db = get_db_config(config)
    trust = "yes" if db.get("trust_cert") else "no"
    return (
        f"DRIVER={db['driver']};"
        f"SERVER={db['host']},{db['port']};"
        f"DATABASE=master;"
        f"UID={db['user']};"
        f"PWD={db['password']};"
        f"TrustServerCertificate={trust};"
        f"Encrypt={trust};"
    )


def get_jdbc_url(config: Dict[str, Any]) -> str:
    db = get_db_config(config)
    trust = "true" if db.get("trust_cert") else "false"
    return (
        f"jdbc:sqlserver://{db['host']}:{db['port']};"
        f"databaseName={db['name']};"
        f"encrypt={trust};"
        f"trustServerCertificate={trust};"
    )


def get_jdbc_properties(config: Dict[str, Any]) -> Dict[str, str]:
    db = get_db_config(config)
    return {
        "user": db["user"],
        "password": db["password"],
        "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
    }
