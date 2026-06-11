"""Environment configuration for the BIIS ETL jobs.

Two backends are supported:

* ``test``  -> a self-contained SQLite database + local landing directory.
  This requires no external services (no Docker, no SQL Server, no ODBC
  driver) so the full ``build -> run -> test -> validate`` loop runs anywhere.
* ``prod``  -> SQL Server reached over JDBC (Spark) / pyodbc (assertions).

The active environment is selected with the ``--env`` CLI flag on every job
and defaults to ``test``.
"""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Any, Dict


REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _test_root() -> str:
    return os.environ.get("BIIS_TEST_ROOT", os.path.join(REPO_ROOT, ".biis-test"))


@dataclass
class Config:
    env: str
    database: Dict[str, Any]
    paths: Dict[str, str]
    secrets: Dict[str, Any] = field(default_factory=dict)
    notifications: Dict[str, Any] = field(default_factory=dict)

    @property
    def backend(self) -> str:
        return self.database["backend"]

    @property
    def sqlite_path(self) -> str:
        return self.database["sqlite_path"]

    def as_dict(self) -> Dict[str, Any]:
        return {
            "env": self.env,
            "database": self.database,
            "paths": self.paths,
            "secrets": self.secrets,
            "notifications": self.notifications,
        }


def get_config(env: str = "test") -> Config:
    """Return the :class:`Config` for ``env`` (``test`` or ``prod``)."""
    if env == "test":
        root = _test_root()
        return Config(
            env="test",
            database={
                "backend": "sqlite",
                "sqlite_path": os.path.join(root, "biis_test.db"),
                "schema": "dbo",
            },
            paths={
                "landing": os.path.join(root, "landing"),
                "outbound": os.path.join(root, "outbound"),
                "archive": os.path.join(root, "archive"),
            },
            secrets={"provider": "local"},
            notifications={"provider": "mock"},
        )
    if env == "prod":
        return Config(
            env="prod",
            database={
                "backend": "sqlserver",
                "host": os.environ.get("BIIS_DB_HOST", "localhost"),
                "port": int(os.environ.get("BIIS_DB_PORT", "1433")),
                "name": os.environ.get("BIIS_DB_NAME", "biis"),
                "schema": os.environ.get("BIIS_DB_SCHEMA", "dbo"),
                "jdbc_jar": os.environ.get(
                    "BIIS_JDBC_JAR",
                    os.path.join(REPO_ROOT, "jars", "mssql-jdbc-12.4.2.jre11.jar"),
                ),
            },
            paths={
                "landing": os.environ.get("BIIS_LANDING", "/data/BIISINT/data/int/in"),
                "outbound": os.environ.get("BIIS_OUTBOUND", "/data/BIISINT/data/int/out"),
                "archive": os.environ.get("BIIS_ARCHIVE", "/data/BIISINT/data/int/archive"),
            },
            secrets={"provider": "aws"},
            notifications={"provider": "smtp"},
        )
    raise ValueError(f"Unknown env: {env!r} (expected 'test' or 'prod')")
