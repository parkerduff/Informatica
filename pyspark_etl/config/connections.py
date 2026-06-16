"""Connection and path configuration.

Replaces the credentials that the original ksh scripts read from
``$HOME/.use`` / ``$HOME/.pw`` files and the hard-coded paths in ``bin/SETENV``.
All secrets are sourced from environment variables so nothing sensitive lives in
source control. Use a ``.env`` file (loaded via ``python-dotenv``) for local
development or a real secrets manager in production.

Original Informatica DBD names map to logical connections here:
    ORA_BIIS        -> target warehouse (BIIS, schemas HISTDBA / INFO_TARGET_DEV)
    ORA_BIISPRD_SRC -> source system   (EHRP schema, NKNIGHT staging schema)
"""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Optional

try:  # optional; only needed for local .env loading
    from dotenv import load_dotenv

    load_dotenv()
except Exception:  # pragma: no cover - dotenv is optional
    pass


def _env(name: str, default: Optional[str] = None) -> Optional[str]:
    return os.environ.get(name, default)


@dataclass(frozen=True)
class OracleConnection:
    """JDBC connection parameters for an Oracle database."""

    name: str
    host: str
    port: int
    service_name: str
    user: str
    password: str
    driver: str = "oracle.jdbc.OracleDriver"

    @property
    def jdbc_url(self) -> str:
        return f"jdbc:oracle:thin:@//{self.host}:{self.port}/{self.service_name}"

    def dsn(self) -> str:
        """python-oracledb / cx_Oracle Easy Connect string (no jdbc prefix)."""
        return f"{self.host}:{self.port}/{self.service_name}"


# --- Oracle connections -------------------------------------------------------
# Source: EHRP production (PS_GVT_JOB lives in schema EHRP, staging in NKNIGHT).
ORA_BIISPRD_SRC = OracleConnection(
    name="ORA_BIISPRD_SRC",
    host=_env("ORA_BIISPRD_SRC_HOST", "biisprd-src.hhs.gov"),
    port=int(_env("ORA_BIISPRD_SRC_PORT", "1521")),
    service_name=_env("ORA_BIISPRD_SRC_SERVICE", "BIISPRD"),
    user=_env("ORA_BIISPRD_SRC_USER", ""),
    password=_env("ORA_BIISPRD_SRC_PASSWORD", ""),
)

# Target: BIIS warehouse (HISTDBA production, INFO_TARGET_DEV for PseudoSSN/CPM).
ORA_BIIS = OracleConnection(
    name="ORA_BIIS",
    host=_env("ORA_BIIS_HOST", "biis.hhs.gov"),
    port=int(_env("ORA_BIIS_PORT", "1521")),
    service_name=_env("ORA_BIIS_SERVICE", "BIIS"),
    user=_env("ORA_BIIS_USER", ""),
    password=_env("ORA_BIIS_PASSWORD", ""),
)

CONNECTIONS = {c.name: c for c in (ORA_BIISPRD_SRC, ORA_BIIS)}


def get_connection(name: str) -> OracleConnection:
    try:
        return CONNECTIONS[name]
    except KeyError as exc:  # pragma: no cover - defensive
        raise KeyError(
            f"Unknown connection {name!r}; known: {sorted(CONNECTIONS)}"
        ) from exc


# --- Oracle schema names (owners in the Informatica metadata) -----------------
SCHEMA_EHRP = _env("SCHEMA_EHRP", "EHRP")
SCHEMA_NKNIGHT = _env("SCHEMA_NKNIGHT", "NKNIGHT")
SCHEMA_HISTDBA = _env("SCHEMA_HISTDBA", "HISTDBA")
SCHEMA_INFO_TARGET = _env("SCHEMA_INFO_TARGET", "INFO_TARGET_DEV")


@dataclass(frozen=True)
class SftpHost:
    host: str
    user: str
    # Either a private key path or a password (key preferred).
    key_path: Optional[str] = None
    password: Optional[str] = None
    port: int = 22


# Central dropbox used by every transfer script (sa-cdirect@m1csv301.hhs.gov).
SFTP_DROPBOX = SftpHost(
    host=_env("SFTP_HOST", "m1csv301.hhs.gov"),
    user=_env("SFTP_USER", "sa-cdirect"),
    key_path=_env("SFTP_KEY_PATH"),
    password=_env("SFTP_PASSWORD"),
    port=int(_env("SFTP_PORT", "22")),
)


@dataclass(frozen=True)
class Paths:
    """Filesystem layout (from bin/SETENV and the ksh scripts)."""

    input_dir: str = field(default_factory=lambda: _env("BIIS_INPUT_DIR", "/data/BIISINT/data/int/in/"))
    cpm_output_dir: str = field(default_factory=lambda: _env("BIIS_CPM_OUT_DIR", "/data/BIISINT/data/int/out/CPM/"))
    les_output_dir: str = field(default_factory=lambda: _env("BIIS_LES_OUT_DIR", "/data/BIISINT/data/int/out/LES/"))
    log_dir: str = field(default_factory=lambda: _env("BIIS_LOG_DIR", "/home/sa-biisint/data/int/log"))
    home_dir: str = field(default_factory=lambda: _env("BIIS_HOME", "/data/BIISINT/bin/EHRP2BIIS"))


PATHS = Paths()
