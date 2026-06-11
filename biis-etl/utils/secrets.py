"""Configuration loading and secret resolution.

Configuration is environment-driven (config/{env}.yaml). Secrets are resolved
either from the local config file (``provider: local``) for dev/test or from
AWS Secrets Manager (``provider: aws``) for prod. Credentials are never
hard-coded in source.
"""
from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, Optional

import yaml

CONFIG_DIR = Path(__file__).resolve().parents[1] / "config"


@dataclass
class DatabaseConfig:
    driver: str
    host: str
    port: int
    name: str
    schema: str
    user: Optional[str] = None
    password: Optional[str] = None


@dataclass
class SecretsConfig:
    provider: str = "local"
    region: str = "us-east-1"
    db_secret_name: Optional[str] = None
    sftp_secret_name: Optional[str] = None


@dataclass
class NotificationsConfig:
    provider: str = "log"
    recipients: str = ""
    sender: Optional[str] = None
    region: str = "us-east-1"
    smtp_host: Optional[str] = None
    smtp_port: int = 25


@dataclass
class PathsConfig:
    landing: str
    staging: str
    archive: str


@dataclass
class SftpConfig:
    host: str
    port: int = 22
    user: Optional[str] = None
    password: Optional[str] = None
    routes: Dict[str, str] = field(default_factory=dict)


@dataclass
class Config:
    env: str
    database: DatabaseConfig
    secrets: SecretsConfig
    notifications: NotificationsConfig
    paths: PathsConfig
    sftp: SftpConfig


@dataclass
class DbSecret:
    """Resolved database credentials."""
    user: str
    password: str


def load_config(env: Optional[str] = None) -> Config:
    """Load ``config/{env}.yaml``. ``env`` defaults to ``$BIIS_ENV`` or ``test``."""
    env = env or os.environ.get("BIIS_ENV", "test")
    path = CONFIG_DIR / f"{env}.yaml"
    if not path.exists():
        raise FileNotFoundError(f"No config for env={env!r} at {path}")
    raw = yaml.safe_load(path.read_text()) or {}

    db = raw["database"]
    sec = raw.get("secrets", {})
    notif = raw.get("notifications", {})
    paths = raw["paths"]
    sftp = raw.get("sftp", {})

    return Config(
        env=env,
        database=DatabaseConfig(
            driver=db["driver"], host=db["host"], port=int(db["port"]),
            name=db["name"], schema=db.get("schema", "dbo"),
            user=db.get("user"), password=db.get("password"),
        ),
        secrets=SecretsConfig(
            provider=sec.get("provider", "local"),
            region=sec.get("region", "us-east-1"),
            db_secret_name=sec.get("db_secret_name"),
            sftp_secret_name=sec.get("sftp_secret_name"),
        ),
        notifications=NotificationsConfig(
            provider=notif.get("provider", "log"),
            recipients=notif.get("recipients", ""),
            sender=notif.get("sender"),
            region=notif.get("region", "us-east-1"),
            smtp_host=notif.get("smtp_host"),
            smtp_port=int(notif.get("smtp_port", 25)),
        ),
        paths=PathsConfig(
            landing=paths["landing"], staging=paths["staging"], archive=paths["archive"],
        ),
        sftp=SftpConfig(
            host=sftp.get("host", "localhost"), port=int(sftp.get("port", 22)),
            user=sftp.get("user"), password=sftp.get("password"),
            routes=dict(sftp.get("routes", {})),
        ),
    )


def _aws_secret(secret_name: str, region: str) -> Dict[str, str]:  # pragma: no cover
    import boto3  # imported lazily so local/test runs need no AWS deps

    client = boto3.client("secretsmanager", region_name=region)
    resp = client.get_secret_value(SecretId=secret_name)
    return json.loads(resp["SecretString"])


def get_secret(secret_name: str, config: Config) -> Dict[str, str]:
    """Return a secret as a dict of key/value pairs.

    For ``provider: local`` the ``secret_name`` selects a section of the loaded
    config (``database`` or ``sftp``); for ``provider: aws`` it is looked up in
    AWS Secrets Manager.
    """
    if config.secrets.provider == "local":
        if secret_name in ("database", "db", config.database.name):
            return {"user": config.database.user or "", "password": config.database.password or ""}
        if secret_name in ("sftp",):
            return {"user": config.sftp.user or "", "password": config.sftp.password or ""}
        raise KeyError(f"Unknown local secret {secret_name!r}")
    return _aws_secret(secret_name, config.secrets.region)  # pragma: no cover


def get_db_secret(config: Config) -> DbSecret:
    """Resolve database credentials for the active provider."""
    if config.secrets.provider == "local":
        return DbSecret(user=config.database.user or "", password=config.database.password or "")
    name = config.secrets.db_secret_name or "biis/db"  # pragma: no cover
    data = _aws_secret(name, config.secrets.region)  # pragma: no cover
    return DbSecret(user=data["username"], password=data["password"])  # pragma: no cover
