"""Secret resolution.

The legacy ksh scripts read DB credentials from ``$HOME/.use`` / ``$HOME/.pw``
files.  This module replaces that with a pluggable provider:

* ``local`` -> read from environment variables, falling back to safe test
  defaults.  Used by the ``test`` environment.
* ``aws``   -> read from AWS Secrets Manager (boto3).  Used in production.
"""
from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:  # pragma: no cover
    from utils.config import Config

import os

_TEST_DEFAULTS = {
    "db_user": "sa",
    "db_password": "TestP@ssw0rd!",
}


def get_secret(name: str, config: "Config") -> str:
    """Return the secret value for ``name`` using the configured provider."""
    provider = config.secrets.get("provider", "local")
    if provider == "local":
        env_key = f"BIIS_SECRET_{name.upper()}"
        if env_key in os.environ:
            return os.environ[env_key]
        if name in _TEST_DEFAULTS:
            return _TEST_DEFAULTS[name]
        raise KeyError(f"Secret {name!r} not found (env {env_key} unset)")
    if provider == "aws":  # pragma: no cover - requires AWS
        import boto3

        secret_id = config.secrets.get("secret_id", "biis/etl")
        client = boto3.client("secretsmanager")
        import json

        payload = json.loads(client.get_secret_value(SecretId=secret_id)["SecretString"])
        return payload[name]
    raise ValueError(f"Unknown secrets provider: {provider!r}")
