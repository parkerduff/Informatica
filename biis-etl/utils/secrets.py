"""Configuration and secret loading."""
import json
import os
from typing import Optional

import yaml

CONFIG_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "config")


def load_config(env: Optional[str] = None) -> dict:
    env = env or os.environ.get("BIIS_ENV", "test")
    path = os.path.join(CONFIG_DIR, f"{env}.yaml")
    with open(path) as f:
        config = yaml.safe_load(f)
    config["env"] = env
    return config


def get_secret(secret_name: str, config: dict) -> dict:
    provider = config.get("secrets", {}).get("provider", "local")
    if provider == "local":
        db = config["database"]
        return {"username": db["user"], "password": db["password"]}
    if provider == "aws":
        import boto3

        client = boto3.client("secretsmanager", region_name=config["secrets"].get("region", "us-east-1"))
        resp = client.get_secret_value(SecretId=secret_name)
        return json.loads(resp["SecretString"])
    raise ValueError(f"Unknown secrets provider: {provider}")
