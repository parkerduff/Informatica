"""Unit tests for config loading, secret resolution and notifications."""
import types

import pytest

from utils import notifications
from utils.secrets import get_db_secret, get_secret, load_config

pytestmark = pytest.mark.unit


def test_load_config_test_env():
    cfg = load_config("test")
    assert cfg.env == "test"
    assert cfg.database.name == "biis_test"
    assert cfg.secrets.provider == "local"


def test_load_config_unknown_env_raises():
    with pytest.raises(FileNotFoundError):
        load_config("does-not-exist")


def test_get_secret_local_database():
    cfg = load_config("test")
    sec = get_secret("database", cfg)
    assert sec["user"] == cfg.database.user
    assert get_db_secret(cfg).password == cfg.database.password


def test_get_secret_unknown_local_raises():
    cfg = load_config("test")
    with pytest.raises(KeyError):
        get_secret("nope", cfg)


def _notif_config(provider):
    notif = types.SimpleNamespace(provider=provider, recipients="a@x.com, b@x.com",
                                  sender=None, region="us-east-1",
                                  smtp_host=None, smtp_port=25)
    return types.SimpleNamespace(notifications=notif)


def test_send_notification_log_provider(caplog):
    import logging

    with caplog.at_level(logging.INFO):
        notifications.send_notification("subj", "body", _notif_config("log"))
    assert any("subj" in r.message for r in caplog.records)


def test_send_notification_unknown_provider_raises():
    with pytest.raises(ValueError):
        notifications.send_notification("s", "b", _notif_config("carrier-pigeon"))
