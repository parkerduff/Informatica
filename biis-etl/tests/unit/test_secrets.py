import pytest

from utils import secrets


def test_load_config_test_env():
    config = secrets.load_config("test")
    assert config["database"]["name"] == "biis_test"
    assert config["env"] == "test"


def test_load_config_default_env(monkeypatch):
    monkeypatch.delenv("BIIS_ENV", raising=False)
    config = secrets.load_config()
    assert config["env"] == "test"


def test_get_secret_local():
    config = secrets.load_config("test")
    secret = secrets.get_secret("biis", config)
    assert secret["username"] == "sa"
    assert secret["password"]


def test_get_secret_aws(monkeypatch):
    config = secrets.load_config("test")
    config["secrets"] = {"provider": "aws", "region": "us-east-1"}

    class FakeClient:
        def get_secret_value(self, SecretId):
            return {"SecretString": '{"username": "u", "password": "p"}'}

    import types
    fake_boto3 = types.SimpleNamespace(client=lambda *a, **k: FakeClient())
    monkeypatch.setitem(__import__("sys").modules, "boto3", fake_boto3)
    secret = secrets.get_secret("biis", config)
    assert secret == {"username": "u", "password": "p"}


def test_get_secret_unknown_provider():
    config = secrets.load_config("test")
    config["secrets"] = {"provider": "vault"}
    with pytest.raises(ValueError):
        secrets.get_secret("biis", config)
