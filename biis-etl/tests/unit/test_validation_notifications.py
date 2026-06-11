import pytest

from utils import notifications, validation


class FakeCursor:
    def __init__(self):
        self.calls = []

    def execute(self, sql, *params):
        self.calls.append((sql, params))
        return self


class FakeConn:
    def __init__(self):
        self.cur = FakeCursor()

    def cursor(self):
        return self.cur


def test_validate_row_count_ok(spark):
    df = spark.createDataFrame([(1,), (2,)], ["x"])
    assert validation.validate_row_count(df, expected_min=1, expected_max=5) == 2


def test_validate_row_count_too_few(spark):
    df = spark.createDataFrame([(1,)], ["x"])
    with pytest.raises(validation.ValidationError):
        validation.validate_row_count(df, expected_min=2)


def test_validate_row_count_too_many(spark):
    df = spark.createDataFrame([(1,), (2,)], ["x"])
    with pytest.raises(validation.ValidationError):
        validation.validate_row_count(df, expected_max=1)


def test_validate_schema(spark):
    df = spark.createDataFrame([(1, "a")], ["x", "y"])
    validation.validate_schema(df, ["x", "y"])
    with pytest.raises(validation.ValidationError):
        validation.validate_schema(df, ["x", "z"])


def test_validate_no_nulls(spark):
    df = spark.createDataFrame([(1, None)], "x int, y string")
    validation.validate_no_nulls(df, ["x"])
    with pytest.raises(validation.ValidationError):
        validation.validate_no_nulls(df, ["y"])


def test_log_row_count():
    conn = FakeConn()
    validation.log_row_count(conn, "T", "PROC", 5, {"pp_end_year": 2026, "pp_num": 12})
    sql, params = conn.cur.calls[0]
    assert "INSERT INTO COUNTER_TBL" in sql
    assert params[:3] == ("PROC", "Rows loaded into T", 5)
    assert params[3:] == (2026, 12, 1)


def test_send_notification_log(caplog):
    notifications.send_notification("subj", "body", {"notifications": {"provider": "log"}})


def test_send_notification_unknown():
    with pytest.raises(ValueError):
        notifications.send_notification("s", "b", {"notifications": {"provider": "pigeon"}})


def test_send_notification_ses(monkeypatch):
    sent = {}

    class FakeSES:
        def send_email(self, **kwargs):
            sent.update(kwargs)

    import sys
    import types
    monkeypatch.setitem(sys.modules, "boto3",
                        types.SimpleNamespace(client=lambda *a, **k: FakeSES()))
    notifications.send_notification(
        "s", "b", {"notifications": {"provider": "ses", "recipients": "a@x.gov,b@x.gov"}})
    assert sent["Destination"]["ToAddresses"] == ["a@x.gov", "b@x.gov"]


def test_send_notification_smtp(monkeypatch):
    sent = []

    class FakeSMTP:
        def __init__(self, *a, **k):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

        def send_message(self, msg):
            sent.append(msg)

    monkeypatch.setattr(notifications.smtplib, "SMTP", FakeSMTP)
    notifications.send_notification(
        "s", "b", {"notifications": {"provider": "smtp", "recipients": "ops@x.gov"}})
    assert sent and sent[0]["To"] == "ops@x.gov"
