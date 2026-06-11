import hashlib

from jobs import comptime


def test_derive_pp_year_num():
    assert comptime.derive_pp_year_num(2026, 5) == 202605
    assert comptime.derive_pp_year_num(2026, 12) == 202612


def _input_df(spark):
    row = ("999000001", "EMPLOYEE 001", "ACCT1", "ORG1", "N", "12.50", "2026",
           "2026-06-13", "2026-06-01", "1.50", "4.25", "3")
    cols = ["SSN", "NAME", "CURRENT_ACCT", "CURRENT_ORG", "FLSA_STATUS",
            "COMP_TIME_CUR_BAL", "COMP_TIME_YEAR_EARNED", "PP_END_DATE",
            "DAILY_DATE_EARNED", "COMP_TIME_RATE", "COMP_TIME_HOURS",
            "COMP_TIME_UNDEF"]
    return spark.createDataFrame([row], cols)


def test_transform(spark):
    out = comptime.transform(_input_df(spark), {"pp_num": 12, "pp_end_year": 2026})
    row = out.collect()[0]
    assert str(row["PP_YEAR_NUM"]) == "202612"
    assert str(row["COMP_TIME_CUR_BAL"]) == "12.50"
    assert row["SSN_HASH"] == hashlib.sha256(b"999000001").hexdigest()
    assert row["PP_END_DATE"].strftime("%Y-%m-%d") == "2026-06-13"
    assert row["LOAD_DATE"] is not None
    assert out.columns[:3] == ["PP_END_YEAR", "PP_NUM", "PP_YEAR_NUM"]


def test_transform_decimal_scales(spark):
    out = comptime.transform(_input_df(spark), {"pp_num": 1, "pp_end_year": 2026})
    schema = {f.name: f.dataType.simpleString() for f in out.schema.fields}
    assert schema["COMP_TIME_CUR_BAL"] == "decimal(8,2)"
    assert schema["PP_NUM"] == "decimal(2,0)"
    assert schema["COMP_TIME_UNDEF"] == "decimal(6,0)"


def test_run_missing_columns(monkeypatch, spark, tmp_path):
    bad = tmp_path / "bad.csv"
    bad.write_text("SSN,NAME\n1,x\n")
    monkeypatch.setattr(comptime, "get_spark", lambda name: spark)
    monkeypatch.setattr(spark, "stop", lambda: None)
    monkeypatch.setattr(comptime, "get_current_pay_period",
                        lambda *a: {"pp_num": 12, "pp_end_year": 2026})
    sent = []
    monkeypatch.setattr(comptime, "send_notification", lambda s, b, c: sent.append(s))
    try:
        comptime.run("test", str(bad))
        raise AssertionError("expected ValueError")
    except ValueError:
        pass
    assert any("FAILED" in s for s in sent)
