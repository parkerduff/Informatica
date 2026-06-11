from jobs import pseudossn


def _make_line(values: dict) -> str:
    buf = [" "] * 500
    for f in pseudossn.file_layout():
        raw = values.get(f["name"])
        if raw is None:
            continue
        text = str(raw)[:f["length"]].ljust(f["length"])
        buf[f["offset"]:f["offset"] + f["length"]] = list(text)
    return "".join(buf)


def _write_file(tmp_path, records):
    header = "H" + " " * 499
    trailer = "T" + " " * 499
    lines = [header] + [_make_line(r) for r in records] + [trailer]
    path = tmp_path / "pseudossn_input.dat"
    path.write_text("\n".join(lines) + "\n")
    return str(path)


def test_file_layout_excludes_fillers():
    names = [f["name"] for f in pseudossn.file_layout()]
    assert not any(n.startswith("FILLER") for n in names)
    assert "PSEUDO_SSN" in names


def test_parse_fixed_width(spark, tmp_path):
    path = _write_file(tmp_path, [{"SSN": "900000001", "PSEUDO_SSN": "800000001"}])
    df = pseudossn.parse_fixed_width(spark, path)
    rows = {r["REC_TYPE"]: r for r in df.collect()}
    assert set(rows) == {"H", "T", "D"}
    assert rows["D"]["SSN"] == "900000001"
    assert rows["D"]["CAN_CD"] is None  # blanks become NULL


def test_convert_date(spark):
    from pyspark.sql import functions as F

    df = spark.createDataFrame(
        [("20260110",), ("06151995",), ("00000000",), ("",)], ["d"])
    out = df.select(pseudossn.convert_date(F.col("d")).alias("ts")).collect()
    assert out[0]["ts"].strftime("%Y-%m-%d") == "2026-01-10"
    assert out[1]["ts"].strftime("%Y-%m-%d") == "1995-06-15"
    assert out[2]["ts"] is None
    assert out[3]["ts"] is None


def test_parse_signed_decimal(spark):
    from pyspark.sql import functions as F

    df = spark.createDataFrame([("01250+",), ("01250-",), ("01250",), ("",)], ["v"])
    out = df.select(pseudossn.parse_signed_decimal(F.col("v"), 2).alias("d")).collect()
    assert str(out[0]["d"]) == "12.50"
    assert str(out[1]["d"]) == "-12.50"
    assert str(out[2]["d"]) == "12.50"
    assert out[3]["d"] is None


def test_transform_details_dedup(spark, tmp_path):
    records = [
        {"SSN": "900000001", "PSEUDO_SSN": "800000001",
         "EFFECTIVE_DATE": "20260110", "EFFECTIVE_SEQ": "001"},
        {"SSN": "900000002", "PSEUDO_SSN": "800000001",
         "EFFECTIVE_DATE": "20251210", "EFFECTIVE_SEQ": "001"},
        {"SSN": "900000003", "PSEUDO_SSN": "800000002",
         "EFFECTIVE_DATE": "20260111", "EFFECTIVE_SEQ": "002"},
    ]
    path = _write_file(tmp_path, records)
    df = pseudossn.parse_fixed_width(spark, path)
    ordered, deduped = pseudossn.transform_details(df, {"pp_num": 12, "pp_end_year": 2026})
    assert ordered.count() == 3
    assert deduped.count() == 2
    kept = {r["PSEUDOSSN"]: r for r in deduped.collect()}
    assert kept["800000001"]["EFFECTIVE_DATE"].strftime("%Y%m%d") == "20260110"
    assert ordered.columns == pseudossn.table_columns()


def test_run(monkeypatch, spark, tmp_path):
    path = _write_file(tmp_path, [
        {"SSN": "900000001", "PSEUDO_SSN": "800000001",
         "EFFECTIVE_DATE": "20260110", "EFFECTIVE_SEQ": "001"}])
    monkeypatch.setattr(pseudossn, "get_spark", lambda name: spark)
    monkeypatch.setattr(spark, "stop", lambda: None)
    monkeypatch.setattr(pseudossn, "get_current_pay_period",
                        lambda *a: {"pp_num": 12, "pp_end_year": 2026})
    written = []
    monkeypatch.setattr(pseudossn, "write_table",
                        lambda df, table, cfg, sec, mode: written.append(table))

    import contextlib

    class FakeConn:
        def commit(self):
            pass

    @contextlib.contextmanager
    def fake_conn(*a, **k):
        yield FakeConn()

    monkeypatch.setattr(pseudossn, "pyodbc_connection", fake_conn)
    monkeypatch.setattr(pseudossn, "log_row_count", lambda *a: None)
    monkeypatch.setattr(pseudossn, "send_notification", lambda *a: None)
    result = pseudossn.run("test", path)
    assert result == {"all": 1, "deduped": 1}
    assert written == ["PSEUDOSSN_FROM_SDA_TBL", "PSEUDOSSN_TBL", "HI_ARCH_PSEUDOSSN_TBL"]
