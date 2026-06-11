import os

from utils import reconciliation


def _write_csv(path, header, rows):
    with open(path, "w") as f:
        f.write(header + "\n")
        for r in rows:
            f.write(r + "\n")


def test_normalized_decimal_and_empty(spark):
    from decimal import Decimal

    df = spark.createDataFrame(
        [(Decimal("12.50"), " x ", "")],
        "amt decimal(8,2), s string, e string",
    )
    out = reconciliation._normalized(df, ["s"]).collect()[0]
    assert out["amt"] == "12.50"
    assert out["s"] == "x"
    assert out["e"] is None


def test_reconcile_table_pass(monkeypatch, spark, tmp_path, config):
    actual = spark.createDataFrame([(1, "a"), (2, "b")], ["ID", "VAL"])
    monkeypatch.setattr(reconciliation, "reconcile_table", reconciliation.reconcile_table)
    from utils import db as dbutil
    monkeypatch.setattr(dbutil, "read_table", lambda *a, **k: actual)

    golden = tmp_path / "g.csv"
    _write_csv(str(golden), "ID,VAL", ["1,a", "2,b"])
    res = reconciliation.reconcile_table(spark, config, "T", ["ID"], str(golden), secret={})
    assert res.passed and res.diff_count == 0


def test_reconcile_table_diff(monkeypatch, spark, tmp_path, config):
    actual = spark.createDataFrame([(1, "a"), (2, "X")], ["ID", "VAL"])
    from utils import db as dbutil
    monkeypatch.setattr(dbutil, "read_table", lambda *a, **k: actual)

    golden = tmp_path / "g.csv"
    _write_csv(str(golden), "ID,VAL", ["1,a", "2,b", "3,c"])
    res = reconciliation.reconcile_table(spark, config, "T", ["ID"], str(golden), secret={})
    assert not res.passed
    assert res.diff_count == 2  # value mismatch on 2, missing row 3
    assert res.column_diffs.get("VAL")
    assert res.diff_sample


def test_reconcile_table_schema_mismatch(monkeypatch, spark, tmp_path, config):
    actual = spark.createDataFrame([(1,)], ["ID"])
    from utils import db as dbutil
    monkeypatch.setattr(dbutil, "read_table", lambda *a, **k: actual)

    golden = tmp_path / "g.csv"
    _write_csv(str(golden), "ID,MISSING_COL", ["1,x"])
    res = reconciliation.reconcile_table(spark, config, "T", ["ID"], str(golden), secret={})
    assert not res.passed and not res.schema_match


def test_reconcile_table_ignore_columns(monkeypatch, spark, tmp_path, config):
    actual = spark.createDataFrame([(1, "a", "noise1")], ["ID", "VAL", "LOAD_DATE"])
    from utils import db as dbutil
    monkeypatch.setattr(dbutil, "read_table", lambda *a, **k: actual)

    golden = tmp_path / "g.csv"
    _write_csv(str(golden), "ID,VAL,LOAD_DATE", ["1,a,noise2"])
    res = reconciliation.reconcile_table(
        spark, config, "T", ["ID"], str(golden), secret={}, ignore_columns=["LOAD_DATE"])
    assert res.passed


def test_reconcile_module_and_report(monkeypatch, spark, tmp_path, config):
    actual = spark.createDataFrame([(1, "a")], ["ID", "VAL"])
    from utils import db as dbutil
    monkeypatch.setattr(dbutil, "read_table", lambda *a, **k: actual)
    monkeypatch.setattr("utils.secrets.get_secret", lambda *a, **k: {})

    golden = tmp_path / "t_golden.csv"
    _write_csv(str(golden), "ID,VAL", ["1,a"])
    results = reconciliation.reconcile_module(
        spark, config, "mod", str(tmp_path),
        [{"table": "T", "key_columns": ["ID"], "golden_file": "t_golden.csv"}])
    assert results[0].passed

    report_dir = str(tmp_path / "reports")
    reconciliation.generate_report(results, report_dir)
    assert os.path.exists(os.path.join(report_dir, "reconciliation.json"))
    assert os.path.exists(os.path.join(report_dir, "reconciliation.html"))
