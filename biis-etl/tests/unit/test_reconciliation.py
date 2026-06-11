"""Unit tests for the reconciliation engine's pure pieces."""
import pytest

from utils import reconciliation as R

pytestmark = pytest.mark.unit


def test_module_registry_points_at_existing_golden(golden_dir):
    for module, specs in R.MODULE_TABLES.items():
        for table, keys, golden_file in specs:
            assert keys, f"{table} needs key columns"
            assert (golden_dir / golden_file).exists(), golden_file


def test_normalize_collapses_numeric_and_trims_strings(spark):
    df = spark.createDataFrame([("  hi  ", "1.5000000000")], ["s", "n"])
    df = df.withColumn("n", df["n"].cast("double"))
    out = R._normalize(df, ["s", "n"]).collect()[0]
    assert out["s"] == "hi"
    assert float(out["n"]) == 1.5


def test_read_golden_is_all_strings(spark, golden_dir):
    df = R._read_golden(spark, str(golden_dir / "comptime_expected_comp_time_daily_tbl.csv"))
    assert all(t == "string" for _, t in df.dtypes)
    assert df.count() == 100


def test_generate_report_writes_json_and_html(tmp_path):
    res = [R.ReconciliationResult(table="T", module="m", expected_count=1,
                                  actual_count=1, row_count_match=True,
                                  schema_match=True, diff_count=0, passed=True)]
    json_path = R.generate_report(res, str(tmp_path))
    assert json_path.endswith(".json")
    import json
    payload = json.loads(open(json_path).read())
    assert payload["passed"] == 1 and payload["failed"] == 0
    assert (tmp_path / "reconciliation.html").exists()
