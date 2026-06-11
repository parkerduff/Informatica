"""Unit tests for jobs/comptime.py."""
import pytest

from jobs import comptime
from utils.validation import ValidationError

COLS = [n for n, _ in comptime.INPUT_FIELDS]


def _row(ssn="900112233", bal="12.50", undef="0"):
    return (ssn, "SMITH,ALEX", "100001", "ORG1", "E", bal,
            "2026", "2026-06-13", "2026-06-11", "1.50", "8.00", undef)


def test_pp_year_num_derivation():
    assert comptime.derive_pp_year_num(2026, 3) == 202603
    assert comptime.derive_pp_year_num(2026, 26) == 202626


def test_csv_parsing_all_12_fields(spark, tmp_path):
    csv = tmp_path / "comptime.csv"
    csv.write_text(",".join(COLS) + "\n" + ",".join(_row()) + "\n")
    df = comptime.parse_comptime_csv(spark, str(csv))
    assert df.columns == COLS
    assert df.count() == 1


def test_ssn_hashing(spark):
    df = spark.createDataFrame([_row()], COLS)
    hashed = comptime.hash_ssn(df).collect()[0]["SSN"]
    assert hashed != "900112233"
    assert len(hashed) == 64  # SHA-256 hex digest


def test_malformed_numeric_becomes_null(spark):
    df = spark.createDataFrame([_row(bal="NOT_A_NUMBER")], COLS)
    out = comptime.coerce_numerics(df).collect()[0]
    assert out["COMP_TIME_CUR_BAL"] is None


def test_add_pay_period_stamps_all_rows(spark):
    df = spark.createDataFrame([_row(), _row(ssn="900112244")], COLS)
    out = comptime.add_pay_period(df, 2026, 3)
    vals = {(r["PP_END_YEAR"], r["PP_NUM"], r["PP_YEAR_NUM"]) for r in out.collect()}
    assert vals == {(2026, 3, 202603)}


def test_empty_file_raises_error(spark):
    empty = spark.createDataFrame([], comptime.INPUT_SCHEMA)
    with pytest.raises(ValidationError):
        comptime.transform(empty, 2026, 3)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
