"""Unit tests for the FDA leave router logic (build_errors) via stubbed reads."""
import pytest

from jobs import fda_leave

pytestmark = pytest.mark.unit


def test_build_errors_flags_missing_lookups(spark, monkeypatch):
    tatran = spark.createDataFrame(
        [("B1", "TK1", "E1", "2026", "12"),
         ("B1", "TK2", "E2", "2026", "12"),
         ("B1", "TK3", "E3", "2026", "12")],
        ["FDA_BATCH_ID", "FDA_TK_NO", "FDA_EMP_ID", "FDA_PP_YEAR", "FDA_PP_NUM"],
    )
    # E1 present everywhere; E2 missing MER; E3 missing YTD+PAD.
    ytd = spark.createDataFrame([("E1",), ("E2",)], ["DYD_SSN_1"])
    pad = spark.createDataFrame([("E1",), ("E2",)], ["PAD_SOC_SEC_NO"])
    mer = spark.createDataFrame([("E1",), ("E3",)], ["MER_SSN"])

    tables = {
        "HI_PM_FDA_TATRAN_TBL": tatran,
        "CPM_YTD_DETAIL_STG_TBL": ytd,
        "CPM_PAD_DETAIL_STG_TBL": pad,
        "CPM_MER_DETAIL_STG_TBL": mer,
    }

    def fake_read(spark_, table, config, secret, columns=None):
        return tables[table]

    monkeypatch.setattr(fda_leave, "read_table", fake_read)

    errors = {r["FDA_EMP_ID"]: r["ERROR_MESSAGE"]
              for r in fda_leave.build_errors(spark, config=None, secret=None).collect()}
    assert "E1" not in errors
    assert "MER" in errors["E2"]
    assert "YTD" in errors["E3"] and "PAD" in errors["E3"]
