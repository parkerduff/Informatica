"""Unit tests for the CPM agency transform + fixed-width output rules."""
import pytest

from jobs.common import build_record_text
from jobs.cpm import cpm_common

pytestmark = pytest.mark.unit


def test_transform_builds_record_text_matching_pure_rule(spark):
    df = spark.createDataFrame(
        [(2026, 12, "999000001", "1001.01", "950.00")],
        ["PP_END_YEAR", "PP_NUM", "SOC_SEC_NO", "ADJ_GROSS_PAY", "ADJ_NET_PAY"],
    )
    row = cpm_common.transform(df, "NIH").collect()[0]
    assert row["AGENCY_CD"] == "NIH"
    assert row["SSN"] == "999000001"
    assert row["RECORD_TEXT"] == build_record_text("999000001", "NIH", "1001.01", "950.00")
    assert len(row["RECORD_TEXT"]) == 9 + 3 + 11 + 11


def test_staging_columns_present(spark):
    df = spark.createDataFrame(
        [(2026, 12, "999000002", "10.00", "9.00")],
        ["PP_END_YEAR", "PP_NUM", "SOC_SEC_NO", "ADJ_GROSS_PAY", "ADJ_NET_PAY"],
    )
    out = cpm_common.transform(df, "OIG")
    assert out.columns == cpm_common.STAGING_COLS


def test_write_flat_file_has_header_detail_trailer(tmp_path):
    records = [{"RECORD_TEXT": "REC1"}, {"RECORD_TEXT": "REC2"}]
    path = cpm_common.write_flat_file(records, "cdc", {"pp_num": 12, "pp_end_year": 2026},
                                      str(tmp_path))
    lines = open(path).read().splitlines()
    assert lines[0].startswith("H")
    assert lines[1:3] == ["REC1", "REC2"]
    assert lines[-1] == "T" + f"{2:09d}"
