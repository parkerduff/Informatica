"""Unit tests for jobs/cpm/cpm_common.py."""
import pytest

from jobs.cpm import cpm_common
from utils.schemas import column_names

FILTER_COLS = ["BUSINESS_UNIT", "ORG_CDE", "MP_POOL_DES",
               "PP_END_YEAR", "PP_NUM", "DFAS_PSEUDO_SSN"]


def _newpay(spark, rows):
    return spark.createDataFrame(rows, FILTER_COLS)


def test_501_field_schema_fidelity():
    assert len(column_names("CPM_NEWPAY_TBL")) == 501


def test_agency_filter_nih(spark):
    df = _newpay(spark, [
        ("NIH00", "", "", 2026, 26, "900000001"),
        ("OIG00", "", "", 2026, 26, "900000002"),
        ("NIH00", "", "X", 2026, 26, "900000003"),  # excluded: MP_POOL_DES set
    ])
    out = cpm_common.agency_filter(df, "NIH").collect()
    assert {r["DFAS_PSEUDO_SSN"] for r in out} == {"900000001"}


def test_agency_filter_oig(spark):
    df = _newpay(spark, [
        ("OIG00", "", "", 2026, 26, "900000010"),
        ("NIH00", "", "", 2026, 26, "900000011"),
    ])
    out = cpm_common.agency_filter(df, "OIG").collect()
    assert {r["DFAS_PSEUDO_SSN"] for r in out} == {"900000010"}


def test_agency_filter_cdc_business_unit_and_org(spark):
    df = _newpay(spark, [
        ("CDC00", "", "", 2026, 26, "900000020"),
        ("ATSDR", "", "", 2026, 26, "900000021"),
        ("ZZZ00", "ANC34", "", 2026, 26, "900000022"),  # org-code route
        ("ZZZ00", "OTHER", "", 2026, 26, "900000023"),  # excluded
    ])
    out = cpm_common.agency_filter(df, "CDC").collect()
    assert {r["DFAS_PSEUDO_SSN"] for r in out} == {"900000020", "900000021", "900000022"}


def test_agency_filter_unknown_raises(spark):
    df = _newpay(spark, [("NIH00", "", "", 2026, 26, "900000001")])
    with pytest.raises(ValueError):
        cpm_common.agency_filter(df, "BOGUS")


def test_signed_decimal_output_format():
    assert cpm_common.format_overpunch(-12340, 5) == "1234}"
    assert cpm_common.format_overpunch(12340, 5) == "1234{"
    assert cpm_common.format_overpunch(1231, 4) == "123A"


def test_to_staging_projects_schema(spark):
    df = _newpay(spark, [("NIH00", "", "", 2026, 26, "900000001")])
    out = cpm_common.to_staging(df, "NIH")
    assert out.columns == column_names("CPM_NIH_STG_TBL")
    assert out.collect()[0]["AGENCY"] == "NIH"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
