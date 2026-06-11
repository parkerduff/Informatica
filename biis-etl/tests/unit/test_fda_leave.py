"""Unit tests for jobs/fda_leave.py."""
import pytest

from jobs import fda_leave


def _fda(spark, emp_ids):
    rows = [("B1", f"TK{i:04d}", emp) for i, emp in enumerate(emp_ids)]
    return spark.createDataFrame(rows, ["FDA_BATCH_ID", "FDA_TK_NO", "FDA_EMP_ID"])


def _stg(spark, emp_ids):
    return spark.createDataFrame([(e,) for e in emp_ids], ["FDA_EMP_ID"])


def test_all_present_no_errors(spark):
    emps = ["900000001", "900000002", "900000003"]
    fda = _fda(spark, emps)
    staging = {t: _stg(spark, emps) for t, _, _ in fda_leave.REQUIRED_STAGING}
    assert fda_leave.find_errors(fda, staging).count() == 0


def test_missing_pad_generates_error(spark):
    emps = ["900000001", "900000002"]
    fda = _fda(spark, emps)
    staging = {
        "CPM_YTD_DETAIL_STG_TBL": _stg(spark, emps),
        "CPM_PAD_DETAIL_STG_TBL": _stg(spark, ["900000001"]),  # 002 missing PAD
        "CPM_MER_DETAIL_STG_TBL": _stg(spark, emps),
    }
    errs = fda_leave.find_errors(fda, staging).collect()
    assert len(errs) == 1
    assert errs[0]["ERROR_TYPE"] == "MISSING_PAD"
    assert errs[0]["FDA_EMP_ID"] == "900000002"


def test_error_counter_aggregation(spark):
    emps = ["900000001", "900000002", "900000003"]
    fda = _fda(spark, emps)
    staging = {
        "CPM_YTD_DETAIL_STG_TBL": _stg(spark, ["900000001"]),  # 002,003 miss YTD
        "CPM_PAD_DETAIL_STG_TBL": _stg(spark, ["900000001", "900000002"]),  # 003 miss PAD
        "CPM_MER_DETAIL_STG_TBL": _stg(spark, emps),
    }
    errs = fda_leave.find_errors(fda, staging)
    counts = fda_leave.count_errors_by_type(errs)
    assert counts["MISSING_YTD"] == 2
    assert counts["MISSING_PAD"] == 1
    assert "MISSING_MER" not in counts


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
