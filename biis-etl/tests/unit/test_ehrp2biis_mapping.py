"""Unit tests for the EHRP2BIIS field-mapping rules."""
import pytest

from jobs.ehrp2biis import mapping as M
from jobs.schemas import (NWK_ACTION_PRIMARY_TBL_COLS,
                          NWK_ACTION_SECONDARY_TBL_COLS, PS_GVT_JOB_COLS)

pytestmark = pytest.mark.unit


def test_primary_plan_partitions_all_non_override_columns():
    plan = M.primary_plan()
    target_names = {c[0] for c in NWK_ACTION_PRIMARY_TBL_COLS}
    covered = set(plan["copy"]) | set(plan["null"]) | M.PRIMARY_OVERRIDES
    assert target_names <= covered
    # overrides never appear in copy/null
    assert not (set(plan["copy"]) & M.PRIMARY_OVERRIDES)


def test_copy_columns_exist_in_source():
    src = {c[0] for c in PS_GVT_JOB_COLS}
    for name in M.primary_plan()["copy"]:
        assert name in src


def test_null_columns_absent_from_source():
    src = {c[0] for c in PS_GVT_JOB_COLS}
    for name in M.secondary_plan()["null"]:
        assert name not in src


def test_col_type_lookup_and_default():
    assert M.col_type(NWK_ACTION_SECONDARY_TBL_COLS, "EVENT_ID")[0]
    assert M.col_type(NWK_ACTION_SECONDARY_TBL_COLS, "___missing___") == ("varchar2", "255", "0")
