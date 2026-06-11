"""Unit tests for the EHRP2BIIS ETL transformations (jobs/ehrp2biis/etl.py)."""
import pytest
from pyspark.sql import functions as F

from jobs.ehrp2biis import etl
from jobs.ehrp2biis.common import JOIN_KEYS, build_full_df
from utils.schemas import column_names


def _actions(spark, keys):
    return spark.createDataFrame(keys, JOIN_KEYS)


def _gvt(spark, keys, wip="P"):
    rows = [k + (wip,) for k in keys]
    return spark.createDataFrame(rows, JOIN_KEYS + ["GVT_WIP_STATUS"])


def test_join_actions_to_gvt_job_inner(spark):
    actions = _actions(spark, [("E1", 0, "2026-06-01", 0), ("E2", 0, "2026-06-02", 0)])
    gvt = _gvt(spark, [("E1", 0, "2026-06-01", 0)])  # only E1 matches
    joined = etl.join_actions_to_gvt_job(actions, gvt)
    rows = joined.collect()
    assert len(rows) == 1 and rows[0]["EMPLID"] == "E1"


def test_sequence_number_assignment(spark):
    actions = _actions(spark, [("E1", 0, "2026-06-01", 0), ("E2", 0, "2026-06-02", 0)])
    gvt = _gvt(spark, [("E1", 0, "2026-06-01", 0), ("E2", 0, "2026-06-02", 0)])
    matched = etl.join_actions_to_gvt_job(actions, gvt)
    assigned = etl.assign_event_ids(matched, base=1000)
    ids = sorted(r["EVENT_ID"] for r in assigned.collect())
    assert ids == [1001, 1002]


def test_build_full_df_has_full_schema_with_overrides(spark):
    matched = _gvt(spark, [("E1", 0, "2026-06-01", 0)]).withColumn("EVENT_ID", F.lit(5001))
    primary = build_full_df(matched, "NWK_ACTION_PRIMARY_TBL",
                            overrides={"EVENT_ID": F.col("EVENT_ID"), "LOAD_DATE": F.lit("2026-06-11")})
    assert primary.columns == column_names("NWK_ACTION_PRIMARY_TBL")
    row = primary.collect()[0]
    assert row["EVENT_ID"] == 5001
    assert row["LOAD_DATE"] == "2026-06-11"


def test_retained_step_default_placeholder(spark):
    matched = _gvt(spark, [("E1", 0, "2026-06-01", 0)]).withColumn("EVENT_ID", F.lit(5001))
    secondary = build_full_df(matched, "NWK_ACTION_SECONDARY_TBL",
                              overrides={"EVENT_ID": F.col("EVENT_ID"),
                                         "RETND1_STEP_CD": F.lit(etl.RETAINED_STEP_DEFAULT)})
    assert secondary.collect()[0]["RETND1_STEP_CD"] == "0.0000000000000"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
