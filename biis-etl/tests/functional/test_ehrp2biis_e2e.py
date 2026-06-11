"""End-to-end functional tests for the EHRP2BIIS preload -> etl -> afterload chain."""
import datetime as dt

import pytest

from jobs.ehrp2biis import afterload, etl, preload
from utils import db

RD = dt.date(2026, 6, 11)


def test_full_chain_loads_all_tables(seeded_db, spark):
    preload.run(env="test", run_date=RD)
    n = etl.run(env="test", run_date=RD, spark=spark)
    afterload.run(env="test", run_date=RD)

    assert n == 50  # 50 matching action/gvt_job rows
    assert db.count(seeded_db, "ACTION_PRIMARY_ALL") == 50
    assert db.count(seeded_db, "ACTION_SECONDARY_ALL") == 50
    assert db.count(seeded_db, "ACTION_REMARKS_ALL") == 50

    # retained-step default cleaned to NULL for non-900-series events
    leftover = db.fetch_all(
        seeded_db,
        "SELECT COUNT(*) FROM NWK_ACTION_SECONDARY_TBL WHERE RETND1_STEP_CD = ?",
        [etl.RETAINED_STEP_DEFAULT])
    assert leftover[0][0] == 0

    # sequence advanced and new-actions staging truncated
    seq = db.fetch_all(seeded_db, "SELECT SEQ_VALUE FROM SEQUENCE_NUM_TBL WHERE SEQ_NAME='EVENT_ID'")
    assert seq[0][0] == 1050
    assert db.count(seeded_db, "NWK_NEW_EHRP_ACTIONS_TBL") == 0


def test_preload_is_idempotent(seeded_db, spark):
    preload.run(env="test", run_date=RD)
    etl.run(env="test", run_date=RD, spark=spark)
    # second preload clears the same-day staged rows so a re-run does not double-load
    preload.run(env="test", run_date=RD)
    assert db.count(seeded_db, "NWK_ACTION_PRIMARY_TBL") == 0


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
