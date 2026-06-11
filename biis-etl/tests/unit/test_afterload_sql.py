"""Tests for the EHRP2BIIS afterload SQL stage (jobs/ehrp2biis/afterload.py).

These exercise the SQL directly against the SQLite test backend using a clean
schema (the ``fresh_db`` fixture).
"""
import pytest

from jobs.ehrp2biis import afterload
from utils import db

RD = "2026-06-11"


def _insert(cfg, table, cols, values):
    placeholders = ", ".join("?" for _ in cols)
    db.execute(cfg, f"INSERT INTO {table} ({', '.join(cols)}) VALUES ({placeholders})", values)


def _conn(cfg):
    return db.get_connection(cfg)


def test_clean_retained_step_nulls_default(fresh_db):
    cfg = fresh_db
    _insert(cfg, "NWK_ACTION_PRIMARY_TBL", ["EVENT_ID", "LOAD_DATE"], [1001, RD])
    _insert(cfg, "NWK_ACTION_SECONDARY_TBL", ["EVENT_ID", "RETND1_STEP_CD"],
            [1001, afterload.RETAINED_STEP_DEFAULT])
    conn = _conn(cfg)
    cur = conn.cursor()
    n = afterload.clean_retained_step(cur, RD)
    conn.commit()
    conn.close()
    assert n == 1
    rows = db.fetch_all(cfg, "SELECT RETND1_STEP_CD FROM NWK_ACTION_SECONDARY_TBL WHERE EVENT_ID=1001")
    assert rows[0][0] is None


def test_insert_into_all_copies_todays_rows(fresh_db):
    cfg = fresh_db
    _insert(cfg, "NWK_ACTION_PRIMARY_TBL", ["EVENT_ID", "LOAD_DATE"], [1001, RD])
    _insert(cfg, "NWK_ACTION_PRIMARY_TBL", ["EVENT_ID", "LOAD_DATE"], [1002, "2026-01-01"])
    _insert(cfg, "NWK_ACTION_SECONDARY_TBL", ["EVENT_ID"], [1001])
    _insert(cfg, "NWK_ACTION_REMARKS_TBL", ["EVENT_ID", "REMARK_SEQ"], [1001, 1])
    conn = _conn(cfg)
    cur = conn.cursor()
    afterload.insert_into_all(cur, RD)
    conn.commit()
    conn.close()
    primary = db.fetch_all(cfg, "SELECT EVENT_ID FROM ACTION_PRIMARY_ALL")
    assert [r[0] for r in primary] == [1001]  # only today's row copied
    assert db.count(cfg, "ACTION_SECONDARY_ALL") == 1
    assert db.count(cfg, "ACTION_REMARKS_ALL") == 1


def test_cancelled_actions_deleted_and_reinserted(fresh_db):
    cfg = fresh_db
    # existing ALL row that should be replaced
    _insert(cfg, "ACTION_PRIMARY_ALL", ["EVENT_ID", "LOAD_DATE"], [1001, "2026-01-01"])
    _insert(cfg, "NWK_ACTION_PRIMARY_TBL", ["EVENT_ID", "LOAD_DATE"], [1001, RD])
    _insert(cfg, "EHRP_RECS_TRACKING_TBL",
            ["BIIS_EVENT_ID", "BIIS_WIP_STATUS_CHANGED_DT"], [1001, RD])
    conn = _conn(cfg)
    cur = conn.cursor()
    ids = afterload.cancelled_event_ids(cur, RD)
    assert ids == [1001]
    afterload.update_cancelled_actions(cur, RD)
    conn.commit()
    conn.close()
    rows = db.fetch_all(cfg, "SELECT LOAD_DATE FROM ACTION_PRIMARY_ALL WHERE EVENT_ID=1001")
    assert len(rows) == 1 and rows[0][0] == RD  # re-inserted from today's staging


def test_truncate_new_actions(fresh_db):
    cfg = fresh_db
    _insert(cfg, "NWK_NEW_EHRP_ACTIONS_TBL",
            ["EMPLID", "EMPL_RCD", "EFFDT", "EFFSEQ"], ["E1", 0, RD, 0])
    assert db.count(cfg, "NWK_NEW_EHRP_ACTIONS_TBL") == 1
    conn = _conn(cfg)
    cur = conn.cursor()
    afterload.truncate_new_actions(cur)
    conn.commit()
    conn.close()
    assert db.count(cfg, "NWK_NEW_EHRP_ACTIONS_TBL") == 0


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
