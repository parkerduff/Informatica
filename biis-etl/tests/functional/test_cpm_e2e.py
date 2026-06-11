"""End-to-end functional tests for the CPM agency extracts."""
import datetime as dt

import pytest

from jobs import pay_calendar
from jobs.cpm import cpm_cdc, cpm_nih, cpm_oig
from utils import db


def test_agency_record_counts(seeded_db, spark):
    pay_calendar.run(env="test", run_date=dt.date(2026, 6, 11), spark=spark)
    assert cpm_nih.run(env="test", run_date="2026-06-11", spark=spark) == 60
    assert cpm_oig.run(env="test", run_date="2026-06-11", spark=spark) == 40
    assert cpm_cdc.run(env="test", run_date="2026-06-11", spark=spark) == 50

    assert db.count(seeded_db, "CPM_NIH_STG_TBL") == 60
    assert db.count(seeded_db, "CPM_OIG_STG_TBL") == 40
    assert db.count(seeded_db, "CPM_CDC_STG_TBL") == 50


def test_agency_label_stamped(seeded_db, spark):
    pay_calendar.run(env="test", run_date=dt.date(2026, 6, 11), spark=spark)
    cpm_nih.run(env="test", run_date="2026-06-11", spark=spark)
    agencies = {r[0] for r in db.fetch_all(seeded_db, "SELECT DISTINCT AGENCY FROM CPM_NIH_STG_TBL")}
    assert agencies == {"NIH"}


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
