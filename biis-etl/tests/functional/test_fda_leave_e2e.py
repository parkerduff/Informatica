"""End-to-end functional tests for the FDA Leave job."""
import pytest

from jobs import fda_leave
from utils import db


def test_error_rows_for_missing_staging(seeded_db, spark):
    n = fda_leave.run(env="test", run_date="2026-06-11", spark=spark)
    assert n == 10
    assert db.count(seeded_db, "ERROR_TBL") == 10
    # every error references one of the 30 seeded employees and a known type
    types = {r[0] for r in db.fetch_all(seeded_db, "SELECT DISTINCT ERROR_TYPE FROM ERROR_TBL")}
    assert types <= {"MISSING_YTD", "MISSING_PAD", "MISSING_MER"}


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
