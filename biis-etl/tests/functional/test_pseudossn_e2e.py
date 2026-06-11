"""End-to-end functional tests for the PseudoSSN job."""
import os

import pytest

from jobs import pseudossn
from utils import db


def test_loads_detail_dedup_and_archive(seeded_db, spark, golden_dir):
    pseudossn.run(env="test", file_path=os.path.join(golden_dir, "pseudossn_input.dat"),
                  run_date="2026-06-11", spark=spark)

    # 50 detail (D) records; header/trailer filtered
    assert db.count(seeded_db, "PSEUDOSSN_FROM_SDA_TBL") == 50
    # deduped to one row per pseudo-SSN (5 duplicates removed)
    assert db.count(seeded_db, "PSEUDOSSN_TBL") == 45
    # archive populated
    assert db.count(seeded_db, "HI_ARCH_PSEUDOSSN_TBL") == 45


def test_requires_file_path(seeded_db, spark):
    with pytest.raises(ValueError):
        pseudossn.run(env="test", file_path=None, spark=spark)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
