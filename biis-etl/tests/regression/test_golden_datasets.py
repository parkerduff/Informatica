"""Regression tests: PySpark output vs golden expected datasets.

This is the definitive proof of zero deprecation.  The whole pipeline runs once
(``pipeline`` fixture) and every reconciliation target is compared against its
golden ``*_expected_*.csv`` with row-count, schema and row/column-level checks.
"""
import pytest

from utils import db
from utils.config import get_config
from utils.reconciliation import reconcile_table
from utils.recon_targets import flat_targets

TARGETS = list(flat_targets())


@pytest.fixture(scope="module")
def conn():
    c = db.get_connection(get_config("test"))
    yield c
    c.close()


@pytest.mark.parametrize("module,table,keys", TARGETS,
                         ids=[f"{m}:{t}" for m, t, _ in TARGETS])
def test_golden_match(pipeline, conn, golden_dir, module, table, keys):
    result = reconcile_table(pipeline, conn, module, table, keys, golden_dir)
    assert result.row_count_match, (
        f"Row count mismatch for {table}: expected={result.expected_count}, "
        f"actual={result.actual_count}")
    assert result.schema_match, f"Schema mismatch for {table}: {result.schema_diff}"
    assert result.diff_count == 0, (
        f"{result.diff_count} row(s) differ in {table}. "
        f"Column diffs: {result.column_diffs}. First mismatches:\n{result.diff_sample}")


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
