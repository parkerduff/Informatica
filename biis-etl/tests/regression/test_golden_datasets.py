"""Golden-dataset regression: every target table must reconcile with ZERO diffs."""
import pytest

from utils.reconciliation import MODULE_TABLES, reconcile_table

pytestmark = pytest.mark.regression

CASES = [
    (module, table, keys, golden)
    for module, specs in MODULE_TABLES.items()
    for (table, keys, golden) in specs
]


@pytest.mark.parametrize("module,table,keys,golden", CASES,
                         ids=[f"{m}:{t}" for m, t, _, _ in CASES])
def test_table_zero_diffs(pipeline, spark, config, golden_dir, module, table, keys, golden):
    result = reconcile_table(spark, config, table, keys,
                             str(golden_dir / golden), module=module)
    assert result.error is None, result.error
    assert result.schema_match, f"schema diff: {result.schema_diff}"
    assert result.row_count_match, (
        f"row count: expected={result.expected_count} actual={result.actual_count}")
    assert result.diff_count == 0, (
        f"{result.diff_count} diffs in {table}; columns={result.column_diffs}\n"
        f"{result.diff_sample}")
