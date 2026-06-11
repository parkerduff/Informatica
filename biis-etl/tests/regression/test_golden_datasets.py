"""Regression gate: every table must match its golden dataset with ZERO diffs."""
import importlib.util
import os

import pytest

BASE = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
_spec = importlib.util.spec_from_file_location(
    "reconcile_cli", os.path.join(BASE, "scripts", "reconcile.py"))
reconcile_cli = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(reconcile_cli)

PARAMS = [
    (module, spec)
    for module, specs in reconcile_cli.MODULE_SPECS.items()
    for spec in specs
]


@pytest.mark.parametrize(
    "module,spec", PARAMS, ids=[f"{m}:{s['table']}" for m, s in PARAMS])
def test_zero_diffs(module, spec, spark_jdbc, config, secret, seed_tables):
    from utils.reconciliation import reconcile_table

    res = reconcile_table(
        spark_jdbc, config, spec["table"], spec["key_columns"],
        os.path.join(reconcile_cli.GOLDEN_DIR, spec["golden_file"]),
        secret=secret, ignore_columns=spec.get("ignore_columns"))
    assert res.schema_match, f"{spec['table']} schema mismatch"
    assert res.diff_count == 0, (
        f"{spec['table']}: {res.diff_count} diffs, sample: {res.diff_sample}")
    assert res.passed
