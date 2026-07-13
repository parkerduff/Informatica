"""Integration tests: converted engine output reconciles to the golden baseline.

Uses the pure-Python engine (semantics identical to the Spark engine, which is
covered separately when pyspark is installed) so the suite runs without a JVM.
"""
import os
import sys

import pytest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from migration.baseline import reference as REF
from migration.jobs import configs
from migration.jobs.runner import _resolver_from_dir
from migration.lib import engine_pandas as EP
from migration.recon import reconcile as RC
from migration.synth import generate as SYNTH

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
DATA_DIR = os.path.join(REPO_ROOT, "migration", "local", "data", "functional")


@pytest.fixture(scope="session", autouse=True)
def _ensure_data():
    # generate functional data once if missing
    if not os.path.isdir(DATA_DIR) or not os.listdir(DATA_DIR):
        SYNTH.main(["generate", "--mode", "functional"])


@pytest.mark.parametrize("folder", configs.ALL_FOLDERS)
def test_engine_matches_baseline(folder):
    pdef = configs.build(folder)
    resolver = _resolver_from_dir(pdef, os.path.join(DATA_DIR, folder))
    conv = EP.run(pdef, resolver)
    base = REF.run(pdef, resolver)
    cols = [{"name": c.name, "datatype": c.datatype, "scale": c.scale,
             "precision": c.precision} for c in pdef.target.columns]
    rep = RC.reconcile(folder, conv, base, cols)
    assert rep["verdict"] == "PASS", (
        f"{folder}: mismatches={rep['field']['cell_mismatches']} "
        f"examples={rep['field']['examples'][:3]}")
    assert rep["error_parity"]["keys_match"]
    assert rep["counter_parity"]["match"]


def test_all_specs_present():
    for folder in configs.ALL_FOLDERS:
        assert os.path.exists(os.path.join(REPO_ROOT, "migration", "spec", f"{folder}.json"))


def test_classification_split():
    spark = [f for f in configs.ALL_FOLDERS if configs.build(f).engine == "pyspark"]
    shell = [f for f in configs.ALL_FOLDERS if configs.build(f).engine == "python_shell"]
    assert set(shell) == {"Pay_Calendar", "COMPTIME"}
    assert len(spark) == 6


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-v"]))
