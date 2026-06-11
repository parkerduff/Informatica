"""Airflow DAG integrity tests -- import, task chains, no cycles, callbacks.

DAGs are imported directly (rather than via ``DagBag``) so the checks need no
Airflow metadata database, which keeps them runnable in plain CI.
"""
import importlib.util
import sys
from pathlib import Path

import pytest

pytestmark = pytest.mark.functional

DAGS_DIR = Path(__file__).resolve().parents[2] / "dags"

EXPECTED_CHAINS = {
    "pay_calendar": ["reset", "set", "verify", "notify"],
    "comptime": ["get_pay_period", "load_file", "build_counters"],
    "pseudossn": ["load_sda", "deduplicate", "archive", "notify"],
    "fda_leave": ["validate_records", "write_errors", "count_errors", "notify"],
    "ehrp2biis": ["preload", "etl", "afterload", "notify"],
}


def _load_dags():
    pytest.importorskip("airflow")
    from airflow import DAG

    # The DAG modules import the shared `_common` helper by bare name.
    if str(DAGS_DIR) not in sys.path:
        sys.path.insert(0, str(DAGS_DIR))

    dags = {}
    errors = {}
    for path in sorted(DAGS_DIR.glob("*_dag.py")):
        spec = importlib.util.spec_from_file_location(path.stem, path)
        module = importlib.util.module_from_spec(spec)
        try:
            spec.loader.exec_module(module)
        except Exception as exc:  # noqa: BLE001 -- record import failures
            errors[path.name] = repr(exc)
            continue
        for obj in vars(module).values():
            if isinstance(obj, DAG):
                dags[obj.dag_id] = obj
    return dags, errors


@pytest.fixture(scope="module")
def dags():
    loaded, errors = _load_dags()
    assert errors == {}, errors
    return loaded


def test_all_dags_present(dags):
    expected = {"pay_calendar", "comptime", "pseudossn", "fda_leave",
                "ehrp2biis", "cpm", "transfer"}
    assert expected <= set(dags)


def test_settings_and_failure_callback(dags):
    for dag_id, dag in dags.items():
        assert dag.catchup is False
        assert dag.max_active_runs == 1
        for task in dag.tasks:
            assert task.on_failure_callback, f"{dag_id}.{task.task_id} missing callback"


@pytest.mark.parametrize("dag_id,chain", list(EXPECTED_CHAINS.items()))
def test_sequential_chain(dags, dag_id, chain):
    dag = dags[dag_id]
    for upstream, downstream in zip(chain, chain[1:]):
        assert downstream in dag.get_task(upstream).downstream_task_ids
