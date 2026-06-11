"""Airflow DAG integrity tests -- import, task chains, no cycles, callbacks."""
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


@pytest.fixture(scope="module")
def dagbag():
    pytest.importorskip("airflow")
    from airflow.models import DagBag

    return DagBag(dag_folder=str(DAGS_DIR), include_examples=False)


def test_no_import_errors(dagbag):
    assert dagbag.import_errors == {}, dagbag.import_errors


def test_all_dags_present(dagbag):
    expected = {"pay_calendar", "comptime", "pseudossn", "fda_leave",
                "ehrp2biis", "cpm", "transfer"}
    assert expected <= set(dagbag.dag_ids)


def test_settings_and_failure_callback(dagbag):
    for dag_id in dagbag.dag_ids:
        dag = dagbag.get_dag(dag_id)
        assert dag.catchup is False
        assert dag.max_active_runs == 1
        for task in dag.tasks:
            assert task.on_failure_callback is not None


@pytest.mark.parametrize("dag_id,chain", list(EXPECTED_CHAINS.items()))
def test_sequential_chain(dagbag, dag_id, chain):
    dag = dagbag.get_dag(dag_id)
    for upstream, downstream in zip(chain, chain[1:]):
        assert downstream in dag.get_task(upstream).downstream_task_ids
