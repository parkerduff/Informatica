import os

import pytest

airflow = pytest.importorskip("airflow")

BASE = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DAGS_DIR = os.path.join(BASE, "dags")

EXPECTED_DAGS = ["pay_calendar", "comptime", "pseudossn", "fda_leave",
                 "ehrp2biis", "cpm", "transfer"]


@pytest.fixture(scope="module")
def dagbag():
    import sys

    from airflow.models import DagBag

    if DAGS_DIR not in sys.path:
        sys.path.insert(0, DAGS_DIR)
    return DagBag(dag_folder=DAGS_DIR, include_examples=False)


def test_no_import_errors(dagbag):
    assert dagbag.import_errors == {}


def test_all_dags_present(dagbag):
    for dag_id in EXPECTED_DAGS:
        assert dag_id in dagbag.dags, f"missing DAG {dag_id}"


def test_dag_settings(dagbag):
    for dag_id in EXPECTED_DAGS:
        dag = dagbag.dags[dag_id]
        assert dag.catchup is False
        assert dag.max_active_runs == 1


def test_on_failure_callback_on_all_tasks(dagbag):
    for dag_id in EXPECTED_DAGS:
        for task in dagbag.dags[dag_id].tasks:
            assert task.on_failure_callback, f"{dag_id}.{task.task_id} missing callback"


def test_no_cycles(dagbag):
    try:
        from airflow.utils.dag_cycle_tester import check_cycle
    except ImportError:
        pytest.skip("cycle tester unavailable")
    for dag_id in EXPECTED_DAGS:
        check_cycle(dagbag.dags[dag_id])


def test_cpm_dag_structure(dagbag):
    dag = dagbag.dags["cpm"]
    ids = {t.task_id for t in dag.tasks}
    assert {"load_common", "extract_nih", "extract_oig", "extract_cdc",
            "transfer_nih", "transfer_oig", "transfer_cdc", "archive"} <= ids
    load_common = dag.get_task("load_common")
    downstream = set(load_common.downstream_task_ids)
    assert {"extract_nih", "extract_oig", "extract_cdc"} <= downstream
