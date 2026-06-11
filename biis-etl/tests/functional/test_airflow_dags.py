"""Validate that every Airflow DAG parses and has the expected task chain.

Skipped automatically when Airflow is not installed (it is heavy and not part of
``requirements.txt``); the docker-compose ``airflow`` service and CI exercise the
real parse.  Run locally with ``pip install apache-airflow==2.8.1``.
"""
import os

import pytest

airflow = pytest.importorskip("airflow")

DAGS_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), "dags")

EXPECTED = {
    "biis_pay_calendar": {"reset", "set_current", "verify", "notify"},
    "biis_comptime": {"load", "count", "notify"},
    "biis_pseudossn": {"parse_dedup_archive", "notify"},
    "biis_fda_leave": {"validate", "notify"},
    "biis_ehrp2biis": {"preload", "etl", "afterload", "notify"},
    "biis_cpm": {"extract_nih", "extract_oig", "extract_cdc", "notify"},
}


@pytest.fixture(scope="module")
def dagbag():
    import sys

    sys.path.insert(0, DAGS_DIR)
    from airflow.models import DagBag

    return DagBag(dag_folder=DAGS_DIR, include_examples=False)


def test_no_import_errors(dagbag):
    assert dagbag.import_errors == {}, f"DAG import errors: {dagbag.import_errors}"


@pytest.mark.parametrize("dag_id,tasks", EXPECTED.items())
def test_dag_tasks_present(dagbag, dag_id, tasks):
    dag = dagbag.get_dag(dag_id)
    assert dag is not None, f"{dag_id} not found"
    assert {t.task_id for t in dag.tasks} == tasks


def test_pay_calendar_chain(dagbag):
    dag = dagbag.get_dag("biis_pay_calendar")
    downstream = {t.task_id: {d.task_id for d in t.downstream_list} for t in dag.tasks}
    assert downstream["reset"] == {"set_current"}
    assert downstream["set_current"] == {"verify"}
    assert downstream["verify"] == {"notify"}


def test_ehrp2biis_chain(dagbag):
    dag = dagbag.get_dag("biis_ehrp2biis")
    downstream = {t.task_id: {d.task_id for d in t.downstream_list} for t in dag.tasks}
    assert downstream["preload"] == {"etl"}
    assert downstream["etl"] == {"afterload"}
    assert downstream["afterload"] == {"notify"}


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
