"""EHRP2BIIS daily DAG: preload -> etl -> afterload -> notify (strict chain)."""
from __future__ import annotations

from airflow import DAG
from airflow.operators.python import PythonOperator

from _common import DEFAULT_START, default_args, run_date_from_context


def _preload(**context):
    from jobs.ehrp2biis.preload import run
    from utils.secrets import load_config

    return run(load_config(), run_date_from_context(context))


def _etl(**context):
    from jobs.ehrp2biis.etl import run
    from utils.secrets import load_config

    return run(load_config(), run_date_from_context(context))


def _afterload(**context):
    from jobs.ehrp2biis.afterload import run
    from utils.secrets import load_config

    return run(load_config(), run_date_from_context(context))


def _notify(**_):
    from utils.notifications import send_notification
    from utils.secrets import load_config

    send_notification("EHRP2BIIS: complete", "EHRP2BIIS pipeline finished.", load_config())


with DAG(
    dag_id="ehrp2biis",
    schedule="@daily",
    start_date=DEFAULT_START,
    catchup=False,
    max_active_runs=1,
    default_args=default_args(),
    tags=["biis", "ehrp2biis"],
) as dag:
    preload = PythonOperator(task_id="preload", python_callable=_preload)
    etl = PythonOperator(task_id="etl", python_callable=_etl)
    afterload = PythonOperator(task_id="afterload", python_callable=_afterload)
    notify = PythonOperator(task_id="notify", python_callable=_notify)

    preload >> etl >> afterload >> notify
