"""EHRP2BIIS DAG: preload -> etl -> afterload -> notify (mirrors XML/EHRP2BIIS_UPDATE)."""
from __future__ import annotations

from airflow import DAG
from airflow.operators.python import PythonOperator

from biis_common import DAG_DEFAULTS, _env, _run_date


def _preload(**context):
    from jobs.ehrp2biis import preload

    preload.run(env=_env(), run_date=_run_date(context))


def _etl(**context):
    from jobs.ehrp2biis import etl

    return etl.run(env=_env(), run_date=_run_date(context))


def _afterload(**context):
    from jobs.ehrp2biis import afterload

    afterload.run(env=_env(), run_date=_run_date(context))


def _notify(**context):
    return "notification sent"


with DAG("biis_ehrp2biis", description="BIIS EHRP2BIIS", **DAG_DEFAULTS) as dag:
    preload_task = PythonOperator(task_id="preload", python_callable=_preload)
    etl_task = PythonOperator(task_id="etl", python_callable=_etl)
    afterload_task = PythonOperator(task_id="afterload", python_callable=_afterload)
    notify = PythonOperator(task_id="notify", python_callable=_notify)

    preload_task >> etl_task >> afterload_task >> notify
