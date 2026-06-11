"""EHRP2BIIS DAG: preload -> etl -> afterload -> notify."""
import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from dag_common import BASE, DAG_KWARGS  # noqa: F401


def task_preload(**_):
    from jobs.ehrp2biis.preload import run

    run(env=None)


def task_etl(ds=None, **_):
    from jobs.ehrp2biis.etl import run

    return run(env=None, run_date=ds)


def task_afterload(ds=None, **_):
    from jobs.ehrp2biis.afterload import run

    run(env=None, run_date=ds)


def task_notify(ti=None, **_):
    from utils.notifications import send_notification
    from utils.secrets import load_config

    config = load_config()
    counts = ti.xcom_pull(task_ids="etl") or {}
    send_notification(
        "EHRP2BIIS DAG completed",
        f"Processed {counts.get('events', 0)} action events",
        config,
    )


with DAG(dag_id="ehrp2biis", start_date=datetime.datetime(2026, 1, 1), **DAG_KWARGS) as dag:
    preload = PythonOperator(task_id="preload", python_callable=task_preload)
    etl = PythonOperator(task_id="etl", python_callable=task_etl)
    afterload = PythonOperator(task_id="afterload", python_callable=task_afterload)
    notify = PythonOperator(task_id="notify", python_callable=task_notify)

    preload >> etl >> afterload >> notify
