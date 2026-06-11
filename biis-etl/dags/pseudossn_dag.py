"""PseudoSSN DAG: load_sda -> deduplicate -> archive -> notify."""
import datetime
import os

from airflow import DAG
from airflow.operators.python import PythonOperator

from dag_common import BASE, DAG_KWARGS  # noqa: F401


def task_load_sda(**_):
    from jobs.pseudossn import run
    from utils.secrets import load_config

    config = load_config()
    path = os.path.join(config["paths"]["landing"], "pseudossn_input.dat")
    return run(env=None, file_path=path)


def task_deduplicate(ti=None, **_):
    counts = ti.xcom_pull(task_ids="load_sda")
    return counts["deduped"]


def task_archive(ti=None, **_):
    counts = ti.xcom_pull(task_ids="load_sda")
    return counts["all"]


def task_notify(ti=None, **_):
    from utils.notifications import send_notification
    from utils.secrets import load_config

    config = load_config()
    counts = ti.xcom_pull(task_ids="load_sda")
    send_notification(
        "PseudoSSN DAG completed",
        f"{counts['all']} detail rows loaded, {counts['deduped']} deduplicated",
        config,
    )


with DAG(dag_id="pseudossn", start_date=datetime.datetime(2026, 1, 1), **DAG_KWARGS) as dag:
    load_sda = PythonOperator(task_id="load_sda", python_callable=task_load_sda)
    deduplicate = PythonOperator(task_id="deduplicate", python_callable=task_deduplicate)
    archive = PythonOperator(task_id="archive", python_callable=task_archive)
    notify = PythonOperator(task_id="notify", python_callable=task_notify)

    load_sda >> deduplicate >> archive >> notify
