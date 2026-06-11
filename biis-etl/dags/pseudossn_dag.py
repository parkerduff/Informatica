"""PseudoSSN daily DAG: load_sda -> deduplicate -> archive -> notify.

The pseudossn Spark job performs parse, dedup and archive writes atomically; the
DAG models the logical stages and surfaces failures via on_failure_callback.
"""
from __future__ import annotations

import os

from airflow import DAG
from airflow.operators.python import PythonOperator

from _common import DEFAULT_START, default_args


def _load_sda(**_):
    from jobs.pseudossn import run
    from utils.secrets import load_config

    config = load_config()
    file_path = os.path.join(config.paths.landing, "pseudossn_input.dat")
    return run(config, file_path)


def _deduplicate(**context):
    if not context["ti"].xcom_pull(task_ids="load_sda"):
        raise ValueError("pseudossn load produced no detail records")


def _archive(**_):
    return True


def _notify(**_):
    from utils.notifications import send_notification
    from utils.secrets import load_config

    send_notification("PSEUDOSSN: complete", "PseudoSSN SDA load finished.", load_config())


with DAG(
    dag_id="pseudossn",
    schedule="@daily",
    start_date=DEFAULT_START,
    catchup=False,
    max_active_runs=1,
    default_args=default_args(),
    tags=["biis", "pseudossn"],
) as dag:
    load_sda = PythonOperator(task_id="load_sda", python_callable=_load_sda)
    deduplicate = PythonOperator(task_id="deduplicate", python_callable=_deduplicate)
    archive = PythonOperator(task_id="archive", python_callable=_archive)
    notify = PythonOperator(task_id="notify", python_callable=_notify)

    load_sda >> deduplicate >> archive >> notify
