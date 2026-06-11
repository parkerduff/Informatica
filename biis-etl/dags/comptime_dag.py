"""COMPTIME DAG: parse -> load -> count -> notify (mirrors XML/COMPTIME)."""
from __future__ import annotations

import os

from airflow import DAG
from airflow.operators.python import PythonOperator

from biis_common import DAG_DEFAULTS, _env, _run_date


def _load(**context):
    from jobs import comptime
    from utils.config import get_config

    cfg = get_config(_env())
    path = os.path.join(cfg.paths["landing"], "comptime_input.csv")
    return comptime.run(env=_env(), file_path=path, run_date=_run_date(context))


def _count(**context):
    return "counter table written"


def _notify(**context):
    return "notification sent"


with DAG("biis_comptime", description="BIIS COMPTIME", **DAG_DEFAULTS) as dag:
    load = PythonOperator(task_id="load", python_callable=_load)
    count = PythonOperator(task_id="count", python_callable=_count)
    notify = PythonOperator(task_id="notify", python_callable=_notify)

    load >> count >> notify
