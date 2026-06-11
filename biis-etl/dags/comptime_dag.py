"""COMPTIME daily DAG: get_pay_period -> load_file -> build_counters.

The load step runs the comptime Spark job end-to-end (it derives the pay period,
loads the flat file and writes counters); the surrounding tasks make the
dependency chain explicit and provide hooks for monitoring/alerting.
"""
from __future__ import annotations

import os

from airflow import DAG
from airflow.operators.python import PythonOperator

from _common import DEFAULT_START, default_args


def _get_pay_period(**_):
    from utils.db import get_current_pay_period
    from utils.secrets import get_db_secret, load_config
    from utils.spark import get_spark

    config = load_config()
    return get_current_pay_period(get_spark("comptime-pp"), config, get_db_secret(config))


def _load_file(**_):
    from jobs.comptime import run
    from utils.secrets import load_config

    config = load_config()
    file_path = os.path.join(config.paths.landing, "comptime_input.csv")
    return run(config, file_path)


def _build_counters(**context):
    # Counters are written inside the load step; this task asserts a load ran.
    count = context["ti"].xcom_pull(task_ids="load_file")
    if not count:
        raise ValueError("comptime load produced no rows")


with DAG(
    dag_id="comptime",
    schedule="@daily",
    start_date=DEFAULT_START,
    catchup=False,
    max_active_runs=1,
    default_args=default_args(),
    tags=["biis", "comptime"],
) as dag:
    get_pp = PythonOperator(task_id="get_pay_period", python_callable=_get_pay_period)
    load_file = PythonOperator(task_id="load_file", python_callable=_load_file)
    build_counters = PythonOperator(task_id="build_counters", python_callable=_build_counters)

    get_pp >> load_file >> build_counters
