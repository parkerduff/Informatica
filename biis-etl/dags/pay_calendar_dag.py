"""Pay Calendar daily DAG: reset -> set -> verify -> notify."""
from __future__ import annotations

from airflow import DAG
from airflow.operators.python import PythonOperator

from _common import DEFAULT_START, default_args, run_date_from_context


def _reset(**_):
    from jobs.pay_calendar import session_reset
    from utils.db import pyodbc_connection
    from utils.secrets import get_db_secret, load_config

    config = load_config()
    with pyodbc_connection(get_db_secret(config), config) as conn:
        session_reset(conn)


def _set(**context):
    from jobs.pay_calendar import session_set
    from utils.db import pyodbc_connection
    from utils.secrets import get_db_secret, load_config

    config = load_config()
    with pyodbc_connection(get_db_secret(config), config) as conn:
        session_set(conn, run_date_from_context(context))


def _verify(**context):
    from jobs.pay_calendar import session_verify
    from utils.db import pyodbc_connection
    from utils.secrets import get_db_secret, load_config

    config = load_config()
    with pyodbc_connection(get_db_secret(config), config) as conn:
        pp = session_verify(conn)
    return {k: str(v) for k, v in pp.items()}


def _notify(**context):
    from jobs.pay_calendar import session_notify
    from utils.secrets import load_config

    pp = context["ti"].xcom_pull(task_ids="verify") or {}
    session_notify(pp, load_config())


with DAG(
    dag_id="pay_calendar",
    schedule="@daily",
    start_date=DEFAULT_START,
    catchup=False,
    max_active_runs=1,
    default_args=default_args(),
    tags=["biis", "pay_calendar"],
) as dag:
    reset = PythonOperator(task_id="reset", python_callable=_reset)
    set_pp = PythonOperator(task_id="set", python_callable=_set)
    verify = PythonOperator(task_id="verify", python_callable=_verify)
    notify = PythonOperator(task_id="notify", python_callable=_notify)

    reset >> set_pp >> verify >> notify
