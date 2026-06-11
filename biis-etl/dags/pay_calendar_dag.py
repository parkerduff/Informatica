"""Pay Calendar DAG: reset -> set -> verify -> notify."""
import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from dag_common import BASE, DAG_KWARGS  # noqa: F401


def _conn():
    from utils.secrets import get_secret, load_config

    config = load_config()
    return config, get_secret("biis", config)


def task_reset(**_):
    from jobs.pay_calendar import session_reset
    from utils.db import pyodbc_connection

    config, secret = _conn()
    with pyodbc_connection(secret, config) as conn:
        session_reset(conn)


def task_set(ds=None, **_):
    from jobs.pay_calendar import session_set
    from utils.db import pyodbc_connection

    config, secret = _conn()
    run_date = datetime.datetime.strptime(ds, "%Y-%m-%d").date()
    with pyodbc_connection(secret, config) as conn:
        session_set(conn, run_date)


def task_verify(**_):
    from jobs.pay_calendar import session_verify
    from utils.db import pyodbc_connection

    config, secret = _conn()
    with pyodbc_connection(secret, config) as conn:
        return session_verify(conn)


def task_notify(ti=None, **_):
    from jobs.pay_calendar import session_notify

    config, _secret = _conn()
    pp = ti.xcom_pull(task_ids="verify")
    session_notify(pp, config)


with DAG(dag_id="pay_calendar", start_date=datetime.datetime(2026, 1, 1), **DAG_KWARGS) as dag:
    reset = PythonOperator(task_id="reset", python_callable=task_reset)
    set_current = PythonOperator(task_id="set_current", python_callable=task_set)
    verify = PythonOperator(task_id="verify", python_callable=task_verify)
    notify = PythonOperator(task_id="notify", python_callable=task_notify)

    reset >> set_current >> verify >> notify
