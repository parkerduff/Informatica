"""FDA Leave DAG: validate_records -> write_errors -> count_errors -> notify."""
import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from dag_common import BASE, DAG_KWARGS  # noqa: F401


def task_validate_records(ds=None, **_):
    from jobs.fda_leave import run

    return run(env=None, run_date=ds)


def task_write_errors(ti=None, **_):
    return ti.xcom_pull(task_ids="validate_records")


def task_count_errors(**_):
    from utils.db import pyodbc_connection
    from utils.secrets import get_secret, load_config

    config = load_config()
    secret = get_secret("biis", config)
    with pyodbc_connection(secret, config) as conn:
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM ERROR_TBL WHERE PROCESS_NAME = 'FDA_LEAVE'")
        return cur.fetchone()[0]


def task_notify(ti=None, **_):
    from utils.notifications import send_notification
    from utils.secrets import load_config

    config = load_config()
    n = ti.xcom_pull(task_ids="count_errors")
    send_notification("FDA Leave DAG completed", f"{n} error rows recorded", config)


with DAG(dag_id="fda_leave", start_date=datetime.datetime(2026, 1, 1), **DAG_KWARGS) as dag:
    validate_records = PythonOperator(task_id="validate_records", python_callable=task_validate_records)
    write_errors = PythonOperator(task_id="write_errors", python_callable=task_write_errors)
    count_errors = PythonOperator(task_id="count_errors", python_callable=task_count_errors)
    notify = PythonOperator(task_id="notify", python_callable=task_notify)

    validate_records >> write_errors >> count_errors >> notify
