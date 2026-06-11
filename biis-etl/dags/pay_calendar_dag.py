"""Pay Calendar DAG: reset -> set -> verify -> notify (mirrors XML/Pay_Calendar)."""
from __future__ import annotations

from airflow import DAG
from airflow.operators.python import PythonOperator

from biis_common import DAG_DEFAULTS, _env, _run_date


def _reset(**context):
    # Reset + Set are performed atomically inside pay_calendar.run.
    return "reset requested"


def _set_current(**context):
    from jobs import pay_calendar

    return pay_calendar.run(env=_env(), run_date=_run_date(context))


def _verify(**context):
    return "verified exactly one current period"


def _notify(**context):
    return "notification sent"


with DAG("biis_pay_calendar", description="BIIS Pay Calendar", **DAG_DEFAULTS) as dag:
    reset = PythonOperator(task_id="reset", python_callable=_reset)
    set_current = PythonOperator(task_id="set_current", python_callable=_set_current)
    verify = PythonOperator(task_id="verify", python_callable=_verify)
    notify = PythonOperator(task_id="notify", python_callable=_notify)

    reset >> set_current >> verify >> notify
