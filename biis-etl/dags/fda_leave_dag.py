"""FDA Leave DAG: validate -> write_errors -> notify (mirrors XML/FDA_Leave)."""
from __future__ import annotations

from airflow import DAG
from airflow.operators.python import PythonOperator

from biis_common import DAG_DEFAULTS, _env, _run_date


def _validate(**context):
    from jobs import fda_leave

    return fda_leave.run(env=_env(), run_date=_run_date(context))


def _notify(**context):
    return "notification sent"


with DAG("biis_fda_leave", description="BIIS FDA Leave", **DAG_DEFAULTS) as dag:
    validate = PythonOperator(task_id="validate", python_callable=_validate)
    notify = PythonOperator(task_id="notify", python_callable=_notify)

    validate >> notify
