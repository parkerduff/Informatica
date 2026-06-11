"""FDA Leave daily DAG: validate_records -> write_errors -> count_errors -> notify."""
from __future__ import annotations

from airflow import DAG
from airflow.operators.python import PythonOperator

from _common import DEFAULT_START, default_args, run_date_from_context


def _validate_records(**context):
    # Runs the fda_leave job which validates lookups and writes ERROR_TBL.
    from jobs.fda_leave import run
    from utils.secrets import load_config

    return run(load_config(), run_date_from_context(context))


def _write_errors(**_):
    # Error rows are written inside the validate step (single Spark action).
    return True


def _count_errors(**context):
    count = context["ti"].xcom_pull(task_ids="validate_records")
    context["ti"].xcom_push(key="error_count", value=count)
    return count


def _notify(**context):
    from utils.notifications import send_notification
    from utils.secrets import load_config

    count = context["ti"].xcom_pull(task_ids="validate_records")
    send_notification("FDA_LEAVE: complete", f"Wrote {count} error rows.", load_config())


with DAG(
    dag_id="fda_leave",
    schedule="@daily",
    start_date=DEFAULT_START,
    catchup=False,
    max_active_runs=1,
    default_args=default_args(),
    tags=["biis", "fda_leave"],
) as dag:
    validate_records = PythonOperator(task_id="validate_records", python_callable=_validate_records)
    write_errors = PythonOperator(task_id="write_errors", python_callable=_write_errors)
    count_errors = PythonOperator(task_id="count_errors", python_callable=_count_errors)
    notify = PythonOperator(task_id="notify", python_callable=_notify)

    validate_records >> write_errors >> count_errors >> notify
