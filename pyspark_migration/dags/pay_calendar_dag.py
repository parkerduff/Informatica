"""
Airflow DAG: Pay Calendar

Replaces Informatica PowerCenter workflow ``wf_Pay_Calendar``.

Task chain:
  reset_pay_calendar  -->  set_pay_calendar  -->  verify_pay_calendar
      -->  build_message_and_email

Parameters ``$$MAP_PP_NUM`` and ``$$MAP_PP_END_YEAR`` are read from
Airflow Variables (if set) and passed to the Set step.
"""

from datetime import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

default_args = {
    "owner": "biisint",
    "depends_on_past": False,
    "email": [
        "peter.chen@hhs.gov",
        "nathan.knight@hhs.gov",
        "marvin.simon@hhs.gov",
    ],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
}


def _get_spark_submit_args() -> str:
    """Build CLI arguments for the PySpark job from Airflow Variables."""
    pp_num = Variable.get("MAP_PP_NUM", default_var=None)
    pp_end_year = Variable.get("MAP_PP_END_YEAR", default_var=None)

    args = []
    if pp_num:
        args.append(f"--pp-num {pp_num}")
    if pp_end_year:
        args.append(f"--pp-end-year {pp_end_year}")

    return " ".join(args)


with DAG(
    dag_id="pay_calendar",
    default_args=default_args,
    description=(
        "Pay Calendar workflow: reset, set, verify, and email notification "
        "for the current pay period."
    ),
    schedule_interval="@daily",
    start_date=datetime(2018, 9, 28),
    catchup=False,
    tags=["pay_calendar", "etl", "pyspark"],
) as dag:

    # Single SparkSubmitOperator that runs the full pay_calendar.py job
    # (which internally executes Reset -> Set -> Verify -> Email in order)
    pay_calendar_job = SparkSubmitOperator(
        task_id="pay_calendar_job",
        application="pyspark_migration/jobs/pay_calendar.py",
        application_args=_get_spark_submit_args().split() or [],
        name="Pay_Calendar",
        conn_id="spark_default",
        verbose=True,
    )

    pay_calendar_job
