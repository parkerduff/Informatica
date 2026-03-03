"""
Airflow DAG: FDA Leave

Replaces Informatica PowerCenter workflow for FDA Leave processing
(source file: XML/FDA_Leave).

This DAG runs the FDA Leave Cycle ID Tracking PySpark job, which
increments the CYCLE_ID in CPM_CYCLE_TBL for PROCESS_NAME = 'FDA'
and updates the pay period fields from the current PAY_PERIOD row.
"""

from datetime import datetime

from airflow import DAG
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

with DAG(
    dag_id="fda_leave",
    default_args=default_args,
    description="FDA Leave Cycle ID Tracking — increment cycle counter and update pay period.",
    schedule_interval="@daily",
    start_date=datetime(2018, 9, 28),
    catchup=False,
    tags=["fda", "leave", "etl", "pyspark"],
) as dag:

    fda_leave_job = SparkSubmitOperator(
        task_id="fda_leave_cycle_id",
        application="pyspark_migration/jobs/fda_leave.py",
        name="FDA_Leave_Cycle_ID",
        conn_id="spark_default",
        verbose=True,
    )

    fda_leave_job
