"""
Airflow DAG: CPM NIH

Replaces Informatica PowerCenter workflow for CPM NIH processing
(source file: XML/CPM_NIH).

Reads ``$$MAP_PP_NUM`` and ``$$MAP_PP_END_YEAR`` from Airflow Variables
and passes them as CLI arguments to the PySpark job.
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

with DAG(
    dag_id="cpm_nih",
    default_args=default_args,
    description="CPM NIH Build Message — filter, aggregate, and email notification.",
    schedule_interval="@daily",
    start_date=datetime(2018, 9, 28),
    catchup=False,
    tags=["cpm", "nih", "etl", "pyspark"],
) as dag:

    pp_num = Variable.get("MAP_PP_NUM")
    pp_end_year = Variable.get("MAP_PP_END_YEAR")

    cpm_nih_job = SparkSubmitOperator(
        task_id="cpm_nih_build_message",
        application="pyspark_migration/jobs/cpm_nih.py",
        application_args=[
            "--pp-end-year", str(pp_end_year),
            "--pp-num", str(pp_num),
        ],
        name="CPM_NIH_Build_Message",
        conn_id="spark_default",
        verbose=True,
    )

    cpm_nih_job
