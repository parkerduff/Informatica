"""
Airflow DAG: EHRP2BIIS Update

Replaces Informatica PowerCenter workflow ``wf_EHRP2BIIS_UPDATE``.

Schedule: Daily (every 1 day, starting 9/28/2018, runs forever).

Task chain:
  preload_sql  -->  m_ehrp2biis_update  -->  afterload_sql  -->  action_stage_load
"""

from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

from pyspark_migration.utils.preload_sql import (
    run_preload_sql,
    run_afterload_sql,
    run_action_stage_load,
)

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
    dag_id="ehrp2biis_update",
    default_args=default_args,
    description=(
        "EHRP2BIIS Update pipeline: preload SQL, PySpark ETL, afterload SQL, "
        "action stage load."
    ),
    schedule_interval="@daily",
    start_date=datetime(2018, 9, 28),
    catchup=False,
    tags=["ehrp2biis", "etl", "pyspark"],
) as dag:

    # Task 1 — Pre-load SQL (replaces ehrp2biis_preload KSH script)
    preload = PythonOperator(
        task_id="preload_sql",
        python_callable=run_preload_sql,
    )

    # Task 2 — Main PySpark ETL (replaces m_EHRP2BIIS_UPDATE mapping)
    transform = SparkSubmitOperator(
        task_id="m_ehrp2biis_update",
        application="pyspark_migration/jobs/ehrp2biis_update.py",
        name="EHRP2BIIS_UPDATE",
        conn_id="spark_default",
        verbose=True,
    )

    # Task 3 — After-load SQL (replaces ehrp2biis_afterload.sql)
    afterload = PythonOperator(
        task_id="afterload_sql",
        python_callable=run_afterload_sql,
    )

    # Task 4 — Action stage load (replaces actstage_load KSH script)
    stage_load = PythonOperator(
        task_id="action_stage_load",
        python_callable=run_action_stage_load,
    )

    preload >> transform >> afterload >> stage_load
