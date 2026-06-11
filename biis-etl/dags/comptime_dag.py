"""COMPTIME DAG: get_pay_period -> load_file -> build_counters."""
import datetime
import os

from airflow import DAG
from airflow.operators.python import PythonOperator

from dag_common import BASE, DAG_KWARGS  # noqa: F401


def task_get_pay_period(**_):
    from jobs.spark_common import get_spark
    from utils.db import get_current_pay_period
    from utils.secrets import get_secret, load_config

    config = load_config()
    secret = get_secret("biis", config)
    spark = get_spark("airflow-comptime-pp")
    try:
        pp = get_current_pay_period(spark, config, secret)
        return {"pp_num": pp["pp_num"], "pp_end_year": pp["pp_end_year"]}
    finally:
        spark.stop()


def task_load_file(**_):
    from jobs.comptime import run
    from utils.secrets import load_config

    config = load_config()
    path = os.path.join(config["paths"]["landing"], "comptime_input.csv")
    return run(env=None, file_path=path)


def task_build_counters(ti=None, **_):
    from utils.db import pyodbc_connection
    from utils.secrets import get_secret, load_config

    config = load_config()
    secret = get_secret("biis", config)
    count = ti.xcom_pull(task_ids="load_file")
    pp = ti.xcom_pull(task_ids="get_pay_period")
    with pyodbc_connection(secret, config) as conn:
        cur = conn.cursor()
        cur.execute(
            "SELECT COUNT(*) FROM COUNTER_TBL WHERE PROCESS_NAME = 'COMPTIME' "
            "AND PP_NUM = ? AND PP_END_YEAR = ?",
            pp["pp_num"], pp["pp_end_year"],
        )
        n = cur.fetchone()[0]
    return {"loaded": count, "counter_rows": n}


with DAG(dag_id="comptime", start_date=datetime.datetime(2026, 1, 1), **DAG_KWARGS) as dag:
    get_pay_period = PythonOperator(task_id="get_pay_period", python_callable=task_get_pay_period)
    load_file = PythonOperator(task_id="load_file", python_callable=task_load_file)
    build_counters = PythonOperator(task_id="build_counters", python_callable=task_build_counters)

    get_pay_period >> load_file >> build_counters
