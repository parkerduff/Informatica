"""Triggered transfer DAG -- accepts ``agency`` + ``filename`` run params.

Trigger with a config such as::

    {"agency": "nih_cpm", "filename": "nihtest_NIH_PAYROLL_MASTER.dat"}
"""
from __future__ import annotations

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.python import PythonOperator

from _common import DEFAULT_START, default_args


def _transfer(**context):
    from transfers.sftp_transfer import transfer_file
    from utils.secrets import load_config

    params = context["params"]
    agency = params["agency"]
    filename = params["filename"]
    return transfer_file(agency, filename, load_config())


with DAG(
    dag_id="transfer",
    schedule=None,
    start_date=DEFAULT_START,
    catchup=False,
    max_active_runs=1,
    default_args=default_args(),
    params={
        "agency": Param("nih_cpm", type="string"),
        "filename": Param("", type="string"),
    },
    tags=["biis", "transfer"],
) as dag:
    transfer = PythonOperator(task_id="transfer", python_callable=_transfer)
