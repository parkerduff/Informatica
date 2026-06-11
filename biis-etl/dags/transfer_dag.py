"""Manually-triggered transfer DAG with agency/filename params."""
import datetime

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.python import PythonOperator

from dag_common import BASE, DAG_KWARGS  # noqa: F401


def task_transfer(params=None, **_):
    from transfers.sftp_transfer import transfer_file
    from utils.secrets import load_config

    config = load_config()
    return transfer_file(params["agency"], params["filename"], config)


def task_notify(ti=None, **_):
    from utils.notifications import send_notification
    from utils.secrets import load_config

    config = load_config()
    remote = ti.xcom_pull(task_ids="transfer")
    send_notification("Transfer DAG completed", f"Delivered to {remote}", config)


with DAG(
    dag_id="transfer",
    start_date=datetime.datetime(2026, 1, 1),
    params={
        "agency": Param("nih_cpm", type="string"),
        "filename": Param("cpm_nih_payroll.txt", type="string"),
    },
    **DAG_KWARGS,
) as dag:
    transfer = PythonOperator(task_id="transfer", python_callable=task_transfer)
    notify = PythonOperator(task_id="notify", python_callable=task_notify)

    transfer >> notify
