"""CPM daily DAG.

load_common -> [cpm_nih, cpm_oig, cpm_cdc] (parallel)
            -> [transfer_nih, transfer_oig, transfer_cdc] (parallel)
            -> archive -> notify
"""
from __future__ import annotations

from airflow import DAG
from airflow.operators.python import PythonOperator

from _common import DEFAULT_START, default_args, run_date_from_context

AGENCIES = ("nih", "oig", "cdc")
FILENAMES = {
    "nih": "nihtest_NIH_PAYROLL_MASTER.dat",
    "oig": "oigsgndec_SKPAYROLL_MASTER.dat",
    "cdc": "cdcskel_WS_PAY_OUT_REC.dat",
}


def _load_common(**_):
    # Pay-period lookup gate shared by all three agency extracts.
    from utils.db import get_current_pay_period
    from utils.secrets import get_db_secret, load_config
    from utils.spark import get_spark

    config = load_config()
    return get_current_pay_period(get_spark("cpm-common"), config, get_db_secret(config))


def _run_agency(agency: str):
    def _inner(**_):
        from jobs.cpm.cpm_common import run_agency
        from utils.secrets import load_config

        return run_agency(agency, load_config())
    return _inner


def _transfer(agency: str):
    def _inner(**_):
        from transfers.sftp_transfer import transfer_file
        from utils.secrets import load_config

        config = load_config()
        return transfer_file(agency, FILENAMES[agency], config)
    return _inner


def _archive(**context):
    from transfers.sftp_transfer import archive_files
    from utils.secrets import load_config

    config = load_config()
    pp = run_date_from_context(context).strftime("%Y%m%d")
    return archive_files(config.paths.staging, config.paths.archive, pp)


def _notify(**_):
    from utils.notifications import send_notification
    from utils.secrets import load_config

    send_notification("CPM: complete", "CPM extracts + transfers finished.", load_config())


with DAG(
    dag_id="cpm",
    schedule="@daily",
    start_date=DEFAULT_START,
    catchup=False,
    max_active_runs=1,
    default_args=default_args(),
    tags=["biis", "cpm"],
) as dag:
    load_common = PythonOperator(task_id="load_common", python_callable=_load_common)
    archive = PythonOperator(task_id="archive", python_callable=_archive)
    notify = PythonOperator(task_id="notify", python_callable=_notify)

    for agency in AGENCIES:
        extract = PythonOperator(task_id=f"cpm_{agency}", python_callable=_run_agency(agency))
        transfer = PythonOperator(task_id=f"transfer_{agency}", python_callable=_transfer(agency))
        load_common >> extract >> transfer >> archive

    archive >> notify
