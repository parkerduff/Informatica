"""CPM DAG: load_common -> [nih, oig, cdc] -> [transfer_*] -> archive -> notify."""
import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from dag_common import BASE, DAG_KWARGS  # noqa: F401

AGENCY_FILES = {
    "nih": ("nih_cpm", "cpm_nih_payroll.txt"),
    "oig": ("oig", "cpm_oig_payroll.txt"),
    "cdc": ("cdc", "cpm_cdc_payroll.txt"),
}


def task_load_common(**_):
    from jobs.cpm.cpm_common import get_pay_period
    from jobs.spark_common import get_spark
    from utils.secrets import get_secret, load_config

    config = load_config()
    secret = get_secret("biis", config)
    spark = get_spark("airflow-cpm-common")
    try:
        pp = get_pay_period(spark, config, secret)
        return {"pp_num": pp["pp_num"], "pp_end_year": pp["pp_end_year"]}
    finally:
        spark.stop()


def make_extract(agency):
    def _run(ds=None, **_):
        import importlib

        mod = importlib.import_module(f"jobs.cpm.cpm_{agency}")
        return mod.run(env=None, run_date=ds)
    return _run


def make_transfer(agency):
    def _run(**_):
        from transfers.sftp_transfer import transfer_file
        from utils.secrets import load_config

        config = load_config()
        route, filename = AGENCY_FILES[agency]
        return transfer_file(route, filename, config)
    return _run


def task_archive(ti=None, **_):
    from transfers.sftp_transfer import archive_files
    from utils.secrets import load_config

    config = load_config()
    pp = ti.xcom_pull(task_ids="load_common")
    pay_period = f"{pp['pp_end_year']}{pp['pp_num']:02d}"
    return archive_files(config["paths"]["staging"], config["paths"]["archive"], pay_period)


def task_notify(ti=None, **_):
    from utils.notifications import send_notification
    from utils.secrets import load_config

    config = load_config()
    archived = ti.xcom_pull(task_ids="archive") or []
    send_notification("CPM DAG completed", f"Archived {len(archived)} agency files", config)


with DAG(dag_id="cpm", start_date=datetime.datetime(2026, 1, 1), **DAG_KWARGS) as dag:
    load_common = PythonOperator(task_id="load_common", python_callable=task_load_common)
    archive = PythonOperator(task_id="archive", python_callable=task_archive)
    notify = PythonOperator(task_id="notify", python_callable=task_notify)

    transfers = []
    for agency in AGENCY_FILES:
        extract = PythonOperator(task_id=f"extract_{agency}", python_callable=make_extract(agency))
        transfer = PythonOperator(task_id=f"transfer_{agency}", python_callable=make_transfer(agency))
        load_common >> extract >> transfer
        transfers.append(transfer)
    transfers >> archive >> notify
