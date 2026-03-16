"""
BIIS ETL Airflow DAG

Orchestrates all BIIS ETL jobs in the correct dependency order.
Replaces the Informatica PowerCenter workflow scheduler.

Dependency chain:
    pay_calendar >> pseudossn >> [comptime, ehrp2biis_preload]
    ehrp2biis_preload >> ehrp2biis_main_etl >> ehrp2biis_afterload
    ehrp2biis_afterload >> cpm_staging >> [cpm_nih, cpm_oig, cpm_cdc, cpm_afps]
    [cpm_nih, cpm_oig, cpm_cdc, cpm_afps] >> fda_validation >> les
    les >> [transfer_nih_les, transfer_nih_cpm, transfer_oig, transfer_fda,
            transfer_cdc, transfer_afps]
    transfers >> [archive_files, remove_old_files]
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

# Default DAG arguments
default_args = {
    "owner": "biis-etl",
    "depends_on_past": False,
    "email": [
        "peter.chen@hhs.gov",
        "nathan.knight@hhs.gov",
        "marvin.simon@hhs.gov",
    ],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2024, 1, 1),
}

dag = DAG(
    dag_id="biis_etl_pipeline",
    default_args=default_args,
    description="HHS BIIS Data Warehouse ETL Pipeline (PySpark)",
    schedule_interval="0 6 * * 1-5",  # Weekdays at 6 AM ET
    catchup=False,
    max_active_runs=1,
    tags=["biis", "etl", "hhs"],
)


# ---------------------------------------------------------------------------
# Task functions — each wraps the corresponding job's run() entry point
# ---------------------------------------------------------------------------

def _run_pay_calendar(**kwargs):
    from pyspark_migration.jobs.pay_calendar.pay_calendar_job import run
    pp_num = kwargs.get("params", {}).get("pp_num")
    pp_end_year = kwargs.get("params", {}).get("pp_end_year")
    run(pp_num=pp_num, pp_end_year=pp_end_year)


def _run_pseudossn(**kwargs):
    from pyspark_migration.jobs.pseudossn.pseudossn_job import run
    run()


def _run_comptime(**kwargs):
    from pyspark_migration.jobs.comptime.comptime_job import run
    run()


def _run_ehrp2biis_preload(**kwargs):
    from pyspark_migration.jobs.ehrp2biis.preload import run
    run()


def _run_ehrp2biis_main_etl(**kwargs):
    from pyspark_migration.jobs.ehrp2biis.main_etl import run
    run()


def _run_ehrp2biis_afterload(**kwargs):
    from pyspark_migration.jobs.ehrp2biis.afterload import run
    run()


def _run_cpm_staging(**kwargs):
    from pyspark_migration.jobs.cpm.staging import run
    run()


def _run_cpm_nih(**kwargs):
    from pyspark_migration.jobs.cpm.cpm_nih import run
    run()


def _run_cpm_oig(**kwargs):
    from pyspark_migration.jobs.cpm.cpm_oig import run
    run()


def _run_cpm_cdc(**kwargs):
    from pyspark_migration.jobs.cpm.cpm_cdc import run
    run()


def _run_cpm_afps(**kwargs):
    from pyspark_migration.jobs.cpm.cpm_afps import run
    run()


def _run_fda_validation(**kwargs):
    from pyspark_migration.jobs.fda_validation.fda_validation_job import run
    run()


def _run_les(**kwargs):
    from pyspark_migration.jobs.les.les_job import run
    run()


def _run_transfer_nih_les(**kwargs):
    from pyspark_migration.jobs.file_transfers.transfer_jobs import transfer_nih_les
    transfer_nih_les()


def _run_transfer_nih_cpm(**kwargs):
    from pyspark_migration.jobs.file_transfers.transfer_jobs import transfer_nih_cpm
    transfer_nih_cpm()


def _run_transfer_oig(**kwargs):
    from pyspark_migration.jobs.file_transfers.transfer_jobs import transfer_oig
    transfer_oig()


def _run_transfer_fda(**kwargs):
    from pyspark_migration.jobs.file_transfers.transfer_jobs import transfer_fda
    transfer_fda()


def _run_transfer_cdc(**kwargs):
    from pyspark_migration.jobs.file_transfers.transfer_jobs import transfer_cdc
    transfer_cdc()


def _run_transfer_afps(**kwargs):
    from pyspark_migration.jobs.file_transfers.transfer_jobs import transfer_afps
    transfer_afps()


def _run_archive(**kwargs):
    from pyspark_migration.jobs.file_transfers.transfer_jobs import archive_files
    archive_files()


def _run_remove_old(**kwargs):
    from pyspark_migration.jobs.file_transfers.transfer_jobs import remove_old_files
    remove_old_files()


# ---------------------------------------------------------------------------
# Task definitions
# ---------------------------------------------------------------------------

pay_calendar = PythonOperator(
    task_id="pay_calendar",
    python_callable=_run_pay_calendar,
    dag=dag,
)

pseudossn = PythonOperator(
    task_id="pseudossn",
    python_callable=_run_pseudossn,
    dag=dag,
)

comptime = PythonOperator(
    task_id="comptime",
    python_callable=_run_comptime,
    dag=dag,
)

ehrp2biis_preload = PythonOperator(
    task_id="ehrp2biis_preload",
    python_callable=_run_ehrp2biis_preload,
    dag=dag,
)

ehrp2biis_main_etl = PythonOperator(
    task_id="ehrp2biis_main_etl",
    python_callable=_run_ehrp2biis_main_etl,
    dag=dag,
)

ehrp2biis_afterload = PythonOperator(
    task_id="ehrp2biis_afterload",
    python_callable=_run_ehrp2biis_afterload,
    dag=dag,
)

cpm_staging = PythonOperator(
    task_id="cpm_staging",
    python_callable=_run_cpm_staging,
    dag=dag,
)

cpm_nih = PythonOperator(
    task_id="cpm_nih",
    python_callable=_run_cpm_nih,
    dag=dag,
)

cpm_oig = PythonOperator(
    task_id="cpm_oig",
    python_callable=_run_cpm_oig,
    dag=dag,
)

cpm_cdc = PythonOperator(
    task_id="cpm_cdc",
    python_callable=_run_cpm_cdc,
    dag=dag,
)

cpm_afps = PythonOperator(
    task_id="cpm_afps",
    python_callable=_run_cpm_afps,
    dag=dag,
)

fda_validation = PythonOperator(
    task_id="fda_validation",
    python_callable=_run_fda_validation,
    dag=dag,
)

les = PythonOperator(
    task_id="les",
    python_callable=_run_les,
    dag=dag,
)

transfer_nih_les = PythonOperator(
    task_id="transfer_nih_les",
    python_callable=_run_transfer_nih_les,
    dag=dag,
)

transfer_nih_cpm = PythonOperator(
    task_id="transfer_nih_cpm",
    python_callable=_run_transfer_nih_cpm,
    dag=dag,
)

transfer_oig = PythonOperator(
    task_id="transfer_oig",
    python_callable=_run_transfer_oig,
    dag=dag,
)

transfer_fda = PythonOperator(
    task_id="transfer_fda",
    python_callable=_run_transfer_fda,
    dag=dag,
)

transfer_cdc = PythonOperator(
    task_id="transfer_cdc",
    python_callable=_run_transfer_cdc,
    dag=dag,
)

transfer_afps = PythonOperator(
    task_id="transfer_afps",
    python_callable=_run_transfer_afps,
    dag=dag,
)

archive = PythonOperator(
    task_id="archive_files",
    python_callable=_run_archive,
    dag=dag,
)

remove_old = PythonOperator(
    task_id="remove_old_files",
    python_callable=_run_remove_old,
    dag=dag,
)

# ---------------------------------------------------------------------------
# Dependencies
# ---------------------------------------------------------------------------

# Phase 1: Pay Calendar first
pay_calendar >> pseudossn

# Phase 2: PseudoSSN enables COMPTIME and EHRP2BIIS
pseudossn >> [comptime, ehrp2biis_preload]

# Phase 4: EHRP2BIIS pipeline
ehrp2biis_preload >> ehrp2biis_main_etl >> ehrp2biis_afterload

# Phase 5: CPM depends on EHRP2BIIS completion
ehrp2biis_afterload >> cpm_staging
cpm_staging >> [cpm_nih, cpm_oig, cpm_cdc, cpm_afps]

# Phase 7: FDA validation after all CPM agency processing
[cpm_nih, cpm_oig, cpm_cdc, cpm_afps] >> fda_validation

# Phase 6: LES after FDA validation
fda_validation >> les

# Phase 8: File transfers after LES
les >> [transfer_nih_les, transfer_nih_cpm, transfer_oig,
        transfer_fda, transfer_cdc, transfer_afps]

# Maintenance after all transfers
[transfer_nih_les, transfer_nih_cpm, transfer_oig,
 transfer_fda, transfer_cdc, transfer_afps] >> archive >> remove_old
