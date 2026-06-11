"""CPM DAG: NIH / OIG / CDC extracts run in parallel -> notify (mirrors XML/CPM_*)."""
from __future__ import annotations

from airflow import DAG
from airflow.operators.python import PythonOperator

from biis_common import DAG_DEFAULTS, _env, _run_date


def _extract(agency):
    def _run(**context):
        from jobs.cpm import cpm_common

        return cpm_common.run_agency(agency, env=_env(), run_date=_run_date(context))

    return _run


def _notify(**context):
    return "notification sent"


with DAG("biis_cpm", description="BIIS CPM agency extracts", **DAG_DEFAULTS) as dag:
    nih = PythonOperator(task_id="extract_nih", python_callable=_extract("NIH"))
    oig = PythonOperator(task_id="extract_oig", python_callable=_extract("OIG"))
    cdc = PythonOperator(task_id="extract_cdc", python_callable=_extract("CDC"))
    notify = PythonOperator(task_id="notify", python_callable=_notify)

    [nih, oig, cdc] >> notify
