"""PseudoSSN DAG: parse -> dedup -> archive -> notify (mirrors XML/Pseudossn)."""
from __future__ import annotations

import os

from airflow import DAG
from airflow.operators.python import PythonOperator

from biis_common import DAG_DEFAULTS, _env, _run_date


def _parse_dedup_archive(**context):
    from jobs import pseudossn
    from utils.config import get_config

    cfg = get_config(_env())
    path = os.path.join(cfg.paths["landing"], "pseudossn_input.dat")
    return pseudossn.run(env=_env(), file_path=path, run_date=_run_date(context))


def _notify(**context):
    return "notification sent"


with DAG("biis_pseudossn", description="BIIS PseudoSSN", **DAG_DEFAULTS) as dag:
    process = PythonOperator(task_id="parse_dedup_archive", python_callable=_parse_dedup_archive)
    notify = PythonOperator(task_id="notify", python_callable=_notify)

    process >> notify
