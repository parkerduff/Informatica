"""Shared Airflow DAG helpers for the BIIS ETL workflows.

The task callables import the job modules lazily and resolve the environment
from the ``biis_env`` Airflow Variable (default ``test``) and the logical date,
so DAG *parsing* never requires Spark or a database connection.
"""
from __future__ import annotations

import datetime as dt

default_args = {
    "owner": "biis",
    "retries": 1,
    "retry_delay": dt.timedelta(minutes=5),
    "depends_on_past": False,
}

DAG_DEFAULTS = dict(
    schedule="@daily",
    start_date=dt.datetime(2026, 1, 1),
    catchup=False,
    default_args=default_args,
)


def _env() -> str:
    from airflow.models import Variable

    return Variable.get("biis_env", default_var="test")


def _run_date(context) -> dt.date:
    return context["logical_date"].date()
