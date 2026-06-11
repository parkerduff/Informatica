"""Shared helpers for the BIIS ETL Airflow DAGs.

Airflow places the DAGs folder on ``sys.path`` so sibling DAG modules can import
this. Job/Spark modules are imported lazily inside task callables so that DAG
*parsing* stays lightweight (and works in the DagBag import test without a Spark
runtime). Failure notifications reuse the same ``send_notification`` config path
as the jobs.
"""
from __future__ import annotations

import datetime as dt
from typing import Any, Dict

DEFAULT_START = dt.datetime(2026, 1, 1)


def on_failure(context: Dict[str, Any]) -> None:
    """on_failure_callback shared by every DAG -- routes to send_notification."""
    try:
        from utils.notifications import send_notification
        from utils.secrets import load_config

        task = context.get("task_instance")
        dag = context.get("dag")
        dag_id = dag.dag_id if dag is not None else "unknown"
        task_id = task.task_id if task is not None else "?"
        send_notification(
            f"BIIS DAG failure: {dag_id}",
            f"Task {task_id} failed: {context.get('exception')}",
            load_config(),
        )
    except Exception:  # never let the callback mask the original failure
        pass


def default_args() -> Dict[str, Any]:
    return {
        "owner": "biis-etl",
        "retries": 1,
        "retry_delay": dt.timedelta(minutes=5),
        "on_failure_callback": on_failure,
    }


def run_date_from_context(context: Dict[str, Any]) -> dt.date:
    ds = context.get("ds")
    return dt.date.fromisoformat(ds) if ds else dt.date.today()
