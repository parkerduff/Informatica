"""Shared Airflow DAG defaults and failure callback."""
import os
import sys

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if BASE not in sys.path:
    sys.path.insert(0, BASE)


def on_failure_callback(context):
    from utils.notifications import send_notification
    from utils.secrets import load_config

    config = load_config()
    ti = context.get("task_instance")
    send_notification(
        f"BIIS DAG failure: {ti.dag_id}.{ti.task_id}",
        f"Run {context.get('run_id')} failed: {context.get('exception')}",
        config,
    )


DEFAULT_ARGS = {
    "owner": "biis",
    "retries": 0,
    "on_failure_callback": on_failure_callback,
}

DAG_KWARGS = {
    "catchup": False,
    "max_active_runs": 1,
    "default_args": DEFAULT_ARGS,
    "schedule": None,
}
