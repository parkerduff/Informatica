#!/usr/bin/env python3
"""
orchestration.generate_orchestration -- emit Step Functions ASL state machines.

Legacy mapping:
  ksh + cron            -> Step Functions + EventBridge (biweekly pay-period rule)
  independent agencies  -> Parallel state branches (the "child session" analog)
  preload / afterload   -> push-down SQL tasks (glue python-shell / JDBC)
  mailx alerts          -> SNS Publish
  SFTP delivery scripts -> AWS Transfer Family / Lambda SFTP task
  archive_files/remove  -> S3 lifecycle + a cleanup task

Produces one state machine per workflow family plus a master state machine that
fans the independent families out across parallel branches. Each dependent feed
runs: preload SQL -> Glue mapping job -> afterload SQL -> SFTP delivery.
"""
from __future__ import annotations

import json
import os
import sys
from typing import Any, Dict, List

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from migration.jobs import configs
from migration.sql.pushdown import PUSHDOWN_PLAN

OUT_DIR = os.path.join(os.path.dirname(__file__), "statemachines")

# independent families -> parallel branches (mirrors run_pipeline BRANCHES)
FAMILIES = {
    "agency_feeds": ["CPM_NIH", "CPM_OIG", "CPM_CDC", "FDA_Leave"],
    "pseudossn": ["Pseudossn"],
    "ehrp": ["EHRP2BIIS_UPDATE"],
    "reference": ["Pay_Calendar", "COMPTIME"],
}


def _glue_task(folder: str, nxt: str = None, end: bool = False) -> Dict[str, Any]:
    task = {
        "Type": "Task",
        "Resource": "arn:aws:states:::glue:startJobRun.sync",
        "Parameters": {
            "JobName": f"informatica-{folder.lower()}",
            "Arguments": {
                "--folder": folder,
                "--in-dir.$": "$.input_prefix",
                "--out-dir.$": "$.output_prefix",
            },
        },
        "Retry": [{"ErrorEquals": ["States.ALL"], "MaxAttempts": 2,
                   "IntervalSeconds": 30, "BackoffRate": 2.0}],
        "Catch": [{"ErrorEquals": ["States.ALL"], "Next": "NotifyFailure",
                   "ResultPath": "$.error"}],
    }
    if end:
        task["End"] = True
    else:
        task["Next"] = nxt
    return task


def _sql_task(name: str, nxt: str) -> Dict[str, Any]:
    return {
        "Type": "Task",
        "Resource": "arn:aws:states:::glue:startJobRun.sync",
        "Comment": f"Push-down PL/SQL step: {name}",
        "Parameters": {"JobName": "informatica-pushdown-sql",
                        "Arguments": {"--step": name}},
        "Next": nxt,
    }


def _sftp_task(folder: str, nxt: str) -> Dict[str, Any]:
    return {
        "Type": "Task",
        "Resource": "arn:aws:states:::lambda:invoke",
        "Comment": "SFTP delivery via AWS Transfer Family / Lambda",
        "Parameters": {"FunctionName": "informatica-sftp-delivery",
                        "Payload": {"folder": folder, "target.$": "$.output_prefix"}},
        "Next": nxt,
    }


def _notify_states() -> Dict[str, Any]:
    return {
        "NotifyFailure": {
            "Type": "Task",
            "Resource": "arn:aws:states:::sns:publish",
            "Parameters": {"TopicArn.$": "$.alert_topic_arn",
                            "Subject": "Informatica->Glue pipeline failure",
                            "Message.$": "$.error"},
            "Next": "FailState",
        },
        "FailState": {"Type": "Fail", "Error": "PipelineError"},
    }


def branch_for_folder(folder: str) -> Dict[str, Any]:
    """A single feed's sub-DAG: preload -> job -> afterload -> sftp."""
    family_key = "EHRP2BIIS_UPDATE" if folder == "EHRP2BIIS_UPDATE" else None
    states: Dict[str, Any] = {}
    start = f"{folder}_Job"
    pre = PUSHDOWN_PLAN.get(family_key, {}).get("preload", []) if family_key else []
    post = PUSHDOWN_PLAN.get(family_key, {}).get("afterload", []) if family_key else []

    seq: List[str] = []
    for p in pre:
        seq.append(("sql", f"{folder}_preload_{p}", p))
    seq.append(("job", start, folder))
    for p in post:
        seq.append(("sql", f"{folder}_afterload_{p}", p))
    seq.append(("sftp", f"{folder}_SFTP", folder))

    start = seq[0][1]
    for i, (kind, sname, arg) in enumerate(seq):
        is_last = i == len(seq) - 1
        nxt = None if is_last else seq[i + 1][1]
        if kind == "sql":
            states[sname] = _sql_task(arg, nxt)
        elif kind == "job":
            states[sname] = _glue_task(arg, nxt=nxt, end=False if nxt else True)
            if nxt is None:
                states[sname]["End"] = True
        elif kind == "sftp":
            st = _sftp_task(arg, nxt="__END__")
            st.pop("Next")
            st["End"] = True
            states[sname] = st
    states.update(_notify_states())
    return {"StartAt": start, "States": states}


def family_machine(family: str, folders: List[str]) -> Dict[str, Any]:
    """Parallel state running each independent folder branch concurrently."""
    branches = [branch_for_folder(f) for f in folders]
    return {
        "Comment": f"Workflow family '{family}' — parallel independent feeds",
        "StartAt": "RunFeeds",
        "States": {
            "RunFeeds": {
                "Type": "Parallel",
                "Branches": branches,
                "Next": "Done",
                "Catch": [{"ErrorEquals": ["States.ALL"], "Next": "NotifyFailure",
                           "ResultPath": "$.error"}],
            },
            "Done": {"Type": "Succeed"},
            **_notify_states(),
        },
    }


def master_machine() -> Dict[str, Any]:
    branches = []
    for family, folders in FAMILIES.items():
        branches.append({
            "StartAt": f"Family_{family}",
            "States": {f"Family_{family}": {
                "Type": "Parallel",
                "Branches": [branch_for_folder(f) for f in folders],
                "End": True,
            }},
        })
    return {
        "Comment": "Master EHRP->BIIS pipeline — all independent families in parallel",
        "StartAt": "AllFamilies",
        "States": {
            "AllFamilies": {
                "Type": "Parallel",
                "Branches": branches,
                "Next": "Succeed",
                "Catch": [{"ErrorEquals": ["States.ALL"], "Next": "NotifyFailure",
                           "ResultPath": "$.error"}],
            },
            "Succeed": {"Type": "Succeed"},
            **_notify_states(),
        },
    }


def main() -> int:
    os.makedirs(OUT_DIR, exist_ok=True)
    written = []
    for family, folders in FAMILIES.items():
        path = os.path.join(OUT_DIR, f"{family}.asl.json")
        with open(path, "w") as fh:
            json.dump(family_machine(family, folders), fh, indent=2)
        written.append(path)
    mpath = os.path.join(OUT_DIR, "master.asl.json")
    with open(mpath, "w") as fh:
        json.dump(master_machine(), fh, indent=2)
    written.append(mpath)
    for w in written:
        print("[orchestration] wrote", os.path.relpath(w, os.path.dirname(__file__)))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
