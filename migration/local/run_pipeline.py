#!/usr/bin/env python3
"""
local.run_pipeline -- end-to-end local execution + reconciliation driver.

This is the local analog of the Step Functions master state machine: it runs the
independent agency feeds in parallel branches (ThreadPool == Parallel state),
executes each converted job through its classified engine, produces the golden
baseline for the same inputs, and reconciles field-by-field.

Outputs (consumed by the HTML report generators):
  migration/reports/_results_<mode>.json
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, List

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from migration.baseline import reference as REF
from migration.jobs import configs
from migration.jobs.runner import _resolver_from_dir, run_job
from migration.lib import engine_pandas as EP
from migration.recon import reconcile as RC

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
DATA_ROOT = os.path.join(REPO_ROOT, "migration", "local", "data")
OUT_ROOT = os.path.join(REPO_ROOT, "migration", "local", "out")
REPORT_DIR = os.path.join(REPO_ROOT, "migration", "reports")

# parallel "child session" branches -- independent agency feeds run concurrently.
BRANCHES = {
    "agency_feeds": ["CPM_NIH", "CPM_OIG", "CPM_CDC", "FDA_Leave"],
    "pseudossn": ["Pseudossn"],
    "ehrp": ["EHRP2BIIS_UPDATE"],
    "reference": ["Pay_Calendar", "COMPTIME"],
}


def _run_one(folder: str, mode: str) -> Dict[str, Any]:
    pdef = configs.build(folder)
    in_dir = os.path.join(DATA_ROOT, mode, folder)
    out_dir = os.path.join(OUT_ROOT, mode)
    resolver = _resolver_from_dir(pdef, in_dir)

    conv = run_job(folder, in_dir, out_dir)             # converted job (writes outputs)

    t0 = time.time()
    base = REF.run(pdef, resolver)
    base.metrics["runtime_sec"] = round(time.time() - t0, 4)

    cols = [{"name": c.name, "datatype": c.datatype, "scale": c.scale,
             "precision": c.precision} for c in pdef.target.columns]
    report = RC.reconcile(folder, conv, base, cols)
    report["engine"] = pdef.engine
    report["mapping"] = pdef.mapping
    report["mode"] = mode
    return report


def run(mode: str, folders: List[str], parallel: bool = True) -> Dict[str, Any]:
    os.makedirs(REPORT_DIR, exist_ok=True)
    results: Dict[str, Any] = {}

    t0 = time.time()
    if parallel:
        with ThreadPoolExecutor(max_workers=4) as ex:
            futs = {ex.submit(_run_one, f, mode): f for f in folders}
            for fut in as_completed(futs):
                f = futs[fut]
                results[f] = fut.result()
    else:
        for f in folders:
            results[f] = _run_one(f, mode)

    wall = round(time.time() - t0, 3)
    summary = {
        "mode": mode,
        "wall_clock_sec": wall,
        "branches": BRANCHES,
        "folders": folders,
        "results": results,
        "overall": _verdict(results),
    }
    with open(os.path.join(REPORT_DIR, f"_results_{mode}.json"), "w") as fh:
        json.dump(summary, fh, indent=2, default=str)
    _print_summary(summary)
    return summary


def _verdict(results: Dict[str, Any]) -> Dict[str, Any]:
    passed = sum(1 for r in results.values() if r["verdict"] == "PASS")
    total = len(results)
    return {
        "jobs_pass": passed, "jobs_total": total,
        "business": "PASS" if passed == total else "FAIL",
        "technical": "PASS" if passed == total else "FAIL",
        "functional": "PASS" if passed == total else "FAIL",
        "verdict": "PASS" if passed == total else "FAIL",
    }


def _print_summary(summary: Dict[str, Any]) -> None:
    print(f"\n=== reconciliation ({summary['mode']}) wall={summary['wall_clock_sec']}s ===")
    for f, r in sorted(summary["results"].items()):
        fr = r["field"]
        print(f"  {f:18s} {r['verdict']:4s} rows={fr['rows_compared']:>7} "
              f"cells={fr['total_cells']:>8} mism={fr['cell_mismatches']:>4} "
              f"errParity={r['error_parity']['keys_match']} "
              f"cntParity={r['counter_parity']['match']} engine={r['engine']}")
    o = summary["overall"]
    print(f"  OVERALL: {o['verdict']}  ({o['jobs_pass']}/{o['jobs_total']} jobs)")


def main(argv: List[str]) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", choices=["functional", "performance"], default="functional")
    ap.add_argument("--folders", nargs="*", default=configs.ALL_FOLDERS)
    ap.add_argument("--serial", action="store_true")
    args = ap.parse_args(argv[1:])
    summary = run(args.mode, args.folders, parallel=not args.serial)
    return 0 if summary["overall"]["verdict"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
