"""Final validation gate: read the reconciliation summary and fail on any diff.

Exit code 0 only if every reconciled table passed (zero diffs).
"""
from __future__ import annotations

import argparse
import json
import os
import sys


def check(report_dir: str) -> bool:
    summary_path = os.path.join(report_dir, "summary.json")
    if not os.path.exists(summary_path):
        print(f"No summary at {summary_path}; run reconcile.py first", file=sys.stderr)
        return False
    with open(summary_path) as fh:
        summary = json.load(fh)
    failed = [s for s in summary if s["verdict"] != "PASS"]
    for s in summary:
        print(f"  [{s['verdict']}] {s['module']}/{s['table']} (diffs={s['diffs']})")
    if failed:
        print(f"FAIL: {len(failed)} table(s) show deprecation", file=sys.stderr)
        return False
    print(f"All {len(summary)} reconciled table(s) PASS: zero deprecation")
    return True


def main(argv=None) -> None:
    p = argparse.ArgumentParser(description="Check reconciliation results")
    p.add_argument("--report-dir", default="reports/reconciliation/")
    args = p.parse_args(argv)
    if not check(args.report_dir):
        sys.exit(1)


if __name__ == "__main__":
    main()
