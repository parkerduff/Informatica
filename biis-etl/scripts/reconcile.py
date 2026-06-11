"""Source-target reconciliation CLI.

Compares the job output now in the database against the golden expected
datasets, writing one HTML report per table plus a JSON summary.

Exit code 0 = zero diffs across all requested modules; 1 = deprecation found.
"""
from __future__ import annotations

import argparse
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from scripts._common import GOLDEN_DIR  # noqa: E402
from utils import db  # noqa: E402
from utils.config import get_config  # noqa: E402
from utils.reconciliation import reconcile_table, write_report  # noqa: E402
from utils.recon_targets import RECON_TARGETS  # noqa: E402


def run(env: str, modules, report_dir: str) -> bool:
    cfg = get_config(env)
    conn = db.get_connection(cfg)
    summary = []
    all_pass = True
    try:
        for module in modules:
            for table, keys in RECON_TARGETS[module]:
                result = reconcile_table(None, conn, module, table, keys, GOLDEN_DIR)
                path = write_report(result, report_dir)
                all_pass = all_pass and result.passed
                summary.append({
                    "module": module, "table": table, "verdict": result.verdict,
                    "expected": result.expected_count, "actual": result.actual_count,
                    "diffs": result.diff_count, "report": os.path.basename(path),
                })
                status = "PASS" if result.passed else "FAIL"
                print(f"  [{status}] {module}/{table}: "
                      f"expected={result.expected_count} actual={result.actual_count} "
                      f"diffs={result.diff_count}")
                if not result.passed and result.diff_sample:
                    print("    " + result.diff_sample.replace("\n", "\n    "))
    finally:
        conn.close()

    os.makedirs(report_dir, exist_ok=True)
    with open(os.path.join(report_dir, "summary.json"), "w") as fh:
        json.dump(summary, fh, indent=2)
    return all_pass


def main(argv=None) -> None:
    p = argparse.ArgumentParser(description="Source-target reconciliation")
    p.add_argument("--env", default="test", choices=["test", "prod"])
    p.add_argument("--modules", default=None,
                   help="Comma-separated module names (default: all)")
    p.add_argument("--report-dir", default="reports/reconciliation/")
    args = p.parse_args(argv)
    if not args.modules or args.modules.strip().lower() == "all":
        modules = list(RECON_TARGETS)
    else:
        modules = args.modules.split(",")
    ok = run(args.env, modules, args.report_dir)
    if not ok:
        print("RECONCILIATION FAILED: differences detected", file=sys.stderr)
        sys.exit(1)
    print("RECONCILIATION OK: zero diffs")


if __name__ == "__main__":
    main()
