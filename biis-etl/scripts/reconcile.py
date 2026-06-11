#!/usr/bin/env python3
"""Run reconciliation for one or more modules and write reports."""
from __future__ import annotations

import argparse
from pathlib import Path

from utils.reconciliation import MODULE_TABLES, generate_report, reconcile_module
from utils.secrets import load_config
from utils.spark import get_spark

ROOT = Path(__file__).resolve().parents[1]


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--env", default=None)
    ap.add_argument("--modules", default="all",
                    help="comma-separated module names, or 'all'")
    ap.add_argument("--golden-dir", default=str(ROOT / "tests" / "fixtures" / "golden"))
    ap.add_argument("--report-dir", default=str(ROOT / "reports" / "reconciliation"))
    args = ap.parse_args()

    config = load_config(args.env)
    spark = get_spark("biis-reconcile")

    if args.modules == "all":
        modules = list(MODULE_TABLES.keys())
    else:
        modules = [m.strip() for m in args.modules.split(",") if m.strip()]

    all_results = []
    for module in modules:
        all_results.extend(reconcile_module(spark, config, module, args.golden_dir))

    json_path = generate_report(all_results, args.report_dir)
    passed = sum(1 for r in all_results if r.passed)
    print(f"Reconciliation: {passed}/{len(all_results)} tables passed. Report: {json_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
