#!/usr/bin/env python3
"""Final gate: exit non-zero if any reconciled table has diffs."""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--report-dir", default=str(ROOT / "reports" / "reconciliation"))
    args = ap.parse_args()

    report = Path(args.report_dir) / "reconciliation.json"
    if not report.exists():
        print(f"FAIL: reconciliation report not found at {report}", file=sys.stderr)
        return 1

    payload = json.loads(report.read_text())
    failures = [r for r in payload["results"] if not r["passed"]]

    if failures:
        print(f"RECONCILIATION FAILED: {len(failures)} table(s) with diffs:", file=sys.stderr)
        for r in failures:
            print(
                f"  - {r['module']}.{r['table']}: "
                f"expected={r['expected_count']} actual={r['actual_count']} "
                f"diffs={r['diff_count']} schema_match={r['schema_match']} "
                f"error={r.get('error')}",
                file=sys.stderr,
            )
            if r.get("column_diffs"):
                print(f"      column_diffs={r['column_diffs']}", file=sys.stderr)
            if r.get("diff_sample"):
                print(f"      sample: {r['diff_sample'][:500]}", file=sys.stderr)
        return 1

    print(f"RECONCILIATION PASSED: all {payload['total_tables']} tables show zero diffs")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
