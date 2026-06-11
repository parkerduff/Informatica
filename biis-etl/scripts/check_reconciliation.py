"""Final gate: fail the build if any reconciled table shows a non-zero diff."""
import argparse
import json
import os
import sys


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--report-dir", default="reports/reconciliation/")
    args = parser.parse_args()

    path = os.path.join(args.report_dir, "reconciliation.json")
    if not os.path.exists(path):
        print(f"FAIL: no reconciliation report at {path}", file=sys.stderr)
        return 1
    with open(path) as f:
        results = json.load(f)

    failures = []
    for r in results:
        ok = r.get("passed") and r.get("diff_count", 1) == 0
        status = "PASS" if ok else "FAIL"
        print(f"{status} {r['table']}: diffs={r.get('diff_count')} "
              f"rows={r.get('actual_count')}/{r.get('expected_count')}")
        if not ok:
            failures.append(r["table"])
            if r.get("diff_sample"):
                print(f"  sample diffs:\n{r['diff_sample']}")

    if failures:
        print(f"\nRECONCILIATION FAILED for: {failures}", file=sys.stderr)
        return 1
    print("\nAll tables reconciled with ZERO diffs.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
