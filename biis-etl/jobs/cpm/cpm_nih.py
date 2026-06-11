"""CPM NIH agency extract (migrated from XML/CPM_NIH)."""
from __future__ import annotations

from jobs.cpm.cpm_common import run_agency
from utils.spark import parse_args


def run(env: str = "test", run_date=None, spark=None) -> int:
    return run_agency("NIH", env=env, run_date=run_date, spark=spark)


def main(argv=None) -> None:
    args = parse_args("CPM NIH extract", argv)
    n = run(env=args.env, run_date=args.run_date)
    print(f"CPM NIH OK: {n} record(s)")


if __name__ == "__main__":
    main()
