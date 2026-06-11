"""Print a row-count summary for every table and fail on missing tables."""
from __future__ import annotations

import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils import db  # noqa: E402
from utils.config import get_config  # noqa: E402
from utils.schemas import TABLES  # noqa: E402


def verify(env: str) -> None:
    cfg = get_config(env)
    missing = []
    for table in TABLES:
        try:
            n = db.count(cfg, table)
            print(f"{table + ':':<32} {n:>6} rows OK")
        except Exception as exc:  # noqa: BLE001
            missing.append(table)
            print(f"{table + ':':<32} MISSING ({exc})")
    if missing:
        print(f"FAIL: {len(missing)} table(s) missing: {missing}", file=sys.stderr)
        sys.exit(1)
    print("Seed verification OK")


def main(argv=None) -> None:
    p = argparse.ArgumentParser(description="Verify seed counts")
    p.add_argument("--env", default="test", choices=["test", "prod"])
    verify(p.parse_args(argv).env)


if __name__ == "__main__":
    main()
