"""Load golden *input* fixtures into the database.

Loads every ``<module>_input_<table>.csv`` file in the fixtures directory into
its matching table.  ``<module>_expected_*.csv`` files are NOT loaded - they are
used by the regression tests for comparison.  File-based inputs
(``comptime_input.csv``, ``pseudossn_input.dat``) are read directly by their
jobs and are intentionally skipped here.
"""
from __future__ import annotations

import argparse
import glob
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from scripts._common import GOLDEN_DIR, load_csv_to_table  # noqa: E402
from utils import db  # noqa: E402
from utils.config import get_config  # noqa: E402
from utils.schemas import TABLES  # noqa: E402


def table_for(path: str):
    base = os.path.basename(path)
    if "_input_" not in base:
        return None
    table = base.split("_input_", 1)[1][: -len(".csv")].upper()
    return table if table in TABLES else None


def seed(env: str, fixtures_dir: str) -> None:
    cfg = get_config(env)
    loaded = 0
    for path in sorted(glob.glob(os.path.join(fixtures_dir, "*_input_*.csv"))):
        table = table_for(path)
        if not table:
            continue
        db.truncate(cfg, table)
        n = load_csv_to_table(cfg, path, table)
        print(f"  seeded {table:<28} {n:>5} rows  <- {os.path.basename(path)}")
        loaded += 1
    print(f"Seeded {loaded} input table(s)")


def main(argv=None) -> None:
    p = argparse.ArgumentParser(description="Seed golden input fixtures")
    p.add_argument("--env", default="test", choices=["test", "prod"])
    p.add_argument("--fixtures-dir", default=GOLDEN_DIR)
    args = p.parse_args(argv)
    seed(args.env, args.fixtures_dir)


if __name__ == "__main__":
    main()
