#!/usr/bin/env python3
"""Seed golden input data into SQL Server and stage flat-file inputs on disk.

Naming convention in the golden fixtures directory:
  * ``<module>_input_<table>.csv`` -> bulk-loaded into table ``<TABLE>``
  * ``<module>_input.csv`` / ``.dat`` -> copied into the landing directory
    (flat-file inputs consumed by Spark jobs)
  * ``<module>_expected_*.csv``      -> left on disk for reconciliation/regression
"""
from __future__ import annotations

import argparse
import shutil
from pathlib import Path
from typing import List

import pandas as pd

from utils.secrets import get_db_secret, load_config


def table_from_name(path: Path) -> str:
    stem = path.stem  # e.g. pay_calendar_input_pay_period
    marker = "_input_"
    idx = stem.find(marker)
    return stem[idx + len(marker):].upper()


def load_table(cur, table: str, df: pd.DataFrame) -> int:
    df = df.where(pd.notnull(df), None)
    cols = list(df.columns)
    placeholders = ", ".join("?" for _ in cols)
    collist = ", ".join(f"[{c}]" for c in cols)
    sql = f"INSERT INTO dbo.{table} ({collist}) VALUES ({placeholders})"
    cur.fast_executemany = True
    rows = [tuple(None if pd.isna(v) else v for v in r) for r in df.itertuples(index=False, name=None)]
    cur.execute(f"DELETE FROM dbo.{table}")
    if rows:
        cur.executemany(sql, rows)
    return len(rows)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--fixtures-dir", required=True)
    ap.add_argument("--env", default=None)
    args = ap.parse_args()

    import pyodbc

    config = load_config(args.env)
    secret = get_db_secret(config)
    db = config.database
    connstr = (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER={db.host},{db.port};DATABASE={db.name};"
        f"UID={secret.user};PWD={secret.password};"
        "Encrypt=yes;TrustServerCertificate=yes"
    )
    fixtures = Path(args.fixtures_dir)

    # Stage flat-file inputs into the landing dir.
    landing = Path(config.paths.landing)
    landing.mkdir(parents=True, exist_ok=True)
    flat_inputs: List[Path] = []
    for pattern in ("*_input.csv", "*_input.dat"):
        flat_inputs.extend(fixtures.glob(pattern))
    for f in flat_inputs:
        shutil.copy(f, landing / f.name)
        print(f"Staged flat-file input {f.name} -> {landing}")

    # Bulk-load table inputs.
    conn = pyodbc.connect(connstr, autocommit=True)
    cur = conn.cursor()
    table_inputs = sorted(p for p in fixtures.glob("*_input_*.csv"))
    for path in table_inputs:
        table = table_from_name(path)
        df = pd.read_csv(path, dtype=str, keep_default_na=False, na_values=[""])
        n = load_table(cur, table, df)
        print(f"Seeded {n:5d} rows -> dbo.{table} (from {path.name})")
    conn.close()
    print(f"Seed complete: {len(table_inputs)} tables, {len(flat_inputs)} flat files")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
