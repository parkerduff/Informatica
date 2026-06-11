"""Shared helpers for the BIIS ETL helper scripts."""
from __future__ import annotations

import os
import sys

# Make the package importable when scripts are run directly.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd  # noqa: E402

from utils import db  # noqa: E402
from utils.schemas import column_names  # noqa: E402

GOLDEN_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                          "tests", "fixtures", "golden")


def load_csv_to_table(cfg, csv_path: str, table: str) -> int:
    """Insert all rows of ``csv_path`` into ``table``; returns row count."""
    df = pd.read_csv(csv_path, dtype=str, keep_default_na=False, na_values=[""])
    cols = [c for c in column_names(table) if c in df.columns]
    df = df[cols]
    placeholders = ", ".join(["?"] * len(cols))
    sql = f"INSERT INTO {table} ({', '.join(cols)}) VALUES ({placeholders})"
    rows = [tuple(None if pd.isna(v) else v for v in rec) for rec in df.itertuples(index=False, name=None)]
    db.executemany(cfg, sql, rows)
    return len(rows)


def dump_table_to_csv(cfg, table: str, csv_path: str) -> int:
    with db.connection(cfg) as conn:
        out = pd.read_sql_query(f"SELECT * FROM {table}", conn)
    os.makedirs(os.path.dirname(csv_path), exist_ok=True)
    out.to_csv(csv_path, index=False)
    return len(out)
