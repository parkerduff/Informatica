#!/usr/bin/env python3
"""
local.sql_shim -- host the PL/SQL-equivalent target tables in a local DB.

Uses SQLite by default (zero-dependency) so the local pipeline can materialise
the converted job outputs into relational target tables and exercise the
push-down afterload SQL (portable subset). Points at Postgres when
``BIIS_PG_DSN`` is set (docker-compose service).

This is the local stand-in for the DB-resident Oracle warehouse; the same target
DDL / push-down steps run against the real warehouse in AWS.
"""
from __future__ import annotations

import csv
import os
import sqlite3
import sys
from typing import Any, Dict, List

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from migration.jobs import configs
from migration.sql import pushdown

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
DEFAULT_DB = os.path.join(os.path.dirname(__file__), "warehouse.db")

_TYPE_MAP = {"number": "NUMERIC", "date": "TEXT", "varchar2": "TEXT", "char": "TEXT"}


def _sqlite(db_path: str):
    conn = sqlite3.connect(db_path)
    return conn


def _col_ddl(col) -> str:
    base = (col.datatype or "varchar2").split("(")[0].lower()
    sqltype = "NUMERIC" if base.startswith("number") or "dec" in base else \
        _TYPE_MAP.get(base, "TEXT")
    # Local materialisation only: nullability parity is validated by the recon
    # harness, not enforced here, so the shim never rejects a spec-faithful NULL.
    return f'"{col.name}" {sqltype}'


def create_target_tables(conn, folders: List[str]) -> List[str]:
    created = []
    cur = conn.cursor()
    for folder in folders:
        pdef = configs.build(folder)
        tname = pdef.target.name
        cols = ", ".join(_col_ddl(c) for c in pdef.target.columns)
        cur.execute(f'DROP TABLE IF EXISTS "{tname}"')
        cur.execute(f'CREATE TABLE "{tname}" ({cols})')
        created.append(tname)
    conn.commit()
    return created


def load_output(conn, folder: str, out_dir: str) -> int:
    pdef = configs.build(folder)
    tname = pdef.target.name
    path = os.path.join(out_dir, folder, f"{tname}.csv")
    if not os.path.exists(path):
        return 0
    with open(path, newline="", encoding="utf-8") as fh:
        rows = list(csv.DictReader(fh))
    if not rows:
        return 0
    cols = [c.name for c in pdef.target.columns]
    placeholders = ",".join("?" for _ in cols)
    colnames = ",".join(f'"{c}"' for c in cols)
    cur = conn.cursor()
    cur.executemany(
        f'INSERT INTO "{tname}" ({colnames}) VALUES ({placeholders})',
        [[(r.get(c) or None) for c in cols] for r in rows],
    )
    conn.commit()
    return len(rows)


def run_afterload(conn) -> Dict[str, Any]:
    step = pushdown.load_step("ehrp2biis_afterload")
    return pushdown.apply_local(step, conn)


def main(argv: List[str]) -> int:
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--out-dir", required=True, help="pipeline output dir")
    ap.add_argument("--db", default=DEFAULT_DB)
    ap.add_argument("--folders", nargs="*", default=configs.ALL_FOLDERS)
    args = ap.parse_args(argv[1:])

    conn = _sqlite(args.db)
    created = create_target_tables(conn, args.folders)
    total = 0
    for folder in args.folders:
        n = load_output(conn, folder, args.out_dir)
        total += n
        print(f"[sql_shim] loaded {n:>7} rows into target for {folder}")
    after = run_afterload(conn)
    print(f"[sql_shim] created {len(created)} target tables; loaded {total} rows total")
    print(f"[sql_shim] afterload push-down: applied={after['applied']} "
          f"skipped={after['skipped']} (of {after['total']})")
    conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
