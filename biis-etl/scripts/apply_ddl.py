#!/usr/bin/env python3
"""Create the target database and apply the schema DDL + stored-proc stubs.

Idempotent: tables are dropped/recreated by the DDL itself; procedures use
DROP/CREATE. Statements are split on ``GO`` batch separators.
"""
from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path
from typing import List

from utils.secrets import get_db_secret, load_config

ROOT = Path(__file__).resolve().parents[1]
DDL_FILE = ROOT / "sql" / "ddl" / "001_create_all_tables.sql"
PROC_DIR = ROOT / "sql" / "stored_procs"


def split_batches(sql: str) -> List[str]:
    """Split a T-SQL script on lines containing only GO."""
    parts = re.split(r"(?im)^\s*GO\s*$", sql)
    return [p.strip() for p in parts if p.strip()]


def run_script(cur, sql: str) -> None:
    for batch in split_batches(sql):
        cur.execute(batch)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--env", default=None)
    args = ap.parse_args()

    import pyodbc

    config = load_config(args.env)
    secret = get_db_secret(config)
    db = config.database

    base = (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER={db.host},{db.port};DATABASE=master;"
        f"UID={secret.user};PWD={secret.password};"
        "Encrypt=yes;TrustServerCertificate=yes"
    )
    # Database creation must run with autocommit on.
    conn = pyodbc.connect(base, autocommit=True)
    cur = conn.cursor()
    cur.execute(
        f"IF DB_ID('{db.name}') IS NULL CREATE DATABASE [{db.name}];"
    )
    conn.close()
    print(f"Ensured database {db.name} exists")

    target = base.replace("DATABASE=master", f"DATABASE={db.name}")
    conn = pyodbc.connect(target, autocommit=True)
    cur = conn.cursor()

    print(f"Applying DDL {DDL_FILE.name}...")
    run_script(cur, DDL_FILE.read_text())

    for proc in sorted(PROC_DIR.glob("*.sql")):
        print(f"Applying stored proc {proc.name}...")
        run_script(cur, proc.read_text())

    conn.close()
    print("DDL + stored procs applied")
    return 0


if __name__ == "__main__":
    sys.exit(main())
