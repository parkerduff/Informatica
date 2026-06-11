#!/usr/bin/env python3
"""Print row counts for every table; fail if a seeded input table is empty."""
from __future__ import annotations

import argparse
import sys

from utils.secrets import get_db_secret, load_config

# Input tables that must be non-empty after seeding.
REQUIRED_NONEMPTY = [
    "PAY_PERIOD",
    "HI_PM_FDA_TATRAN_TBL",
    "CPM_YTD_DETAIL_STG_TBL",
    "CPM_PAD_DETAIL_STG_TBL",
    "CPM_MER_DETAIL_STG_TBL",
    "NWK_NEW_EHRP_ACTIONS_TBL",
    "PS_GVT_JOB",
    "CPM_NEWPAY_TBL",
    "SEQUENCE_NUM_TBL",
]


def main() -> int:
    ap = argparse.ArgumentParser()
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
    conn = pyodbc.connect(connstr)
    cur = conn.cursor()
    cur.execute(
        "SELECT t.name FROM sys.tables t WHERE t.schema_id = SCHEMA_ID('dbo') ORDER BY t.name"
    )
    tables = [r[0] for r in cur.fetchall()]

    failures = []
    for t in tables:
        cur.execute(f"SELECT COUNT(*) FROM dbo.[{t}]")
        n = cur.fetchone()[0]
        flag = ""
        if t.upper() in REQUIRED_NONEMPTY and n == 0:
            flag = "  <-- REQUIRED BUT EMPTY"
            failures.append(t)
        print(f"  {t:35s} {n:8d}{flag}")
    conn.close()

    if failures:
        print(f"\nFAIL: required tables empty after seed: {failures}", file=sys.stderr)
        return 1
    print("\nverify_seed OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
