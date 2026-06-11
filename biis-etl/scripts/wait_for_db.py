#!/usr/bin/env python3
"""Poll SQL Server until it answers SELECT 1, or time out."""
from __future__ import annotations

import argparse
import sys
import time

from utils.secrets import get_db_secret, load_config


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--env", default=None)
    ap.add_argument("--timeout", type=int, default=60)
    ap.add_argument("--interval", type=int, default=2)
    args = ap.parse_args()

    import pyodbc

    config = load_config(args.env)
    secret = get_db_secret(config)
    db = config.database
    # Connect to master first; the target DB may not exist yet.
    connstr = (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER={db.host},{db.port};DATABASE=master;"
        f"UID={secret.user};PWD={secret.password};"
        "Encrypt=yes;TrustServerCertificate=yes"
    )

    deadline = time.time() + args.timeout
    last_err = None
    while time.time() < deadline:
        try:
            conn = pyodbc.connect(connstr, timeout=5)
            cur = conn.cursor()
            cur.execute("SELECT 1")
            cur.fetchone()
            conn.close()
            print(f"SQL Server ready at {db.host}:{db.port}")
            return 0
        except Exception as e:  # noqa: BLE001 - retry loop
            last_err = e
            print(f"waiting for SQL Server... ({e.__class__.__name__})")
            time.sleep(args.interval)
    print(f"Timed out after {args.timeout}s waiting for SQL Server: {last_err}", file=sys.stderr)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
