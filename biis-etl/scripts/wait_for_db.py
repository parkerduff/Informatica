"""Poll SQL Server until it answers SELECT 1 or the timeout elapses."""
import argparse
import sys
import time

import pyodbc


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="localhost")
    parser.add_argument("--port", type=int, default=1433)
    parser.add_argument("--user", default="sa")
    parser.add_argument("--password", default="TestP@ssw0rd!")
    parser.add_argument("--timeout", type=int, default=60)
    args = parser.parse_args()

    connstr = (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER={args.host},{args.port};DATABASE=master;"
        f"UID={args.user};PWD={args.password};TrustServerCertificate=yes;"
    )
    deadline = time.time() + args.timeout
    last_err = None
    while time.time() < deadline:
        try:
            with pyodbc.connect(connstr, timeout=5) as conn:
                conn.cursor().execute("SELECT 1").fetchone()
            print("SQL Server is ready.")
            return 0
        except Exception as exc:  # noqa: BLE001 - retry until deadline
            last_err = exc
            time.sleep(2)
    print(f"SQL Server not ready after {args.timeout}s: {last_err}", file=sys.stderr)
    return 1


if __name__ == "__main__":
    sys.exit(main())
