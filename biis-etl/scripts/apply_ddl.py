"""Create the database and apply all DDL + stored procedure stubs. Idempotent."""
import argparse
import glob
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pyodbc  # noqa: E402

from utils.secrets import get_secret, load_config  # noqa: E402

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def split_batches(sql: str):
    batch: list = []
    for line in sql.splitlines():
        if line.strip().upper() == "GO":
            if batch:
                yield "\n".join(batch)
            batch = []
        else:
            batch.append(line)
    if any(line.strip() for line in batch):
        yield "\n".join(batch)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--env", default=None)
    args = parser.parse_args()

    config = load_config(args.env)
    secret = get_secret("biis", config)
    db = config["database"]

    master = (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER={db['host']},{db['port']};DATABASE=master;"
        f"UID={secret['username']};PWD={secret['password']};TrustServerCertificate=yes;"
    )
    with pyodbc.connect(master, autocommit=True) as conn:
        cur = conn.cursor()
        cur.execute(
            f"IF DB_ID('{db['name']}') IS NULL CREATE DATABASE [{db['name']}]"
        )

    target = master.replace("DATABASE=master", f"DATABASE={db['name']}")
    files = [os.path.join(BASE, "sql", "ddl", "001_create_all_tables.sql")]
    files += sorted(glob.glob(os.path.join(BASE, "sql", "stored_procs", "*.sql")))
    with pyodbc.connect(target, autocommit=True) as conn:
        cur = conn.cursor()
        for path in files:
            with open(path) as f:
                sql = f.read()
            for batch in split_batches(sql):
                cur.execute(batch)
            print(f"Applied {os.path.basename(path)}")
    print("DDL applied successfully.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
