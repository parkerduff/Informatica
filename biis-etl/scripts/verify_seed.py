"""Print row counts for every seeded table; fail if a required table is empty."""
import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.db import pyodbc_connection  # noqa: E402
from utils.secrets import get_secret, load_config  # noqa: E402

REQUIRED_NONEMPTY = [
    "PAY_PERIOD",
    "HI_PM_FDA_TATRAN_TBL",
    "CPM_YTD_DETAIL_STG_TBL",
    "CPM_PAD_DETAIL_STG_TBL",
    "CPM_MER_DETAIL_STG_TBL",
    "NWK_NEW_EHRP_ACTIONS_TBL",
    "PS_GVT_JOB",
    "SEQUENCE_NUM_TBL",
    "CPM_NEWPAY_TBL",
]


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--env", default=None)
    args = parser.parse_args()

    config = load_config(args.env)
    secret = get_secret("biis", config)
    failures = []
    with pyodbc_connection(secret, config) as conn:
        cur = conn.cursor()
        cur.execute(
            "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES "
            "WHERE TABLE_TYPE='BASE TABLE' ORDER BY TABLE_NAME"
        )
        tables = [r[0] for r in cur.fetchall()]
        for table in tables:
            cur.execute(f"SELECT COUNT(*) FROM [dbo].[{table}]")
            count = cur.fetchone()[0]
            print(f"{table}: {count}")
            if table in REQUIRED_NONEMPTY and count == 0:
                failures.append(table)
    if failures:
        print(f"FAIL: required tables empty: {failures}", file=sys.stderr)
        return 1
    print("Seed verification passed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
