"""Load golden CSV fixtures into SQL Server and stage flat-file inputs."""
import argparse
import os
import shutil
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd  # noqa: E402

from utils.db import pyodbc_connection  # noqa: E402
from utils.secrets import get_secret, load_config  # noqa: E402

# Input CSVs map onto the table named by the trailing part of the file name.
INPUT_TABLE_MAP = {
    "pay_calendar_input_pay_period.csv": "PAY_PERIOD",
    "fda_leave_input_hi_pm_fda_tatran_tbl.csv": "HI_PM_FDA_TATRAN_TBL",
    "fda_leave_input_cpm_ytd_detail_stg_tbl.csv": "CPM_YTD_DETAIL_STG_TBL",
    "fda_leave_input_cpm_pad_detail_stg_tbl.csv": "CPM_PAD_DETAIL_STG_TBL",
    "fda_leave_input_cpm_mer_detail_stg_tbl.csv": "CPM_MER_DETAIL_STG_TBL",
    "ehrp2biis_input_nwk_new_ehrp_actions_tbl.csv": "NWK_NEW_EHRP_ACTIONS_TBL",
    "ehrp2biis_input_ps_gvt_job.csv": "PS_GVT_JOB",
    "ehrp2biis_input_sequence_num_tbl.csv": "SEQUENCE_NUM_TBL",
    "cpm_input_cpm_newpay_tbl.csv": "CPM_NEWPAY_TBL",
}

FLAT_FILE_INPUTS = ["comptime_input.csv", "pseudossn_input.dat"]


def load_csv(conn, table: str, path: str) -> int:
    df = pd.read_csv(path, dtype=str, keep_default_na=False)
    df = df.replace({"": None})
    cur = conn.cursor()
    cur.execute(f"DELETE FROM [dbo].[{table}]")
    cols = ", ".join(f"[{c}]" for c in df.columns)
    marks = ", ".join("?" for _ in df.columns)
    cur.fast_executemany = True
    rows = [tuple(r) for r in df.itertuples(index=False, name=None)]
    if rows:
        cur.executemany(f"INSERT INTO [dbo].[{table}] ({cols}) VALUES ({marks})", rows)
    return len(rows)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--fixtures-dir", default="tests/fixtures/golden/")
    parser.add_argument("--env", default=None)
    args = parser.parse_args()

    config = load_config(args.env)
    secret = get_secret("biis", config)
    landing = config["paths"]["landing"]
    os.makedirs(landing, exist_ok=True)
    os.makedirs(config["paths"]["staging"], exist_ok=True)
    os.makedirs(config["paths"]["archive"], exist_ok=True)

    with pyodbc_connection(secret, config) as conn:
        for fname, table in INPUT_TABLE_MAP.items():
            path = os.path.join(args.fixtures_dir, fname)
            if not os.path.exists(path):
                print(f"SKIP missing fixture {fname}")
                continue
            n = load_csv(conn, table, path)
            conn.commit()
            print(f"Seeded {table} with {n} rows from {fname}")

    for fname in FLAT_FILE_INPUTS:
        src = os.path.join(args.fixtures_dir, fname)
        if os.path.exists(src):
            shutil.copy(src, os.path.join(landing, fname))
            print(f"Staged flat file {fname} -> {landing}")
    print("Seeding complete.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
