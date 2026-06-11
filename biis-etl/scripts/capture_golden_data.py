"""ONE-TIME: capture production golden datasets from the live source system.

Run this against the production Oracle/Informatica environment BEFORE migration
to baseline the "truth" datasets.  It dumps the source (input) and target
(output) tables of a real production run to ``tests/fixtures/golden/`` as CSV,
replacing the synthetic bootstrap fixtures.

Usage (two passes around a production Informatica run)::

    # before the workflow runs - capture source/input tables
    python scripts/capture_golden_data.py --phase before --env prod

    # after the workflow runs - capture target/output tables
    python scripts/capture_golden_data.py --phase after --env prod

This script is intentionally NOT exercised by CI (it needs production access).
Connection details come from the ``prod`` config / environment variables.
"""
from __future__ import annotations

import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from scripts._common import GOLDEN_DIR  # noqa: E402
from utils import db  # noqa: E402
from utils.config import get_config  # noqa: E402

# module -> (input tables, expected/output tables)
CAPTURE_PLAN = {
    "pay_calendar": (["PAY_PERIOD"], ["PAY_PERIOD"]),
    "comptime": ([], ["COMP_TIME_DAILY_TBL", "COUNTER_TBL"]),
    "pseudossn": ([], ["PSEUDOSSN_FROM_SDA_TBL", "PSEUDOSSN_TBL"]),
    "fda_leave": (["HI_PM_FDA_TATRAN_TBL", "CPM_YTD_DETAIL_STG_TBL",
                   "CPM_PAD_DETAIL_STG_TBL", "CPM_MER_DETAIL_STG_TBL"], ["ERROR_TBL"]),
    "ehrp2biis": (["NWK_NEW_EHRP_ACTIONS_TBL", "PS_GVT_JOB", "SEQUENCE_NUM_TBL"],
                  ["ACTION_PRIMARY_ALL", "ACTION_SECONDARY_ALL", "ACTION_REMARKS_ALL"]),
    "cpm_nih": (["CPM_NEWPAY_TBL"], ["CPM_NIH_STG_TBL"]),
    "cpm_oig": ([], ["CPM_OIG_STG_TBL"]),
    "cpm_cdc": ([], ["CPM_CDC_STG_TBL"]),
}


def _dump(cfg, table: str, path: str) -> int:
    import pandas as pd

    conn = db.get_connection(cfg)
    try:
        out = pd.read_sql_query(f"SELECT * FROM {table}", conn)
    finally:
        conn.close()
    out.to_csv(path, index=False)
    return len(out)


def capture(env: str, phase: str, out_dir: str) -> None:
    cfg = get_config(env)
    os.makedirs(out_dir, exist_ok=True)
    for module, (inputs, outputs) in CAPTURE_PLAN.items():
        tables = inputs if phase == "before" else outputs
        suffix = "input" if phase == "before" else "expected"
        for table in tables:
            path = os.path.join(out_dir, f"{module}_{suffix}_{table.lower()}.csv")
            n = _dump(cfg, table, path)
            print(f"  captured {module}/{table} ({phase}): {n} rows -> {os.path.basename(path)}")


def main(argv=None) -> None:
    p = argparse.ArgumentParser(description="Capture production golden data (ONE-TIME)")
    p.add_argument("--env", default="prod", choices=["test", "prod"])
    p.add_argument("--phase", required=True, choices=["before", "after"])
    p.add_argument("--out-dir", default=GOLDEN_DIR)
    args = p.parse_args(argv)
    capture(args.env, args.phase, args.out_dir)


if __name__ == "__main__":
    main()
