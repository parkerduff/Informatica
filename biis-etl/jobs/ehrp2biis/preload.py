"""EHRP2BIIS preload — migration of ehrp2biis_preload (ksh + sqlplus step01).

PRELOAD STUB — the step01 SQL script lives only on the legacy server
(/data/BIISINT/bin/EHRP2BIIS/step01) and is not in this repository. Extract it
and replace the body of run() with the equivalent T-SQL.
"""
import argparse
import logging
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from utils.db import pyodbc_connection  # noqa: E402
from utils.notifications import send_notification  # noqa: E402
from utils.secrets import get_secret, load_config  # noqa: E402

logger = logging.getLogger("ehrp2biis.preload")


def run(env: str = None) -> None:
    config = load_config(env)
    secret = get_secret("biis", config)
    logger.warning("PRELOAD STUB - replace with extracted step01 SQL")
    with pyodbc_connection(secret, config) as conn:
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM NWK_NEW_EHRP_ACTIONS_TBL")
        count = cur.fetchone()[0]
        logger.info("NWK_NEW_EHRP_ACTIONS_TBL staged rows: %d", count)
    send_notification(
        "EHRP2BIIS Preload completed (stub)",
        f"Preload stub verified connectivity; {count} staged actions present.",
        config,
    )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--env", default=None)
    args = parser.parse_args()
    run(args.env)
    return 0


if __name__ == "__main__":
    sys.exit(main())
