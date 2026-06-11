"""Wait for the configured database to accept connections.

For the default ``test`` (sqlite) backend this is effectively instant; for the
``prod`` (SQL Server) backend it polls until the server is ready or the timeout
elapses.  Used by the Makefile ``db-up`` target.
"""
from __future__ import annotations

import argparse
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils import db  # noqa: E402
from utils.config import get_config  # noqa: E402


def wait(env: str, timeout: int) -> None:
    cfg = get_config(env)
    start = time.time()
    last_err = None
    while time.time() - start < timeout:
        try:
            conn = db.get_connection(cfg)
            conn.close()
            print(f"{cfg.backend} is ready")
            return
        except Exception as exc:  # noqa: BLE001
            last_err = exc
            time.sleep(2)
    print(f"TIMEOUT waiting for {cfg.backend}: {last_err}", file=sys.stderr)
    sys.exit(1)


def main(argv=None) -> None:
    p = argparse.ArgumentParser(description="Wait for DB readiness")
    p.add_argument("--env", default="test", choices=["test", "prod"])
    p.add_argument("--timeout", type=int, default=60)
    args = p.parse_args(argv)
    wait(args.env, args.timeout)


if __name__ == "__main__":
    main()
