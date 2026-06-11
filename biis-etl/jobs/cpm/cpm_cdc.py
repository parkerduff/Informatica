#!/usr/bin/env python3
"""CPM CDC payroll extract -- migration of ``XML/CPM_CDC``.

Produces the ``cdcskel_WS_PAY_OUT_REC`` fixed-width output (with the
``cdchdr_WS_CDC_HDR`` header) and the CPM_CDC_STAGING_TBL validation table.
Shared logic lives in cpm_common.
"""
from __future__ import annotations

import argparse

from jobs.cpm.cpm_common import run_agency
from utils.secrets import load_config


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--env", default=None)
    ap.add_argument("--run-date", default=None)
    args = ap.parse_args()
    run_agency("cdc", load_config(args.env))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
