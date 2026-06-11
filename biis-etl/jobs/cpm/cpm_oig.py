#!/usr/bin/env python3
"""CPM OIG payroll extract -- migration of ``XML/CPM_OIG``.

Produces the ``oigsgndec_SKPAYROLL_MASTER`` fixed-width output (signed-decimal
fields) and the CPM_OIG_STAGING_TBL validation table. Shared logic lives in
cpm_common.
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
    run_agency("oig", load_config(args.env))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
