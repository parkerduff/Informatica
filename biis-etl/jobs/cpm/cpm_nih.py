#!/usr/bin/env python3
"""CPM NIH payroll extract -- migration of ``XML/CPM_NIH``.

Produces the ``nihtest_NIH_PAYROLL_MASTER`` fixed-width output and the
CPM_NIH_STAGING_TBL validation table. Shared logic lives in cpm_common.
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
    run_agency("nih", load_config(args.env))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
