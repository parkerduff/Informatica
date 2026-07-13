#!/usr/bin/env python3
"""
Converted Glue job for PowerCenter folder ``Pay_Calendar``.

Classification : python_shell
Primary mapping: m_Pay_Calendar_Set_Pay_Calendar
Primary source : PAY_PERIOD
Primary target : PAY_PERIOD

Runs on AWS Glue (job argument style) and locally via the runtime shim.
Business logic lives in the spec-driven pipeline + infa_compat library; this
file is only the entrypoint (Glue expects one script per job).
"""
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..")))

from migration.jobs.runner import main

FOLDER = "Pay_Calendar"
ENGINE = "python_shell"

if __name__ == "__main__":
    raise SystemExit(main(sys.argv, folder=FOLDER))
