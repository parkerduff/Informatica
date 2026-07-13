#!/usr/bin/env python3
"""
Converted Glue job for PowerCenter folder ``FDA_Leave``.

Classification : pyspark
Primary mapping: m_0100_PM_FDA_Load_TATRAN_To_DB
Primary source : HI_PM_FDA_TATRAN_FLAT
Primary target : HI_PM_FDA_TATRAN_TBL

Runs on AWS Glue (job argument style) and locally via the runtime shim.
Business logic lives in the spec-driven pipeline + infa_compat library; this
file is only the entrypoint (Glue expects one script per job).
"""
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..")))

from migration.jobs.runner import main

FOLDER = "FDA_Leave"
ENGINE = "pyspark"

if __name__ == "__main__":
    raise SystemExit(main(sys.argv, folder=FOLDER))
