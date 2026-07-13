#!/usr/bin/env python3
"""
Converted Glue job for PowerCenter folder ``CPM_OIG``.

Classification : pyspark
Primary mapping: m_CPM_OIG_Load_CPM_OIG_File
Primary source : CPM_NEWPAY_TBL
Primary target : oigsgndec_SKPAYROLL_MASTER

Runs on AWS Glue (job argument style) and locally via the runtime shim.
Business logic lives in the spec-driven pipeline + infa_compat library; this
file is only the entrypoint (Glue expects one script per job).
"""
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..")))

from migration.jobs.runner import main

FOLDER = "CPM_OIG"
ENGINE = "pyspark"

if __name__ == "__main__":
    raise SystemExit(main(sys.argv, folder=FOLDER))
