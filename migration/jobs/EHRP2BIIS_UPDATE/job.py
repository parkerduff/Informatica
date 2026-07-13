#!/usr/bin/env python3
"""
Converted Glue job for PowerCenter folder ``EHRP2BIIS_UPDATE``.

Classification : pyspark
Primary mapping: m_EHRP2BIIS_UPDATE
Primary source : NWK_NEW_EHRP_ACTIONS_TBL
Primary target : NWK_ACTION_PRIMARY_TBL

Runs on AWS Glue (job argument style) and locally via the runtime shim.
Business logic lives in the spec-driven pipeline + infa_compat library; this
file is only the entrypoint (Glue expects one script per job).
"""
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..")))

from migration.jobs.runner import main

FOLDER = "EHRP2BIIS_UPDATE"
ENGINE = "pyspark"

if __name__ == "__main__":
    raise SystemExit(main(sys.argv, folder=FOLDER))
