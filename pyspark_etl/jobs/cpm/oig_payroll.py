"""Replaces the ``CPM_OIG`` mapping -- OIG payroll extract (BUSINESS_UNIT OIG00)."""
from __future__ import annotations

from pyspark_etl.jobs.cpm import OIG, extract_agency


def run(spark, pp_num: int, pp_end_year: int, output_dir=None, write: bool = True):
    return extract_agency(spark, OIG, pp_num, pp_end_year, output_dir, write)
