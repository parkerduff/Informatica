"""Replaces the ``CPM_AFPS`` mapping -- AFPS extract (MP_POOL_DES populated)."""
from __future__ import annotations

from pyspark_etl.jobs.cpm import AFPS, extract_agency


def run(spark, pp_num: int, pp_end_year: int, output_dir=None, write: bool = True):
    return extract_agency(spark, AFPS, pp_num, pp_end_year, output_dir, write)
