"""Shared logic for the CPM agency extracts (NIH / OIG / CDC).

All three agencies read the 501-field CPM_NEWPAY_TBL, project the common payroll
fields, build a fixed-width mainframe (PWX) output record and write both an
agency staging table and a flat file. Agency-specific differences are limited to
the agency code and the output filename; the record layout is shared so the
output is reconcilable.
"""
from __future__ import annotations

import datetime as dt
import logging
import os
from typing import Any, Dict

from pyspark.sql import functions as F
from pyspark.sql import types as T

from jobs.common import (CPM_AGENCY_W, CPM_AMT_SCALE, CPM_AMT_W, CPM_SSN_W,
                         build_record_text, scaled_cents)
from utils.db import get_current_pay_period, write_table
from utils.secrets import Config

logger = logging.getLogger("cpm.common")

# Fixed-width output field widths (re-exported from jobs.common).
SSN_W = CPM_SSN_W
AGENCY_W = CPM_AGENCY_W
AMT_W = CPM_AMT_W
AMT_SCALE = CPM_AMT_SCALE

__all__ = ["build_record_text", "scaled_cents", "transform", "run_agency",
           "write_flat_file"]

# Source columns used by every agency extract.
SRC_SSN = "SOC_SEC_NO"
SRC_GROSS = "ADJ_GROSS_PAY"
SRC_NET = "ADJ_NET_PAY"

STAGING_COLS = ["PP_END_YEAR", "PP_NUM", "SSN", "AGENCY_CD", "GROSS_PAY",
                "NET_PAY", "RECORD_TEXT"]

AGENCIES = {
    "nih": {"code": "NIH", "filename": "nihtest_NIH_PAYROLL_MASTER.dat"},
    "oig": {"code": "OIG", "filename": "oigsgndec_SKPAYROLL_MASTER.dat"},
    "cdc": {"code": "CDC", "filename": "cdcskel_WS_PAY_OUT_REC.dat"},
}


def transform(df: Any, agency_code: str) -> Any:
    """Project CPM_NEWPAY_TBL to the agency staging layout with a PWX record."""
    cents = F.pow(F.lit(10), F.lit(AMT_SCALE))
    gross_cents = (F.col(SRC_GROSS).cast(T.DecimalType(20, 2)) * cents).cast(T.LongType())
    net_cents = (F.col(SRC_NET).cast(T.DecimalType(20, 2)) * cents).cast(T.LongType())
    record_text = F.concat(
        F.lpad(F.col(SRC_SSN).cast(T.StringType()), SSN_W, "0"),
        F.rpad(F.lit(agency_code), AGENCY_W, " "),
        F.lpad(gross_cents.cast(T.StringType()), AMT_W, "0"),
        F.lpad(net_cents.cast(T.StringType()), AMT_W, "0"),
    )
    return df.select(
        F.col("PP_END_YEAR"),
        F.col("PP_NUM"),
        F.col(SRC_SSN).alias("SSN"),
        F.lit(agency_code).alias("AGENCY_CD"),
        F.col(SRC_GROSS).alias("GROSS_PAY"),
        F.col(SRC_NET).alias("NET_PAY"),
        record_text.alias("RECORD_TEXT"),
    )


def write_flat_file(records, agency: str, pay_period: Dict[str, Any], out_dir: str) -> str:
    """Write the header/detail/trailer flat file for PWX/SFTP pickup."""
    os.makedirs(out_dir, exist_ok=True)
    meta = AGENCIES[agency]
    path = os.path.join(out_dir, meta["filename"])
    today = dt.date.today().strftime("%Y%m%d")
    header = "H" + meta["code"].ljust(AGENCY_W) + today
    trailer = "T" + f"{len(records):09d}"
    with open(path, "w", encoding="ascii") as fh:
        fh.write(header + "\n")
        for r in records:
            fh.write(r["RECORD_TEXT"] + "\n")
        fh.write(trailer + "\n")
    logger.info("wrote %d records to %s", len(records), path)
    return path


def run_agency(agency: str, config: Config) -> int:  # pragma: no cover
    """Run a single agency extract end-to-end."""
    from utils.db import get_db_secret, read_table
    from utils.notifications import send_notification
    from utils.spark import get_spark

    meta = AGENCIES[agency]
    spark = get_spark(f"cpm-{agency}")
    secret = get_db_secret(config)
    pay_period = get_current_pay_period(spark, config, secret)

    src = read_table(spark, "CPM_NEWPAY_TBL", config, secret)
    staged = transform(src, meta["code"])

    staging_table = f"CPM_{meta['code']}_STAGING_TBL"
    write_table(staged.select(*STAGING_COLS), staging_table, config, secret, mode="append")

    records = [r.asDict() for r in staged.select("RECORD_TEXT").collect()]
    write_flat_file(records, agency, pay_period, config.paths.staging)

    count = len(records)
    send_notification(
        f"CPM_{meta['code']}: extract complete",
        f"Wrote {count} {meta['code']} payroll records for "
        f"PP {pay_period['pp_num']:02d}/{pay_period['pp_end_year']}.",
        config,
    )
    logger.info("cpm %s complete: %d records", agency, count)
    return count
