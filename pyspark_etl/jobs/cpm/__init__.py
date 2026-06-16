"""CPM agency payroll extracts (NIH / CDC / OIG / AFPS) and FDA leave.

Each agency mapping (``XML/CPM_NIH``, ``CPM_CDC``, ``CPM_OIG``, ``CPM_AFPS``)
reads the shared ``CPM_NEWPAY_TBL`` (501 columns), filters to the current pay
period plus an agency-specific predicate, and writes a fixed-width extract file
to the CPM output directory. The agency predicates are taken verbatim from each
mapping's Source Qualifier "Source Filter".
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Optional

from pyspark_etl.config import connections
from pyspark_etl.config.schemas.cpm_schemas import CPM_NEWPAY_TBL_COLUMNS
from pyspark_etl.utils.oracle_jdbc import read_table

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class AgencyConfig:
    """Describes one CPM agency extract."""

    name: str
    # A Spark SQL boolean expression over CPM_NEWPAY_TBL columns, equivalent to
    # the Informatica Source Qualifier "Source Filter" (pay-period predicate is
    # added separately).
    agency_filter: str
    output_basename: str


# Source Filters lifted directly from the XML Source Qualifiers.
NIH = AgencyConfig(
    name="NIH",
    agency_filter="MP_POOL_DES = ' ' AND BUSINESS_UNIT = 'NIH00'",
    output_basename="CPM_NIH",
)
CDC = AgencyConfig(
    name="CDC",
    agency_filter=(
        "MP_POOL_DES = ' ' AND ("
        "BUSINESS_UNIT IN ('CDC00', 'ATSDR') OR "
        "trim(ORG_CDE) IN ('ANC34','ANC341','ANC342','ANC343','ANC344','ANC345'))"
    ),
    output_basename="CPM_CDC",
)
OIG = AgencyConfig(
    name="OIG",
    agency_filter="MP_POOL_DES = ' ' AND BUSINESS_UNIT = 'OIG00'",
    output_basename="CPM_OIG",
)
AFPS = AgencyConfig(
    name="AFPS",
    agency_filter="MP_POOL_DES <> ' '",
    output_basename="CPM_AFPS",
)


def extract_agency(spark, cfg: AgencyConfig, pp_num: int, pp_end_year: int,
                   output_dir: Optional[str] = None, write: bool = True):
    """Read CPM_NEWPAY_TBL, apply pay-period + agency filter, write fixed-width.

    Returns the filtered DataFrame.
    """
    from pyspark.sql import functions as F

    out_dir = output_dir or connections.PATHS.cpm_output_dir
    logger.info("CPM %s extract for PP %s/%s", cfg.name, pp_num, pp_end_year)

    df = read_table(spark, "ORA_BIIS", connections.SCHEMA_INFO_TARGET, "CPM_NEWPAY_TBL")
    pp_pred = (F.col("PP_END_YEAR") == pp_end_year) & (F.col("PP_NUM") == pp_num)
    df = df.filter(pp_pred).filter(F.expr(cfg.agency_filter))

    if write:
        from pyspark_etl.transforms.fixed_width_writer import write_fixed_width

        # Without the per-field output layout we emit the projected source
        # columns; supply a layout to produce the exact agency record format.
        select_cols = [c for c in CPM_NEWPAY_TBL_COLUMNS if c in df.columns]
        out_path = f"{out_dir.rstrip('/')}/{cfg.output_basename}.txt"
        write_fixed_width(df.select(*select_cols), out_path)
        logger.info("Wrote CPM %s extract to %s", cfg.name, out_path)
    return df
