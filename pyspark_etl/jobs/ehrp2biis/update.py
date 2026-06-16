"""Replaces the ``EHRP2BIIS_UPDATE`` mapping (XML/EHRP2BIIS_UPDATE).

Data flow:
    1. Read NWK_NEW_EHRP_ACTIONS_TBL (NKNIGHT) -- the 4 driver keys
       (EMPLID, EMPL_RCD, EFFDT, EFFSEQ) identifying actions to (re)load.
    2. Read PS_GVT_JOB (EHRP) -- 246 columns of job/comp/position data.
    3. Inner-join on the 4 keys to keep only the requested actions.
    4. Project to the NWK_ACTION_PRIMARY_TBL / NWK_ACTION_SECONDARY_TBL targets
       and stamp LOAD_DATE = today (the secondary refinement of step codes,
       remarks and 900-series handling happens in afterload.sql).

The primary/secondary target column lists come straight from the XML target
definitions (config.schemas.oracle_tables). Only columns present in PS_GVT_JOB
are populated from the join; the remaining target columns are produced by the
downstream PL/SQL procedures and are left null here.
"""
from __future__ import annotations

import logging
from typing import Optional

from pyspark_etl.config import connections
from pyspark_etl.config.schemas.ehrp2biis_schemas import NWK_KEY_COLUMNS
from pyspark_etl.config.schemas.oracle_tables import (
    NWK_ACTION_PRIMARY_TBL,
    NWK_ACTION_SECONDARY_TBL,
)
from pyspark_etl.utils.oracle_jdbc import read_table, write_table

logger = logging.getLogger(__name__)


def _project_to_target(df, target_columns):
    """Select target columns, filling absent ones with NULL (populated later)."""
    from pyspark.sql import functions as F

    available = set(df.columns)
    exprs = []
    for col in target_columns:
        if col in available:
            exprs.append(F.col(col).alias(col))
        else:
            exprs.append(F.lit(None).cast("string").alias(col))
    return df.select(*exprs)


def run(spark, write: bool = True, target_schema: Optional[str] = None):
    """Join NWK driver keys to PS_GVT_JOB and load the NWK action tables."""
    from pyspark.sql import functions as F

    schema = target_schema or connections.SCHEMA_NKNIGHT
    logger.info("EHRP2BIIS_UPDATE starting")

    nwk = read_table(spark, "ORA_BIISPRD_SRC", connections.SCHEMA_NKNIGHT,
                     "NWK_NEW_EHRP_ACTIONS_TBL", columns=NWK_KEY_COLUMNS)
    gvt = read_table(spark, "ORA_BIISPRD_SRC", connections.SCHEMA_EHRP, "PS_GVT_JOB")

    joined = nwk.join(gvt, on=NWK_KEY_COLUMNS, how="inner")
    joined = joined.withColumn("LOAD_DATE", F.current_date())

    primary_cols = [c.name for c in NWK_ACTION_PRIMARY_TBL]
    secondary_cols = [c.name for c in NWK_ACTION_SECONDARY_TBL]
    primary = _project_to_target(joined, primary_cols)
    secondary = _project_to_target(joined, secondary_cols)

    if write:
        write_table(primary, "ORA_BIIS", schema, "NWK_ACTION_PRIMARY_TBL")
        write_table(secondary, "ORA_BIIS", schema, "NWK_ACTION_SECONDARY_TBL")
    logger.info("EHRP2BIIS_UPDATE complete")
    return primary, secondary
