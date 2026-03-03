"""
PySpark job: EHRP2BIIS Update

Replaces Informatica PowerCenter mapping ``m_EHRP2BIIS_UPDATE``
(source file: XML/EHRP2BIIS_UPDATE).

Processing flow:
  1. Read source tables NWK_NEW_EHRP_ACTIONS_TBL and PS_GVT_JOB from
     ORA_BIISPRD_SRC and join on (EMPLID, EMPL_RCD, EFFDT, EFFSEQ).
  2. Perform broadcast lookup joins against nine reference tables.
  3. Apply expression transformations (exp_MAIN2BIIS, exp_GET_EFFDT_YEAR,
     exp_PERS_DATA).
  4. Write results to three target tables on ORA_BIIS:
       - NWK_ACTION_PRIMARY_TBL
       - NWK_ACTION_SECONDARY_TBL
       - EHRP_RECS_TRACKING_TBL
"""

import logging

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    broadcast,
    col,
    concat,
    current_date,
    isnull,
    lit,
    row_number,
    trim,
    when,
    year,
)
from pyspark.sql.window import Window

from pyspark_migration.utils.db import (
    get_src_jdbc_url,
    get_src_jdbc_properties,
    get_tgt_jdbc_url,
    get_tgt_jdbc_properties,
    get_nate_jdbc_url,
    get_nate_jdbc_properties,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Source read helpers
# ---------------------------------------------------------------------------

def _read_source_tables(spark: SparkSession) -> DataFrame:
    """Read and join the two primary source tables (replaces SQ_PS_GVT_JOB)."""
    url = get_src_jdbc_url()
    props = get_src_jdbc_properties()

    actions = spark.read.jdbc(url, "NWK_NEW_EHRP_ACTIONS_TBL", properties=props)
    ps_gvt_job = spark.read.jdbc(url, "PS_GVT_JOB", properties=props)

    df = actions.join(
        ps_gvt_job,
        (actions.EMPLID == ps_gvt_job.EMPLID)
        & (actions.EMPL_RCD == ps_gvt_job.EMPL_RCD)
        & (actions.EFFDT == ps_gvt_job.EFFDT)
        & (actions.EFFSEQ == ps_gvt_job.EFFSEQ),
    )

    return df


# ---------------------------------------------------------------------------
# Lookup joins (replaces lkp_* Lookup Procedures)
# ---------------------------------------------------------------------------

def _apply_lookup_joins(spark: SparkSession, df: DataFrame) -> DataFrame:
    """Apply all broadcast lookup joins.

    All lookups read from ORA_BIISPRD_SRC except lkp_PS_JPM_JP_ITEMS which
    uses the INFO_NATE connection.
    """
    url_src = get_src_jdbc_url()
    props_src = get_src_jdbc_properties()

    # --- Lookups from ORA_BIISPRD_SRC ---

    # lkp_PS_GVT_EMPLOYMENT
    employment = spark.read.jdbc(url_src, "PS_GVT_EMPLOYMENT", properties=props_src)
    df = df.join(
        broadcast(employment),
        ["EMPLID", "EMPL_RCD", "EFFDT", "EFFSEQ"],
        "left",
    )

    # lkp_PS_GVT_PERS_NID
    pers_nid = spark.read.jdbc(url_src, "PS_GVT_PERS_NID", properties=props_src)
    df = df.join(broadcast(pers_nid), ["EMPLID"], "left")

    # lkp_PS_GVT_AWD_DATA
    awd_data = spark.read.jdbc(url_src, "PS_GVT_AWD_DATA", properties=props_src)
    df = df.join(
        broadcast(awd_data),
        ["EMPLID", "EMPL_RCD", "EFFDT", "EFFSEQ"],
        "left",
    )

    # lkp_PS_GVT_EE_DATA_TRK
    ee_data_trk = spark.read.jdbc(url_src, "PS_GVT_EE_DATA_TRK", properties=props_src)
    df = df.join(
        broadcast(ee_data_trk),
        ["EMPLID", "EMPL_RCD", "EFFDT", "EFFSEQ"],
        "left",
    )

    # lkp_PS_HE_FILL_POS
    he_fill_pos = spark.read.jdbc(url_src, "PS_HE_FILL_POS", properties=props_src)
    df = df.join(broadcast(he_fill_pos), ["POSITION_NBR"], "left")

    # lkp_PS_GVT_CITIZENSHIP
    citizenship = spark.read.jdbc(url_src, "PS_GVT_CITIZENSHIP", properties=props_src)
    df = df.join(broadcast(citizenship), ["EMPLID"], "left")

    # lkp_PS_GVT_PERS_DATA
    pers_data = spark.read.jdbc(url_src, "PS_GVT_PERS_DATA", properties=props_src)
    df = df.join(broadcast(pers_data), ["EMPLID", "EFFDT"], "left")

    # lkp_OLD_SEQUENCE_NUMBER
    seq_num = spark.read.jdbc(url_src, "SEQUENCE_NUM_TBL", properties=props_src)
    df = df.join(broadcast(seq_num).alias("seq"), how="cross")

    # --- Lookup from INFO_NATE (different connection) ---

    # lkp_PS_JPM_JP_ITEMS
    url_nate = get_nate_jdbc_url()
    props_nate = get_nate_jdbc_properties()
    jpm_jp_items = spark.read.jdbc(url_nate, "PS_JPM_JP_ITEMS", properties=props_nate)
    df = df.join(broadcast(jpm_jp_items), ["JOBCODE"], "left")

    return df


# ---------------------------------------------------------------------------
# Expression transformations
# ---------------------------------------------------------------------------

def _apply_expressions(df: DataFrame) -> DataFrame:
    """Apply expression transformations.

    Replaces:
      - exp_MAIN2BIIS
      - exp_GET_EFFDT_YEAR
      - exp_PERS_DATA
    """

    # o_AGCY_ASSIGN_CD = COMPANY || GVT_SUB_AGENCY
    df = df.withColumn(
        "o_AGCY_ASSIGN_CD",
        concat(col("COMPANY"), col("GVT_SUB_AGENCY")),
    )

    # o_AGCY_SUBELEMENT_PRIOR_CD: NULL when GVT_XFER_FROM_AGCY is null/spaces,
    # otherwise GVT_XFER_FROM_AGCY || '00'
    df = df.withColumn(
        "o_AGCY_SUBELEMENT_PRIOR_CD",
        when(
            isnull(col("GVT_XFER_FROM_AGCY"))
            | (trim(col("GVT_XFER_FROM_AGCY")) == ""),
            lit(None).cast("string"),
        ).otherwise(concat(col("GVT_XFER_FROM_AGCY"), lit("00"))),
    )

    # o_EFFDT_YEAR — extract year from EFFDT for partitioning EVENT_ID
    df = df.withColumn("o_EFFDT_YEAR", year(col("EFFDT")))

    # v_EVENT_ID — sequential counter per year, starting from the previous
    # EHRP_SEQ_NUMBER stored in SEQUENCE_NUM_TBL.
    # Uses a PySpark Window function with row_number().
    window_spec = Window.partitionBy("o_EFFDT_YEAR").orderBy(
        col("EMPLID"), col("EMPL_RCD"), col("EFFDT"), col("EFFSEQ")
    )
    df = df.withColumn(
        "v_EVENT_ID",
        col("EHRP_SEQ_NUMBER") + row_number().over(window_spec),
    )

    # LOAD_DATE = today's date
    df = df.withColumn("LOAD_DATE", current_date())

    return df


# ---------------------------------------------------------------------------
# Target writes
# ---------------------------------------------------------------------------

def _select_primary_columns(df: DataFrame) -> DataFrame:
    """Select columns destined for NWK_ACTION_PRIMARY_TBL."""
    primary_cols = [
        "v_EVENT_ID",
        "EMPLID",
        "EMPL_RCD",
        "EFFDT",
        "EFFSEQ",
        "o_AGCY_ASSIGN_CD",
        "o_AGCY_SUBELEMENT_PRIOR_CD",
        "ACTION",
        "ACTION_REASON",
        "ACTION_DT",
        "DEPTID",
        "JOBCODE",
        "POSITION_NBR",
        "GVT_NOA_CODE",
        "GVT_PAY_PLAN",
        "GVT_GRADE",
        "GVT_STEP",
        "COMPANY",
        "GVT_SUB_AGENCY",
        "LOCATION",
        "EMPL_STATUS",
        "LOAD_DATE",
    ]
    available = [c for c in primary_cols if c in df.columns]
    return df.select(available)


def _select_secondary_columns(df: DataFrame) -> DataFrame:
    """Select columns destined for NWK_ACTION_SECONDARY_TBL."""
    secondary_cols = [
        "v_EVENT_ID",
        "EMPLID",
        "EMPL_RCD",
        "EFFDT",
        "EFFSEQ",
        "GVT_AWARD_AMOUNT",
        "GVT_AWARD_PCT",
        "GVT_WITHIN_GRADE_DT",
        "FULL_PART_TIME",
        "REG_TEMP",
        "SHIFT",
        "WORK_DAY_HOURS",
        "FTE",
    ]
    available = [c for c in secondary_cols if c in df.columns]
    return df.select(available)


def _select_tracking_columns(df: DataFrame) -> DataFrame:
    """Select columns destined for EHRP_RECS_TRACKING_TBL."""
    tracking_cols = [
        "v_EVENT_ID",
        "EMPLID",
        "EMPL_RCD",
        "EFFDT",
        "EFFSEQ",
        "GVT_NOA_CODE",
        "GVT_WIP_STATUS",
        "ACTION",
        "ACTION_REASON",
        "LOAD_DATE",
    ]
    available = [c for c in tracking_cols if c in df.columns]
    return df.select(available)


def _write_targets(df: DataFrame) -> None:
    """Write transformed data to the three target Oracle tables."""
    url = get_tgt_jdbc_url()
    props = get_tgt_jdbc_properties()

    df_primary = _select_primary_columns(df)
    df_secondary = _select_secondary_columns(df)
    df_tracking = _select_tracking_columns(df)

    logger.info("Writing %d rows to NWK_ACTION_PRIMARY_TBL", df_primary.count())
    df_primary.write.jdbc(
        url, "NWK_ACTION_PRIMARY_TBL", mode="append", properties=props
    )

    logger.info("Writing %d rows to NWK_ACTION_SECONDARY_TBL", df_secondary.count())
    df_secondary.write.jdbc(
        url, "NWK_ACTION_SECONDARY_TBL", mode="append", properties=props
    )

    logger.info("Writing %d rows to EHRP_RECS_TRACKING_TBL", df_tracking.count())
    df_tracking.write.jdbc(
        url, "EHRP_RECS_TRACKING_TBL", mode="append", properties=props
    )


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def run() -> None:
    """Execute the full EHRP2BIIS Update PySpark job."""
    spark = (
        SparkSession.builder
        .appName("EHRP2BIIS_UPDATE")
        .getOrCreate()
    )

    try:
        logger.info("Starting EHRP2BIIS Update job")

        # 1. Read and join source tables
        df = _read_source_tables(spark)

        # 2. Apply lookup joins
        df = _apply_lookup_joins(spark, df)

        # 3. Apply expression transformations
        df = _apply_expressions(df)

        # 4. Write to target tables
        _write_targets(df)

        logger.info("EHRP2BIIS Update job completed successfully")

    finally:
        spark.stop()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run()
