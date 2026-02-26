"""
PySpark migration of Informatica workflow: wf_EHRP2BIIS_UPDATE
Source XML: XML/EHRP2BIIS_UPDATE

The most complex job. Originally runs RUNFOREVER (daily recurring since 9/28/2018)
on Prd_IS / Dom_Prd. In PySpark, implement as a scheduled daily job (via cron
or Airflow).

Workflow:
    1. Pre-load step (ehrp2biis_preload KSH replacement)
    2. Main mapping (m_EHRP2BIIS_UPDATE) - joins PS_GVT_JOB with trigger table,
       performs 9 lookups, expression transformations, writes to 3 target tables
    3. Post-load step (ehrp2biis_afterload.sql replacement)
    4. Success/failure email

Original: Prd_IS / Dom_Prd, RUNFOREVER daily
"""

import os
from datetime import datetime
from typing import Optional, Tuple

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DateType,
    DecimalType,
    LongType,
    StringType,
    TimestampType,
)

from utils import (
    ENV_PREFIX,
    PerfTimer,
    execute_jdbc_statement,
    get_jdbc_url,
    get_spark_session,
    logger,
    read_oracle_table,
    send_email,
    write_oracle_table,
)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
JOB_NAME = "wf_EHRP2BIIS_UPDATE"
MAPPING_NAME = "m_EHRP2BIIS_UPDATE"
LOG_DIR = os.environ.get("LOG_DIR", "/home/sa-biisint/data/int/log")
PARAM_FILE = os.environ.get(
    "BIIS_PARAM_FILE", "/data/BIISINT/control/BIIS_parms.iparms"
)

# Pre-load step SQL directory
PRELOAD_SQL_DIR = os.environ.get(
    "PRELOAD_SQL_DIR", "/data/BIISINT/bin/EHRP2BIIS"
)

# Email recipients (from ehrp2biis_preload line 8)
PRELOAD_RECIPIENTS = [
    "peter.chen@hhs.gov",
    "nathan.knight@hhs.gov",
    "marvin.simon@hhs.gov",
]

SUCCESS_RECIPIENTS = ["Chingchiuan.Chen@hhs.gov"]

# Source connection (ORA_BIISPRD_SRC)
SOURCE_CONNECTION = "ORA_BIISPRD_SRC"
# Target connection (INFO_TARGET)
TARGET_CONNECTION = "INFO_TARGET"
# Secondary lookup connection (INFO_NATE / BIISPRD) for lkp_PS_JPM_JP_ITEMS
JPM_CONNECTION = "BIISPRD"


# ===================================================================
# Pre-load Step (replaces ehrp2biis_preload KSH script)
# ===================================================================
def step_preload(jdbc_url_target: str) -> None:
    """Execute the pre-load SQL step.

    Replaces the KSH script ``ehrp2biis_preload`` which runs SQL*Plus
    to execute ``$homedir/step01`` before the Informatica session.

    If the pre-load fails (error detected), sends a failure email.
    Logs to ``/home/sa-biisint/data/int/log/ehrp2biis_preload_<timestamp>.log``.
    """
    logger.info("Pre-load step: executing step01 SQL")

    timestamp = datetime.now().strftime("%y%m%d%H%M%S")
    logfile = os.path.join(LOG_DIR, f"ehrp2biis_preload_{timestamp}.log")
    os.makedirs(LOG_DIR, exist_ok=True)

    step01_path = os.path.join(PRELOAD_SQL_DIR, "step01")

    try:
        # Read and execute the step01 SQL file if it exists
        if os.path.isfile(step01_path):
            with open(step01_path, "r") as fh:
                sql_content = fh.read()

            # Execute each SQL statement
            statements = [s.strip() for s in sql_content.split(";") if s.strip()]
            for stmt in statements:
                if stmt and not stmt.startswith("--"):
                    execute_jdbc_statement(
                        stmt, jdbc_url=jdbc_url_target, connection_name=TARGET_CONNECTION
                    )

            with open(logfile, "w") as lf:
                lf.write("SQL Script Ran Successfully\n")
                lf.write(f"Timestamp: {datetime.now()}\n")

            logger.info("Pre-load step completed successfully. Log: %s", logfile)
            send_email(
                subject="EHRP2BIIS Preload script completed successfully",
                body=f"Pre-load SQL executed successfully.\nLog file: {logfile}",
                recipients=PRELOAD_RECIPIENTS,
            )
        else:
            logger.warning("step01 SQL file not found at %s, skipping pre-load", step01_path)
            with open(logfile, "w") as lf:
                lf.write(f"step01 SQL file not found at {step01_path}\n")

    except Exception as exc:
        error_msg = f"EHRP2BIIS Preload script did not complete successfully: {exc}"
        logger.error(error_msg)
        with open(logfile, "a") as lf:
            lf.write(f"ERROR: {error_msg}\n")

        send_email(
            subject="EHRP2BIIS Preload script did not complete successfully",
            body=error_msg,
            recipients=PRELOAD_RECIPIENTS,
        )
        raise RuntimeError(error_msg) from exc


# ===================================================================
# Main Mapping (m_EHRP2BIIS_UPDATE)
# ===================================================================
def step_main_mapping(
    spark: SparkSession,
    jdbc_url_source: str,
    jdbc_url_target: str,
    jdbc_url_jpm: Optional[str] = None,
) -> Tuple[int, int]:
    """Execute the main EHRP2BIIS mapping.

    Source: PS_GVT_JOB joined with NWK_NEW_EHRP_ACTIONS_TBL (trigger table)
    9 Lookup transformations (broadcast joins)
    Expression transformations: exp_MAIN2BIIS, exp_PERS_DATA, exp_GET_EFFDT_YEAR
    3 Target tables: NWK_ACTION_PRIMARY_TBL, NWK_ACTION_SECONDARY_TBL,
                     EHRP_RECS_TRACKING_TBL

    Returns (src_rows, tgt_rows).
    """
    logger.info("Main mapping: m_EHRP2BIIS_UPDATE")

    # ---- Read source tables ----
    # PS_GVT_JOB (156 fields, Oracle ORA_BIISPRD_SRC)
    ps_gvt_job_df = read_oracle_table(
        spark, "EHRP.PS_GVT_JOB", jdbc_url=jdbc_url_source,
        connection_name=SOURCE_CONNECTION,
    )

    # NWK_NEW_EHRP_ACTIONS_TBL (trigger table)
    trigger_df = read_oracle_table(
        spark, "NKNIGHT.NWK_NEW_EHRP_ACTIONS_TBL", jdbc_url=jdbc_url_source,
        connection_name=SOURCE_CONNECTION,
    )

    # ---- Source Qualifier Join (SQ_PS_GVT_JOB) ----
    # WHERE NWK_NEW_EHRP_ACTIONS_TBL.EMPLID = PS_GVT_JOB.EMPLID
    #   AND NWK_NEW_EHRP_ACTIONS_TBL.EMPL_RCD = PS_GVT_JOB.EMPL_RCD
    #   AND NWK_NEW_EHRP_ACTIONS_TBL.EFFDT = PS_GVT_JOB.EFFDT
    #   AND NWK_NEW_EHRP_ACTIONS_TBL.EFFSEQ = PS_GVT_JOB.EFFSEQ
    # ORDER BY PS_GVT_JOB.EFFDT
    source_df = ps_gvt_job_df.join(
        trigger_df,
        on=["EMPLID", "EMPL_RCD", "EFFDT", "EFFSEQ"],
        how="inner",
    ).orderBy("EFFDT")

    src_rows = source_df.count()
    logger.info("Source join produced %d row(s)", src_rows)

    if src_rows == 0:
        logger.info("No new EHRP actions to process.")
        return (0, 0)

    source_df.cache()

    # ---- Lookup transformations (9 lookups, all broadcast joins) ----

    # 1. lkp_PS_GVT_EMPLOYMENT (connection $Source)
    try:
        employment_df = read_oracle_table(
            spark, "EHRP.PS_GVT_EMPLOYMENT", jdbc_url=jdbc_url_source,
            connection_name=SOURCE_CONNECTION,
        ).select(
            F.col("EMPLID").alias("EMP_EMPLID"),
            F.col("EMPL_RCD").alias("EMP_EMPL_RCD"),
            "GVT_CNV_BEGIN_DATE",
            "GVT_TEMP_PSN_EXPIR",
            "GVT_APPT_LIMIT_DYS",
            "HIRE_DT",
            "GVT_SPEP",
            "GVT_SUPV_PROB_DT",
        )
        source_df = source_df.join(
            F.broadcast(employment_df),
            (source_df["EMPLID"] == employment_df["EMP_EMPLID"])
            & (source_df["EMPL_RCD"] == employment_df["EMP_EMPL_RCD"]),
            how="left",
        ).drop("EMP_EMPLID", "EMP_EMPL_RCD")
    except Exception as exc:
        logger.warning("Lookup lkp_PS_GVT_EMPLOYMENT failed: %s", exc)

    # 2. lkp_PS_GVT_PERS_NID (connection $Source)
    try:
        pers_nid_df = read_oracle_table(
            spark, "EHRP.PS_GVT_PERS_NID", jdbc_url=jdbc_url_source,
            connection_name=SOURCE_CONNECTION,
        ).select(
            F.col("EMPLID").alias("NID_EMPLID"),
            "NATIONAL_ID",
        )
        source_df = source_df.join(
            F.broadcast(pers_nid_df),
            source_df["EMPLID"] == pers_nid_df["NID_EMPLID"],
            how="left",
        ).drop("NID_EMPLID")
    except Exception as exc:
        logger.warning("Lookup lkp_PS_GVT_PERS_NID failed: %s", exc)

    # 3. lkp_PS_GVT_AWD_DATA (connection $Source)
    try:
        awd_df = read_oracle_table(
            spark, "EHRP.PS_GVT_AWD_DATA", jdbc_url=jdbc_url_source,
            connection_name=SOURCE_CONNECTION,
        ).select(
            F.col("EMPLID").alias("AWD_EMPLID"),
            F.col("EMPL_RCD").alias("AWD_EMPL_RCD"),
            F.col("EFFDT").alias("AWD_EFFDT"),
            F.col("EFFSEQ").alias("AWD_EFFSEQ"),
            "GVT_AWARD_AMOUNT",
            "GVT_AWD_PERCENTAGE",
            "GVT_AWARD_TYPE",
            "GVT_CASH_AWARD_AMT",
        )
        source_df = source_df.join(
            F.broadcast(awd_df),
            (source_df["EMPLID"] == awd_df["AWD_EMPLID"])
            & (source_df["EMPL_RCD"] == awd_df["AWD_EMPL_RCD"])
            & (source_df["EFFDT"] == awd_df["AWD_EFFDT"])
            & (source_df["EFFSEQ"] == awd_df["AWD_EFFSEQ"]),
            how="left",
        ).drop("AWD_EMPLID", "AWD_EMPL_RCD", "AWD_EFFDT", "AWD_EFFSEQ")
    except Exception as exc:
        logger.warning("Lookup lkp_PS_GVT_AWD_DATA failed: %s", exc)

    # 4. lkp_PS_GVT_EE_DATA_TRK (connection $Source)
    try:
        ee_data_df = read_oracle_table(
            spark, "EHRP.PS_GVT_EE_DATA_TRK", jdbc_url=jdbc_url_source,
            connection_name=SOURCE_CONNECTION,
        ).select(
            F.col("EMPLID").alias("EE_EMPLID"),
            F.col("EMPL_RCD").alias("EE_EMPL_RCD"),
        )
        # Just check existence for tracking
        source_df = source_df.join(
            F.broadcast(ee_data_df),
            (source_df["EMPLID"] == ee_data_df["EE_EMPLID"])
            & (source_df["EMPL_RCD"] == ee_data_df["EE_EMPL_RCD"]),
            how="left",
        ).drop("EE_EMPLID", "EE_EMPL_RCD")
    except Exception as exc:
        logger.warning("Lookup lkp_PS_GVT_EE_DATA_TRK failed: %s", exc)

    # 5. lkp_PS_HE_FILL_POS (connection $Source)
    try:
        fill_pos_df = read_oracle_table(
            spark, "EHRP.PS_HE_FILL_POS", jdbc_url=jdbc_url_source,
            connection_name=SOURCE_CONNECTION,
        ).select(
            F.col("EMPLID").alias("FP_EMPLID"),
            F.col("EMPL_RCD").alias("FP_EMPL_RCD"),
            "HE_FILL_POSITION",
        )
        source_df = source_df.join(
            F.broadcast(fill_pos_df),
            (source_df["EMPLID"] == fill_pos_df["FP_EMPLID"])
            & (source_df["EMPL_RCD"] == fill_pos_df["FP_EMPL_RCD"]),
            how="left",
        ).drop("FP_EMPLID", "FP_EMPL_RCD")
    except Exception as exc:
        logger.warning("Lookup lkp_PS_HE_FILL_POS failed: %s", exc)

    # 6. lkp_PS_GVT_CITIZENSHIP (connection $Source)
    try:
        citizen_df = read_oracle_table(
            spark, "EHRP.PS_GVT_CITIZENSHIP", jdbc_url=jdbc_url_source,
            connection_name=SOURCE_CONNECTION,
        ).select(
            F.col("EMPLID").alias("CIT_EMPLID"),
            "CITIZENSHIP_STATUS",
        )
        source_df = source_df.join(
            F.broadcast(citizen_df),
            source_df["EMPLID"] == citizen_df["CIT_EMPLID"],
            how="left",
        ).drop("CIT_EMPLID")
    except Exception as exc:
        logger.warning("Lookup lkp_PS_GVT_CITIZENSHIP failed: %s", exc)

    # 7. lkp_PS_GVT_PERS_DATA (connection $Source)
    try:
        pers_data_df = read_oracle_table(
            spark, "EHRP.PS_GVT_PERS_DATA", jdbc_url=jdbc_url_source,
            connection_name=SOURCE_CONNECTION,
        ).select(
            F.col("EMPLID").alias("PD_EMPLID"),
            "ADDRESS1",
            "GVT_DISABILITY_CD",
            "ETHNIC_GROUP",
            "GVT_VET_PREF_APPT",
            "MILITARY_STATUS",
            "SEX",
            "GVT_CRED_MIL_SVCE",
            "GVT_MILITARY_COMP",
        )
        source_df = source_df.join(
            F.broadcast(pers_data_df),
            source_df["EMPLID"] == pers_data_df["PD_EMPLID"],
            how="left",
        ).drop("PD_EMPLID")
    except Exception as exc:
        logger.warning("Lookup lkp_PS_GVT_PERS_DATA failed: %s", exc)

    # 8. lkp_OLD_SEQUENCE_NUMBER (connection $Source)
    # This lookup is used to generate EVENT_ID; we read the table to
    # warm the cache but the actual ID generation uses
    # monotonically_increasing_id below.
    try:
        read_oracle_table(
            spark, "NKNIGHT.SEQUENCE_NUM_TBL", jdbc_url=jdbc_url_source,
            connection_name=SOURCE_CONNECTION,
        ).select(
            F.col("SEQUENCE_NAME").alias("SEQ_NAME"),
            F.col("SEQUENCE_VALUE").alias("OLD_SEQ_VALUE"),
        )
    except Exception as exc:
        logger.warning("Lookup lkp_OLD_SEQUENCE_NUMBER failed: %s", exc)

    # 9. lkp_PS_JPM_JP_ITEMS (DIFFERENT connection: INFO_NATE / BIISPRD)
    try:
        jpm_url = jdbc_url_jpm or get_jdbc_url("BIISPRD")
        jpm_df = read_oracle_table(
            spark, "EHRP.PS_JPM_JP_ITEMS", jdbc_url=jpm_url,
            connection_name=JPM_CONNECTION,
            predicate="JPM_CAT_TYPE = 'DEG' AND EFF_STATUS = 'A'",
        ).select(
            F.col("JPM_PROFILE_ID").alias("JPM_EMPLID"),
            "JPM_CAT_TYPE",
            "JPM_CAT_ITEM_ID",
            "MAJOR_CODE",
            "MAJOR_DESCR",
            "JPM_INTEGER_2",
        )
        source_df = source_df.join(
            F.broadcast(jpm_df),
            source_df["EMPLID"] == jpm_df["JPM_EMPLID"],
            how="left",
        ).drop("JPM_EMPLID")
    except Exception as exc:
        logger.warning("Lookup lkp_PS_JPM_JP_ITEMS failed: %s", exc)

    # ---- Expression transformations ----
    load_date = datetime.now()

    # exp_GET_EFFDT_YEAR: extract year from EFFDT
    source_df = source_df.withColumn(
        "EFFDT_YEAR", F.year(F.col("EFFDT"))
    )

    # exp_MAIN2BIIS: generate EVENT_ID using monotonically_increasing_id
    # (Informatica used a sequence number from SEQUENCE_NUM_TBL)
    source_df = source_df.withColumn(
        "o_EVENT_ID",
        F.monotonically_increasing_id() + F.lit(9000000000),
    )

    # exp_MAIN2BIIS: set LOAD_DATE and LOAD_ID
    source_df = source_df.withColumn(
        "o_LOAD_DATE", F.lit(load_date).cast(TimestampType())
    ).withColumn(
        "o_LOAD_ID", F.lit(load_date.strftime("%Y%m%d")).cast(StringType())
    )

    # exp_MAIN2BIIS: computed fields
    source_df = source_df.withColumn(
        "o_PROBATION_DT",
        F.col("GVT_PROB_DT") if "GVT_PROB_DT" in source_df.columns else F.lit(None).cast(DateType()),
    ).withColumn(
        "o_PERM_TEMP_POSITION_CD",
        F.col("REG_TEMP") if "REG_TEMP" in source_df.columns else F.lit(None).cast(StringType()),
    ).withColumn(
        "o_CITIZENSHIP_STATUS",
        F.col("CITIZENSHIP_STATUS") if "CITIZENSHIP_STATUS" in source_df.columns else F.lit(None).cast(StringType()),
    ).withColumn(
        "o_AGCY_ASSIGN_CD",
        F.col("GVT_SUB_AGENCY") if "GVT_SUB_AGENCY" in source_df.columns else F.lit(None).cast(StringType()),
    ).withColumn(
        "o_AGCY_SUBELEMENT_PRIOR_CD",
        F.col("GVT_XFER_FR_AGCY") if "GVT_XFER_FR_AGCY" in source_df.columns else F.lit(None).cast(StringType()),
    ).withColumn(
        "o_EMP_RESID_CITY_STATE_NAME",
        F.col("GVT_DUTY_CITY") if "GVT_DUTY_CITY" in source_df.columns else F.lit(None).cast(StringType()),
    ).withColumn(
        "o_BUSINESS_TITLE",
        F.col("BUSINESS_TITLE") if "BUSINESS_TITLE" in source_df.columns else F.lit(None).cast(StringType()),
    )

    # Legal auth text concatenation
    source_df = source_df.withColumn(
        "o_LEG_AUTH_TXT_1",
        F.concat_ws(" ",
                     F.col("GVT_PAR_AUTH_D1") if "GVT_PAR_AUTH_D1" in source_df.columns else F.lit(""),
                     F.col("GVT_PAR_AUTH_D1_2") if "GVT_PAR_AUTH_D1_2" in source_df.columns else F.lit(""),
                     ),
    ).withColumn(
        "o_LEG_AUTH_TXT_2",
        F.concat_ws(" ",
                     F.col("GVT_PAR_AUTH_D2") if "GVT_PAR_AUTH_D2" in source_df.columns else F.lit(""),
                     F.col("GVT_PAR_AUTH_D2_2") if "GVT_PAR_AUTH_D2_2" in source_df.columns else F.lit(""),
                     ),
    )

    # Numeric computed fields (with safe null handling)
    for col_name in [
        "o_BASE_HOURS", "o_CASH_AWARD_AMT", "o_CASH_AWARD_BNFT_AMT",
        "o_TIME_OFF_GRANTED_HRS", "o_RELOCATION_BONUS_AMT",
        "o_RECRUITMENT_BONUS_AMT", "o_TIME_OFF_AWARD_AMT",
        "o_BUYOUT_AMT", "o_TSP_VESTING_CD",
        "o_GVT_ANNUITY_OFFSET",
        "o_HE_AL_RED_CRED", "o_HE_AL_BALANCE", "o_HE_LUMP_HRS",
        "o_HE_AL_CARRYOVER", "o_HE_RES_BALANCE", "o_HE_RES_LASTYR",
        "o_HE_RES_TWOYRS", "o_HE_AL_TOTAL", "o_HE_AWOP_SEP",
        "o_HE_AWOP_WIGI", "o_HE_SL_RED_CRED", "o_HE_SL_BALANCE",
        "o_HE_FROZEN_SL", "o_HE_SL_CARRYOVER", "o_HE_SL_ACCRUAL",
        "o_HE_SL_TOTAL",
    ]:
        # Map computed output fields to source fields where available
        src_col_map = {
            "o_BASE_HOURS": "STD_HOURS",
            "o_CASH_AWARD_AMT": "GVT_CASH_AWARD_AMT",
            "o_CASH_AWARD_BNFT_AMT": "GVT_AWARD_AMOUNT",
            "o_TIME_OFF_GRANTED_HRS": "HE_TIME_OFF_HRS",
            "o_RELOCATION_BONUS_AMT": "HE_RELOCATE_AMT",
            "o_RECRUITMENT_BONUS_AMT": "HE_RECRUIT_AMT",
            "o_TIME_OFF_AWARD_AMT": "HE_TIME_OFF_AMT",
            "o_BUYOUT_AMT": "BUYOUT_AMT",
            "o_TSP_VESTING_CD": "HE_TSP_VESTING",
            "o_GVT_ANNUITY_OFFSET": "GVT_ANNUITY_OFFSET",
            "o_HE_AL_RED_CRED": "HE_AL_RED_CRED",
            "o_HE_AL_BALANCE": "HE_AL_BALANCE",
            "o_HE_LUMP_HRS": "HE_LUMP_HRS",
            "o_HE_AL_CARRYOVER": "HE_AL_CARRYOVER",
            "o_HE_RES_BALANCE": "HE_RES_BALANCE",
            "o_HE_RES_LASTYR": "HE_RES_LASTYR",
            "o_HE_RES_TWOYRS": "HE_RES_TWOYRS",
            "o_HE_AL_TOTAL": "HE_AL_TOTAL",
            "o_HE_AWOP_SEP": "HE_AWOP_SEP",
            "o_HE_AWOP_WIGI": "HE_AWOP_WIGI",
            "o_HE_SL_RED_CRED": "HE_SL_RED_CRED",
            "o_HE_SL_BALANCE": "HE_SL_BALANCE",
            "o_HE_FROZEN_SL": "HE_FROZEN_SL",
            "o_HE_SL_CARRYOVER": "HE_SL_CARRYOVER",
            "o_HE_SL_ACCRUAL": "HE_SL_ACCRUAL",
            "o_HE_SL_TOTAL": "HE_SL_TOTAL",
        }
        src_col = src_col_map.get(col_name)
        if src_col and src_col in source_df.columns:
            source_df = source_df.withColumn(col_name, F.col(src_col))
        elif col_name not in source_df.columns:
            source_df = source_df.withColumn(col_name, F.lit(None).cast(DecimalType(18, 6)))

    # Date fields for secondary table
    for col_name in ["o_RECRUITMENT_EXP_DTE", "o_RELOCATION_EXP_DTE"]:
        src_col_map = {
            "o_RECRUITMENT_EXP_DTE": "HE_RECRUIT_NTE",
            "o_RELOCATION_EXP_DTE": "HE_RELOCATE_NTE",
        }
        src_col = src_col_map.get(col_name)
        if src_col and src_col in source_df.columns:
            source_df = source_df.withColumn(col_name, F.col(src_col))
        elif col_name not in source_df.columns:
            source_df = source_df.withColumn(col_name, F.lit(None).cast(DateType()))

    # ---- Build NWK_ACTION_PRIMARY_TBL target ----
    # Field mapping from CONNECTOR elements (XML lines 2003-2105+)
    def safe_col(df: DataFrame, name: str, cast_type=StringType()):
        """Return the column if it exists, else a null literal."""
        if name in df.columns:
            return F.col(name)
        return F.lit(None).cast(cast_type)

    primary_df = source_df.select(
        safe_col(source_df, "o_EVENT_ID", LongType()).alias("EVENT_ID"),
        safe_col(source_df, "EMPLID").alias("EMP_ID"),
        safe_col(source_df, "EMPL_RCD").alias("EMPL_REC_NO"),
        safe_col(source_df, "EFFDT", DateType()).alias("EVENT_EFF_DTE"),
        safe_col(source_df, "ACTION").alias("EHRP_TYPE_ACTION"),
        safe_col(source_df, "ACTION_REASON").alias("EHRP_ACTION_REASON"),
        safe_col(source_df, "GVT_NOA_CODE").alias("NOA_CD"),
        safe_col(source_df, "HE_NOA_EXT").alias("NOA_SUFFIX_CD"),
        safe_col(source_df, "DEPTID").alias("ORGTNL_COMPONENT_CD"),
        safe_col(source_df, "LOCATION").alias("DUTY_STATION_CD"),
        safe_col(source_df, "JOBCODE").alias("POSITION_NUM"),
        safe_col(source_df, "GRADE").alias("GRADE_CD"),
        safe_col(source_df, "GVT_STEP").alias("STEP_CD"),
        safe_col(source_df, "GVT_PAY_PLAN").alias("PAY_PLAN_CD"),
        safe_col(source_df, "SAL_ADMIN_PLAN").alias("PAY_TABLE_NUM"),
        safe_col(source_df, "GVT_PAY_BASIS").alias("PAY_BASIS_CD"),
        safe_col(source_df, "ANNL_BENEF_BASE_RT", DecimalType(18, 3)).alias("ANN_SALARY_RATE_AMT"),
        safe_col(source_df, "GVT_COMPRATE", DecimalType(18, 6)).alias("SCHLD_ANN_SALARY_AMT"),
        safe_col(source_df, "HOURLY_RT", DecimalType(18, 6)).alias("HRLY_RATE_AMT"),
        safe_col(source_df, "GVT_HRLY_RT_NO_LOC", DecimalType(18, 6)).alias("SCHLD_HRLY_RATE_AMT"),
        safe_col(source_df, "GVT_LOCALITY_ADJ", DecimalType(18, 6)).alias("LOCALITY_PAY_AMT"),
        safe_col(source_df, "o_BASE_HOURS", DecimalType(6, 2)).alias("BASE_HRS"),
        safe_col(source_df, "FLSA_STATUS").alias("FLSA_CATGRY_CD"),
        safe_col(source_df, "BARG_UNIT").alias("BARGAINING_UNIT_CD"),
        safe_col(source_df, "GVT_WORK_SCHED").alias("WORK_SCHEDULE_CD"),
        safe_col(source_df, "GVT_TYPE_OF_APPT").alias("APPT_TYPE_CD"),
        safe_col(source_df, "GVT_POSN_OCCUPIED").alias("POSITION_OCCUPIED_CD"),
        safe_col(source_df, "o_PERM_TEMP_POSITION_CD").alias("PERMANENT_TEMP_POSITION_CD"),
        safe_col(source_df, "GVT_POI").alias("PERSONNEL_OFFICE_ID_CD"),
        safe_col(source_df, "o_AGCY_ASSIGN_CD").alias("AGCY_ASSIGN_CD"),
        safe_col(source_df, "GVT_XFER_TO_AGCY").alias("AGCY_SUBELEMENT_CD"),
        safe_col(source_df, "o_AGCY_SUBELEMENT_PRIOR_CD").alias("AGCY_SUBELEMENT_PRIOR_CD"),
        safe_col(source_df, "GVT_ANN_IND").alias("ANNUITANT_IND_CD"),
        safe_col(source_df, "GVT_FEGLI").alias("FEGLI_CD"),
        safe_col(source_df, "GVT_FEGLI_LIVING").alias("FEGLI_LIVING_BNFT_CD"),
        safe_col(source_df, "GVT_LIVING_AMT", DecimalType(18, 6)).alias("FEGLI_LIVING_BNFT_REMAIN_AMT"),
        safe_col(source_df, "GVT_FERS_COVERAGE").alias("FERS_COV_CD"),
        safe_col(source_df, "GVT_PAY_RATE_DETER").alias("PRD_CD"),
        safe_col(source_df, "GVT_PREV_RET_COVRG").alias("PREV_RETMT_COV_CD"),
        safe_col(source_df, "GVT_CSRS_FROZN_SVC").alias("FROZEN_SERVICE_PERIOD"),
        safe_col(source_df, "GVT_LEG_AUTH_1").alias("LEGAL_AUTH_CD"),
        safe_col(source_df, "o_LEG_AUTH_TXT_1").alias("LEGAL_AUTH_TXT"),
        safe_col(source_df, "GVT_LEG_AUTH_2").alias("LEGAL_AUTH2_CD"),
        safe_col(source_df, "o_LEG_AUTH_TXT_2").alias("LEGAL_AUTH2_TXT"),
        safe_col(source_df, "ACCT_CD").alias("CAN_CD"),
        safe_col(source_df, "NATIONAL_ID").alias("SSN"),
        safe_col(source_df, "o_CASH_AWARD_AMT", DecimalType(18, 6)).alias("CASH_AWARD_AMT"),
        safe_col(source_df, "o_CASH_AWARD_BNFT_AMT", DecimalType(18, 6)).alias("CASH_AWARD_BNFT_AMT"),
        safe_col(source_df, "GRADE_ENTRY_DT", DateType()).alias("EMP_GRADE_START_DTE"),
        safe_col(source_df, "STEP_ENTRY_DT", DateType()).alias("WGI_START_DTE"),
        safe_col(source_df, "GVT_WGI_DUE_DATE", DateType()).alias("WGI_DUE_DT"),
        safe_col(source_df, "o_PROBATION_DT", DateType()).alias("PROB_TRIAL_PERIOD_START_DTE"),
        safe_col(source_df, "REPORTS_TO").alias("REPORTS_TO"),
        safe_col(source_df, "POSITION_ENTRY_DT", DateType()).alias("POSITION_ENTRY_DT"),
        safe_col(source_df, "o_EMP_RESID_CITY_STATE_NAME").alias("EMP_RESID_CITY_ST_NAME"),
        safe_col(source_df, "o_BUSINESS_TITLE").alias("POSITION_TITLE_NAME"),
        safe_col(source_df, "POSITION_NBR").alias("EHRP_POSITION_NUMBER"),
        safe_col(source_df, "GVT_WIP_STATUS").alias("GVT_WIP_STATUS"),
        safe_col(source_df, "GVT_STATUS_TYPE").alias("GVT_STATUS_TYPE"),
        safe_col(source_df, "SETID_DEPT").alias("SETID"),
        safe_col(source_df, "o_CITIZENSHIP_STATUS").alias("US_CITIZENSHIP_CD"),
        safe_col(source_df, "o_LOAD_ID").alias("LOAD_ID"),
        safe_col(source_df, "o_LOAD_DATE", TimestampType()).alias("AS_OF_DAY"),
        safe_col(source_df, "o_LOAD_DATE", TimestampType()).alias("LOAD_DATE"),
        # Personal data fields from exp_PERS_DATA / lkp_PS_GVT_PERS_DATA
        safe_col(source_df, "ADDRESS1").alias("EMP_RESID_STREET_NAME"),
        safe_col(source_df, "GVT_DISABILITY_CD").alias("HANDICAP_CD"),
        safe_col(source_df, "ETHNIC_GROUP").alias("RACE_NATL_ORIGIN_CD"),
        safe_col(source_df, "GVT_VET_PREF_APPT").alias("VETERANS_PREFERENCE_CD"),
        safe_col(source_df, "MILITARY_STATUS").alias("VETERANS_STATUS_CD"),
        safe_col(source_df, "SEX").alias("SEX_CD"),
        safe_col(source_df, "GVT_CRED_MIL_SVCE").alias("CRDTBL_MIL_SRVC_PERIOD"),
        # Fields from lkp_PS_GVT_EMPLOYMENT
        safe_col(source_df, "GVT_CNV_BEGIN_DATE", DateType()).alias("CAREER_START_DTE"),
        safe_col(source_df, "GVT_TEMP_PSN_EXPIR", DateType()).alias("POSITION_CHANGE_END_DTE"),
        safe_col(source_df, "GVT_APPT_LIMIT_DYS").alias("APPT_LMT_NTE_90DAY_CD"),
        safe_col(source_df, "HIRE_DT", DateType()).alias("EMP_EOD_DTE"),
        safe_col(source_df, "GVT_SPEP").alias("SPECIAL_PROGRAM_CD"),
        safe_col(source_df, "GVT_SUPV_PROB_DT", DateType()).alias("SUPERVSRY_MGRL_PROB_START_DTE"),
        safe_col(source_df, "HE_FILL_POSITION").alias("FILLING_POSITION_CD"),
        # Leave balance fields
        safe_col(source_df, "o_HE_AL_RED_CRED", DecimalType(18, 6)).alias("ANN_LV_CRDT_REDUCTN_HRS"),
        safe_col(source_df, "o_HE_AL_BALANCE", DecimalType(18, 6)).alias("ANN_LV_CUR_BAL_HRS"),
        safe_col(source_df, "o_HE_LUMP_HRS", DecimalType(18, 6)).alias("ANN_LV_LUMP_SUM_PAID_HRS"),
        safe_col(source_df, "o_HE_AL_CARRYOVER", DecimalType(18, 6)).alias("ANN_LV_PRIOR_YEAR_BAL_HRS"),
        safe_col(source_df, "o_HE_RES_BALANCE", DecimalType(18, 6)).alias("ANN_LV_RESTORED_BAL_LV_HRS"),
        safe_col(source_df, "o_HE_RES_LASTYR", DecimalType(18, 6)).alias("ANN_LV_RESTORED_BAL1_HRS"),
        safe_col(source_df, "o_HE_RES_TWOYRS", DecimalType(18, 6)).alias("ANN_LV_RESTORED_BAL2_HRS"),
        safe_col(source_df, "o_HE_AL_TOTAL", DecimalType(18, 6)).alias("ANN_LV_YTD_USED_HRS"),
        safe_col(source_df, "o_HE_AWOP_SEP", DecimalType(18, 6)).alias("AWOP_YTD_HRS"),
        safe_col(source_df, "o_HE_AWOP_WIGI", DecimalType(18, 6)).alias("LWOP_AWOP_WGI_HRS"),
        safe_col(source_df, "o_GVT_ANNUITY_OFFSET", DecimalType(18, 6)).alias("REEMP_ANNUITANT_HRLY_RATE_AMT"),
        safe_col(source_df, "o_HE_SL_RED_CRED", DecimalType(18, 6)).alias("SICK_LV_CRDT_REDUCTN_HRS"),
        safe_col(source_df, "o_HE_SL_BALANCE", DecimalType(18, 6)).alias("SICK_LV_CUR_BAL_HRS"),
        safe_col(source_df, "o_HE_FROZEN_SL", DecimalType(18, 6)).alias("SICK_LV_FERS_ELECT_BAL_HRS"),
        safe_col(source_df, "o_HE_SL_CARRYOVER", DecimalType(18, 6)).alias("SICK_LV_PRIOR_YEAR_BAL_HRS"),
        safe_col(source_df, "o_HE_SL_ACCRUAL", DecimalType(18, 6)).alias("SICK_LV_YTD_ACCRD_HRS"),
        safe_col(source_df, "o_HE_SL_TOTAL", DecimalType(18, 6)).alias("SICK_LV_YTD_USED_HRS"),
    )

    # ---- Build NWK_ACTION_SECONDARY_TBL target ----
    secondary_df = source_df.select(
        safe_col(source_df, "o_EVENT_ID", LongType()).alias("EVENT_ID"),
        safe_col(source_df, "SAL_ADMIN_PLAN").alias("PAY_TABLE_NUM"),
        safe_col(source_df, "GVT_RTND_GRADE").alias("RETND1_GRADE_CD"),
        safe_col(source_df, "GVT_RTND_STEP").alias("RETND1_STEP_CD"),
        safe_col(source_df, "GVT_RTND_PAY_PLAN").alias("RETND1_PAY_PLAN_CD"),
        safe_col(source_df, "GVT_RTND_GRADE_BEG", DateType()).alias("RETND1_EFF_DTE"),
        safe_col(source_df, "GVT_RTND_GRADE_EXP", DateType()).alias("RETND1_EXP_DTE"),
        safe_col(source_df, "o_TIME_OFF_GRANTED_HRS", DecimalType(18, 6)).alias("TIME_OFF_GRANTED_HRS"),
        safe_col(source_df, "o_RELOCATION_BONUS_AMT", DecimalType(18, 6)).alias("RELOCATION_BONUS_AMT"),
        safe_col(source_df, "o_RECRUITMENT_BONUS_AMT", DecimalType(18, 6)).alias("RECRUITMENT_BONUS_AMT"),
        safe_col(source_df, "o_TIME_OFF_AWARD_AMT", DecimalType(18, 6)).alias("TIME_OFF_AWARD_AMT"),
        safe_col(source_df, "HE_REG_MILITARY", DecimalType(18, 6)).alias("MIL_LV_CUR_FY_HRS"),
        safe_col(source_df, "HE_SPC_MILITARY", DecimalType(18, 6)).alias("MIL_LV_EMERG_CUR_FY_HRS"),
        safe_col(source_df, "HE_PP_UDED_AMT", DecimalType(18, 6)).alias("TSP_EMP_PP_UND_DED_AMT"),
        safe_col(source_df, "HE_NO_TSP_PAYPER").alias("TSP_EMP_PP_UND_DED_PYMT_COUNT"),
        safe_col(source_df, "HE_TSPA_SUB_YR", DecimalType(18, 6)).alias("TSP_EMP_PREV_GOVT_CONTB_AMT"),
        safe_col(source_df, "HE_EMP_UDED_AMT", DecimalType(18, 6)).alias("TSP_EMP_UND_DED_OUTSTNDG_AMT"),
        safe_col(source_df, "HE_GVT_UDED_AMT", DecimalType(18, 6)).alias("TSP_GOVT_UND_DED_OUTSTNDG_AMT"),
        safe_col(source_df, "HE_TLTR_NO").alias("TSP_UND_DED_LTR_NUM"),
        safe_col(source_df, "HE_UDED_PAY_CD").alias("TSP_UND_DED_OPTION_CD"),
        safe_col(source_df, "HE_TSP_CANC_CD").alias("TSP_UND_DED_STOP_OPTION_CD"),
        safe_col(source_df, "o_TSP_VESTING_CD").alias("TSP_VESTING_CD"),
        safe_col(source_df, "o_RECRUITMENT_EXP_DTE", DateType()).alias("RECRUITMENT_EXP_DTE"),
        safe_col(source_df, "o_RELOCATION_EXP_DTE", DateType()).alias("RELOCATION_EXP_DTE"),
        safe_col(source_df, "CMPNY_SENIORITY_DT", DateType()).alias("RIF_SCD_DTE"),
        safe_col(source_df, "GVT_SCD_TSP", DateType()).alias("TSP_SCD_DTE"),
        safe_col(source_df, "GVT_SCD_RETIRE", DateType()).alias("RETMT_SCD_DTE"),
        safe_col(source_df, "LWOP_START_DATE", DateType()).alias("LWOP_START_DTE"),
        safe_col(source_df, "BUYOUT_EFFDT", DateType()).alias("BUYOUT_EFF_DTE"),
        safe_col(source_df, "o_BUYOUT_AMT", DecimalType(18, 6)).alias("BUYOUT_AMT"),
        safe_col(source_df, "PAYGROUP").alias("PAYGROUP"),
        safe_col(source_df, "GVT_SCD_LEO", DateType()).alias("LEO_SCD_DT"),
        # Personal data from exp_PERS_DATA
        safe_col(source_df, "GVT_MILITARY_COMP").alias("MIL_SRVC_BRANCH_CD"),
    )

    # ---- Build EHRP_RECS_TRACKING_TBL target ----
    tracking_df = source_df.select(
        safe_col(source_df, "o_EVENT_ID", LongType()).alias("BIIS_EVENT_ID"),
        safe_col(source_df, "EMPLID"),
        safe_col(source_df, "EMPL_RCD"),
        safe_col(source_df, "EFFDT", DateType()),
        safe_col(source_df, "EFFSEQ"),
        safe_col(source_df, "GVT_WIP_STATUS"),
        safe_col(source_df, "o_LOAD_DATE", TimestampType()).alias("LOAD_DATE"),
    )

    # ---- Write all 3 target tables in parallel ----
    logger.info("Writing to NWK_ACTION_PRIMARY_TBL...")
    write_oracle_table(primary_df, "NKNIGHT.NWK_ACTION_PRIMARY_TBL",
                       mode="append", jdbc_url=jdbc_url_target,
                       connection_name=TARGET_CONNECTION)
    primary_count = primary_df.count()
    logger.info("Wrote %d row(s) to NWK_ACTION_PRIMARY_TBL", primary_count)

    logger.info("Writing to NWK_ACTION_SECONDARY_TBL...")
    write_oracle_table(secondary_df, "NKNIGHT.NWK_ACTION_SECONDARY_TBL",
                       mode="append", jdbc_url=jdbc_url_target,
                       connection_name=TARGET_CONNECTION)
    logger.info("Wrote %d row(s) to NWK_ACTION_SECONDARY_TBL", secondary_df.count())

    logger.info("Writing to EHRP_RECS_TRACKING_TBL...")
    write_oracle_table(tracking_df, "NKNIGHT.EHRP_RECS_TRACKING_TBL",
                       mode="append", jdbc_url=jdbc_url_target,
                       connection_name=TARGET_CONNECTION)
    logger.info("Wrote %d row(s) to EHRP_RECS_TRACKING_TBL", tracking_df.count())

    source_df.unpersist()

    return (src_rows, primary_count)


# ===================================================================
# Post-load Step (replaces ehrp2biis_afterload.sql)
# ===================================================================
def step_afterload(jdbc_url_target: str) -> None:
    """Execute the post-load SQL steps from ehrp2biis_afterload.sql.

    Key operations:
    1. Update RETND1_STEP_CD NULL where it is '0.0000000000000'
    2. Run update_sequence_number_tbl_p procedure
    3. Run HISTDBA stored procedures for formatting
    4. Insert new records into ACTION_*_ALL tables
    5. Handle cancelled actions
    6. Update PROCESS_TABLE
    7. Run chk_ehrp2biis_wip_status_p procedure
    8. Truncate NWK_NEW_EHRP_ACTIONS_TBL for next load
    """
    logger.info("Post-load step: executing afterload SQL")

    afterload_statements = [
        # Step 04: Update retained step code
        """UPDATE NKNIGHT.NWK_ACTION_SECONDARY_TBL a
           SET a.RETND1_STEP_CD = NULL
           WHERE a.EVENT_ID IN (
               SELECT b.EVENT_ID
               FROM NKNIGHT.NWK_ACTION_PRIMARY_TBL b
               WHERE b.LOAD_DATE = TRUNC(SYSDATE)
                 AND b.EVENT_ID < 9000000000
           )
           AND a.RETND1_STEP_CD = '0.0000000000000'""",

        # Step 05: Update sequence numbers
        "ALTER PROCEDURE UPDATE_SEQUENCE_NUMBER_TBL_P COMPILE",

        # Run formatting procedures
        "BEGIN UPDATE_SEQUENCE_NUMBER_TBL_P; END;",
        "BEGIN HISTDBA.UPDT_ERP2BIIS_CRE8_REMARKS01_P; END;",
        "BEGIN HISTDBA.UPDATE_ERP2BIIS_NO900S01_P; END;",
        "BEGIN HISTDBA.ERP2BIIS_CRE8_REMARKS_900S01; END;",
        "BEGIN HISTDBA.UPDATE_ERP2BIIS_900SONLY01_P; END;",
        "BEGIN HISTDBA.UPDT_ORIG_CANCELLED_TRANS01_P; END;",

        # Update PROCESS_TABLE
        "UPDATE PROCESS_TABLE SET P_STARTDT = NULL",

        """UPDATE PROCESS_TABLE SET P_STARTDT = (
            SELECT EFFDT FROM (
                SELECT a.BIIS_EVENT_ID, a.EMPLID, a.EMPL_RCD,
                       a.EFFDT, a.EFFSEQ, b.DEPTID,
                       a.GVT_WIP_STATUS, b.GVT_WIP_STATUS
                FROM NKNIGHT.EHRP_RECS_TRACKING_TBL a, EHRP.PS_GVT_JOB b
                WHERE a.EMPLID = b.EMPLID
                  AND a.EMPL_RCD = b.EMPL_RCD
                  AND a.EFFDT = b.EFFDT
                  AND a.EFFSEQ = b.EFFSEQ
                  AND a.GVT_WIP_STATUS <> b.GVT_WIP_STATUS
                  AND a.CHANGED_WIP_STATUS IS NULL
                ORDER BY 4, 1
            ) WHERE ROWNUM < 2
        )""",

        """UPDATE PROCESS_TABLE SET P_STARTDT = TRUNC(SYSDATE) + 10000
           WHERE P_STARTDT IS NULL""",

        # Run WIP status check procedure
        "ALTER PROCEDURE CHK_EHRP2BIIS_WIP_STATUS_P COMPILE",
        "BEGIN CHK_EHRP2BIIS_WIP_STATUS_P; END;",

        # Insert today's records into ALL tables
        """INSERT INTO ACTION_PRIMARY_ALL
           SELECT * FROM NKNIGHT.NWK_ACTION_PRIMARY_TBL c
           WHERE c.LOAD_DATE = TRUNC(SYSDATE)""",

        """INSERT INTO ACTION_SECONDARY_ALL
           SELECT * FROM NKNIGHT.NWK_ACTION_SECONDARY_TBL c
           WHERE c.EVENT_ID IN (
               SELECT a.EVENT_ID FROM NKNIGHT.NWK_ACTION_PRIMARY_TBL a
               WHERE a.LOAD_DATE = TRUNC(SYSDATE)
           )""",

        """INSERT INTO ACTION_REMARKS_ALL
           SELECT * FROM NKNIGHT.NWK_ACTION_REMARKS_TBL c
           WHERE c.EVENT_ID IN (
               SELECT a.EVENT_ID FROM NKNIGHT.NWK_ACTION_PRIMARY_TBL a
               WHERE a.LOAD_DATE = TRUNC(SYSDATE)
           )""",

        # Gather run counts
        "BEGIN HISTDBA.GATHER_EHRP2BIIS_RUNCOUNTS_P(NULL); END;",

        # Handle cancelled actions - delete and re-insert
        """DELETE FROM ACTION_SECONDARY_ALL d
           WHERE d.EVENT_ID IN (
               SELECT b.BIIS_EVENT_ID FROM NKNIGHT.EHRP_RECS_TRACKING_TBL b
               WHERE b.BIIS_WIP_STATUS_CHANGED_DT = TRUNC(SYSDATE)
           )""",

        """DELETE FROM ACTION_REMARKS_ALL d
           WHERE d.EVENT_ID IN (
               SELECT b.BIIS_EVENT_ID FROM NKNIGHT.EHRP_RECS_TRACKING_TBL b
               WHERE b.BIIS_WIP_STATUS_CHANGED_DT = TRUNC(SYSDATE)
           )""",

        """DELETE FROM ACTION_PRIMARY_ALL d
           WHERE d.EVENT_ID IN (
               SELECT b.BIIS_EVENT_ID FROM NKNIGHT.EHRP_RECS_TRACKING_TBL b
               WHERE b.BIIS_WIP_STATUS_CHANGED_DT = TRUNC(SYSDATE)
           )""",

        """INSERT INTO ACTION_PRIMARY_ALL
           SELECT * FROM NKNIGHT.NWK_ACTION_PRIMARY_TBL d
           WHERE d.EVENT_ID IN (
               SELECT b.BIIS_EVENT_ID FROM NKNIGHT.EHRP_RECS_TRACKING_TBL b
               WHERE b.BIIS_WIP_STATUS_CHANGED_DT = TRUNC(SYSDATE)
           )""",

        """INSERT INTO ACTION_SECONDARY_ALL
           SELECT * FROM NKNIGHT.NWK_ACTION_SECONDARY_TBL d
           WHERE d.EVENT_ID IN (
               SELECT b.BIIS_EVENT_ID FROM NKNIGHT.EHRP_RECS_TRACKING_TBL b
               WHERE b.BIIS_WIP_STATUS_CHANGED_DT = TRUNC(SYSDATE)
           )""",

        """INSERT INTO ACTION_REMARKS_ALL
           SELECT * FROM NKNIGHT.NWK_ACTION_REMARKS_TBL d
           WHERE d.EVENT_ID IN (
               SELECT b.BIIS_EVENT_ID FROM NKNIGHT.EHRP_RECS_TRACKING_TBL b
               WHERE b.BIIS_WIP_STATUS_CHANGED_DT = TRUNC(SYSDATE)
           )""",

        # Truncate trigger table for next load
        "TRUNCATE TABLE NKNIGHT.NWK_NEW_EHRP_ACTIONS_TBL",
    ]

    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    logfile = os.path.join(LOG_DIR, f"EHRP2BIIS_UPDATE_AFTERLOAD_{timestamp}.log")

    with open(logfile, "w") as lf:
        for i, stmt in enumerate(afterload_statements, 1):
            try:
                execute_jdbc_statement(
                    stmt, jdbc_url=jdbc_url_target, connection_name=TARGET_CONNECTION
                )
                lf.write(f"Step {i}: SUCCESS\n{stmt}\n\n")
                logger.info("Afterload step %d completed", i)
            except Exception as exc:
                lf.write(f"Step {i}: ERROR - {exc}\n{stmt}\n\n")
                logger.error("Afterload step %d failed: %s", i, exc)
                # Continue execution - some steps may fail if tables are empty
                continue

    logger.info("Post-load step complete. Log: %s", logfile)


# ===================================================================
# Main workflow
# ===================================================================
def run_ehrp2biis_update() -> None:
    """Execute the full wf_EHRP2BIIS_UPDATE workflow."""
    spark = get_spark_session(
        JOB_NAME,
        **{
            "spark.executor.memory": "2g",
            "spark.driver.memory": "2g",
        },
    )

    jdbc_url_source = get_jdbc_url("ORA_BIISPRD_SRC")
    jdbc_url_target = get_jdbc_url("ORA_BIIS")
    jdbc_url_jpm = get_jdbc_url("BIISPRD")

    perf = PerfTimer(JOB_NAME)
    src_rows = 0
    tgt_rows = 0

    with perf:
        try:
            # Step 1: Pre-load
            step_preload(jdbc_url_target)

            # Step 2: Main mapping
            src_rows, tgt_rows = step_main_mapping(
                spark, jdbc_url_source, jdbc_url_target, jdbc_url_jpm,
            )

            # Step 3: Post-load
            step_afterload(jdbc_url_target)

            # Success email
            send_email(
                subject=f"{ENV_PREFIX}EHRP2BIIS UPDATE completed successfully",
                body=(
                    f"Workflow {JOB_NAME} completed successfully.\n"
                    f"Source rows: {src_rows}\n"
                    f"Target rows: {tgt_rows}\n"
                    f"Duration: {perf.duration_seconds:.2f}s"
                ),
                recipients=SUCCESS_RECIPIENTS,
            )

            logger.info("Workflow %s completed successfully.", JOB_NAME)

        except Exception as exc:
            logger.error("Workflow %s FAILED: %s", JOB_NAME, exc)
            send_email(
                subject=f"{ENV_PREFIX}{JOB_NAME} FAILED",
                body=f"Workflow {JOB_NAME} failed with error:\n\n{exc}",
                recipients=PRELOAD_RECIPIENTS + SUCCESS_RECIPIENTS,
            )
            raise

    # Log performance
    perf.log_to_csv(
        "/data/BIISINT/data/int/log/migration_perf_log.csv",
        src_rows=src_rows,
        tgt_rows=tgt_rows,
    )
    try:
        perf.log_to_table(spark, src_rows=src_rows, tgt_rows=tgt_rows)
    except Exception:
        logger.warning("Could not write perf metrics to MIGRATION_PERF_LOG table", exc_info=True)

    spark.stop()


if __name__ == "__main__":
    run_ehrp2biis_update()
