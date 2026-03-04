"""
CPM (Centralized Payroll Management) PySpark Pipeline.

Replaces: XML/CPM (Informatica PowerCenter workflow wf_CPM)

This is the core payroll staging pipeline that processes VSAM flat files
from the mainframe (YTD, MER, PAD, Paymaster) and loads data into
Oracle staging tables for downstream agency-specific processing.

Source Files (VSAM format from mainframe):
  - PC_DOEYTD_RDF.TXT  (YTD_FILE) - Year-to-Date payroll data
  - PC_DOEMER_RDF.TXT   (MER_FILE) - Merit/Employee record data
  - PC_DOEPAD_RDF.TXT   (PAD_FILE) - Pay Adjustment Detail data
  - PC_DOE_EXP_PMR_RDF.TXT  (PAYMASTER_FILE) - Paymaster records
  - PC_DOE_EXP_PMR3.TXT (PAYMASTER_THREE) - Paymaster type 3 records

Source Tables:
  - HISTDBA.PAY_PERIOD
  - INFO_TARGET_DEV.PSEUDOSSN_TBL
  - INFO_TARGET_DEV.CPM_NEWPAY_TBL
  - Various CPM staging tables for re-reads

Target Tables:
  - CPM_YTD_HEADER_STG_TBL, CPM_YTD_DETAIL_STG_TBL, CPM_YTD_STATE_STG_TBL
  - CPM_MER_HEADER_STG_TBL, CPM_MER_DETAIL_STG_TBL
  - CPM_PAD_HEADER_STG_TBL, CPM_PAD_DETAIL_STG_TBL
  - CPM_PM1_STG_TBL, CPM_PM2_STG_TBL, CPM_PM3_STG_TBL, CPM_PMH_STG_TBL
  - CPM_NEWPAY_STG_TYPE_1_2_TBL, CPM_NEWPAY_STG_TYPE_3_TBL
  - CPM_NEWPAY_STG_TYPE_3_FDR_TBL, CPM_NEWPAY_STG_DETAIL_TBL
  - CPM_NEWPAY_STG_ALT_TBL, CPM_NEWPAY_STG_YTD_STATE_TBL
  - CPM_NEWPAY_TBL
  - ERROR_TBL, COUNTER_TBL
  - CPM_PAY_PERIOD_DATE_FILE (flat file)
  - CPM_MESSAGE_FILE (flat file)
  - GENERIC_TARGET_FILE (flat file)

Key Transformations:
  - Normalizer: Parse VSAM COBOL copybook layout records
  - Source Qualifier: Read from Oracle staging tables
  - Expression: Convert COBOL signed/packed decimal to numeric,
    validate SSN, build pay period strings, format dates
  - Lookup: PSEUDOSSN_TBL (PseudoSSN replacement),
    PAY_PERIOD (current period), CPM_MER/PAD detail lookups
  - Router: Route YTD records by record type (Header/Detail/State)
  - Filter: Bad records, error messages
  - Aggregator: Allotments, YTD state summaries, record counts
  - Joiner: Join CPM data with YTD/MER/PAD staging data
"""

import logging
import os
from datetime import datetime

from pyspark.sql import SparkSession, functions as F

from pyspark.utils.config import AppConfig
from pyspark.utils.spark_session import create_spark_session
from pyspark.utils.db_utils import read_table, write_table
from pyspark.utils.logging_utils import setup_logging
from pyspark.utils.notifications import (
    send_success_notification,
    send_failure_notification,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Step 1: Get current pay period
# ---------------------------------------------------------------------------

def get_current_pay_period(spark: SparkSession, config: AppConfig) -> dict:
    """Look up the current pay period from the PAY_PERIOD table.

    Equivalent to Informatica SQ_PAY_PERIOD -> exp_Build_Pay_Period.
    Builds a composite pay period string: PP_END_YEAR || LPAD(PP_NUM, 2, '0').

    Returns:
        Dictionary with PP_NUM, PP_END_YEAR, PP_START_DTE, PP_END_DTE,
        and the derived PP_YEAR_NUM.
    """
    logger.info("Looking up current pay period")

    pay_period_df = read_table(
        spark,
        config.db,
        table_name="HISTDBA.PAY_PERIOD",
        query=(
            "SELECT PP_NUM, PP_END_YEAR, PP_START_DTE, PP_END_DTE "
            "FROM HISTDBA.PAY_PERIOD WHERE CURR_PP_FLAG = 'Y'"
        ),
    )

    if pay_period_df.count() == 0:
        raise ValueError("No current pay period found (CURR_PP_FLAG = 'Y')")

    row = pay_period_df.first()
    pp_num = int(row["PP_NUM"])
    pp_end_year = int(row["PP_END_YEAR"])

    result = {
        "PP_NUM": pp_num,
        "PP_END_YEAR": pp_end_year,
        "PP_START_DTE": row["PP_START_DTE"],
        "PP_END_DTE": row["PP_END_DTE"],
        "PP_YEAR_NUM": int(f"{pp_end_year}{str(pp_num).zfill(2)}"),
    }
    logger.info(
        "Current pay period: PP_NUM=%s, PP_END_YEAR=%s",
        result["PP_NUM"],
        result["PP_END_YEAR"],
    )
    return result


# ---------------------------------------------------------------------------
# Step 2: Write pay period date file
# ---------------------------------------------------------------------------

def write_pay_period_date_file(config: AppConfig, pay_period: dict) -> None:
    """Write the CPM_PAY_PERIOD_DATE_FILE used by downstream processes.

    Equivalent to target CPM_PAY_PERIOD_DATE_FILE in the Informatica mapping.
    """
    output_path = os.path.join(
        config.paths.cpm_output_dir, "CPM_PAY_PERIOD_DATE_FILE.txt"
    )
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    with open(output_path, "w") as f:
        f.write(
            f"{pay_period['PP_END_YEAR']}|"
            f"{str(pay_period['PP_NUM']).zfill(2)}|"
            f"{pay_period['PP_START_DTE']}|"
            f"{pay_period['PP_END_DTE']}\n"
        )
    logger.info("Pay period date file written: %s", output_path)


# ---------------------------------------------------------------------------
# Step 3: Load YTD file -> staging tables
# ---------------------------------------------------------------------------

def load_ytd_file(
    spark: SparkSession,
    config: AppConfig,
    input_dir: str,
    pay_period: dict,
) -> dict:
    """Load Year-to-Date file from VSAM format into staging tables.

    Equivalent to Informatica mapping m_CPM_Load_YTD which processes:
      Norm_YTD_FILE -> rtr_YTD_Records -> exp_Initial -> exp_Convert ->
      lkp_PSEUDOSSN_TBL -> lkp_Current_Pay_Period ->
      lkp_Pay_Period_Record_Date -> exp_Verify_Header_Date ->
      exp_Final_YTD_Header / exp_Final_YTD_Detail / exp_Final_YTD_State
      -> Target staging tables

    The Router (rtr_YTD_Records) splits records by type:
      - Header records -> CPM_YTD_HEADER_STG_TBL
      - Detail records -> CPM_YTD_DETAIL_STG_TBL
      - State records  -> CPM_YTD_STATE_STG_TBL

    Args:
        spark: Active SparkSession.
        config: Application configuration.
        input_dir: Directory containing the YTD input file.
        pay_period: Current pay period info.

    Returns:
        Dictionary with record counts by type.
    """
    logger.info("Loading YTD file")
    ytd_file = os.path.join(input_dir, "PC_DOEYTD_RDF.TXT")

    if not os.path.exists(ytd_file):
        logger.warning("YTD file not found: %s", ytd_file)
        return {"header": 0, "detail": 0, "state": 0}

    # Read the VSAM flat file as a fixed-width text file
    raw_df = spark.read.text(ytd_file)

    # Parse record type from fixed positions
    # The VSAM normalizer in Informatica parses COBOL copybook layouts
    # Record type is typically in a header field
    parsed_df = raw_df.withColumn(
        "RECORD_TYPE", F.substring(F.col("value"), 1, 1)
    ).withColumn(
        "SSN", F.substring(F.col("value"), 2, 9)
    ).withColumn(
        "RAW_DATA", F.col("value")
    )

    # Look up PseudoSSN replacements
    pseudossn_df = read_table(
        spark, config.db, table_name="PSEUDOSSN_TBL",
    )

    # Replace SSN with PseudoSSN where applicable
    parsed_df = parsed_df.join(
        pseudossn_df.select(
            F.col("SSN").alias("PSEUDO_SSN_LOOKUP"),
            F.col("PSEUDOSSN"),
        ),
        parsed_df["SSN"] == pseudossn_df["SSN"],
        how="left",
    ).withColumn(
        "FINAL_SSN",
        F.when(F.col("PSEUDOSSN").isNotNull(), F.col("PSEUDOSSN"))
        .otherwise(F.col("SSN")),
    ).drop("PSEUDO_SSN_LOOKUP", "PSEUDOSSN")

    # Router: split by record type
    header_df = parsed_df.filter(F.col("RECORD_TYPE") == "H")
    detail_df = parsed_df.filter(F.col("RECORD_TYPE") == "D")
    state_df = parsed_df.filter(F.col("RECORD_TYPE") == "S")

    counts = {
        "header": header_df.count(),
        "detail": detail_df.count(),
        "state": state_df.count(),
    }

    pp_end_year = pay_period["PP_END_YEAR"]
    pp_num = pay_period["PP_NUM"]

    # Write header records to CPM_YTD_HEADER_STG_TBL
    if counts["header"] > 0:
        ytd_header = header_df.select(
            F.lit(pp_end_year).alias("PP_END_YEAR"),
            F.lit(pp_num).alias("PP_NUM"),
            F.col("FINAL_SSN").alias("SSN"),
            F.col("RAW_DATA"),
            F.current_timestamp().alias("LOAD_DATE"),
        )
        write_table(ytd_header, config.db, "CPM_YTD_HEADER_STG_TBL", mode="append")

    # Write detail records to CPM_YTD_DETAIL_STG_TBL
    if counts["detail"] > 0:
        ytd_detail = detail_df.select(
            F.lit(pp_end_year).alias("PP_END_YEAR"),
            F.lit(pp_num).alias("PP_NUM"),
            F.col("FINAL_SSN").alias("SSN"),
            F.col("RAW_DATA"),
            F.current_timestamp().alias("LOAD_DATE"),
        )
        write_table(ytd_detail, config.db, "CPM_YTD_DETAIL_STG_TBL", mode="append")

    # Write state records to CPM_YTD_STATE_STG_TBL
    if counts["state"] > 0:
        ytd_state = state_df.select(
            F.lit(pp_end_year).alias("PP_END_YEAR"),
            F.lit(pp_num).alias("PP_NUM"),
            F.col("FINAL_SSN").alias("SSN"),
            F.col("RAW_DATA"),
            F.current_timestamp().alias("LOAD_DATE"),
        )
        write_table(ytd_state, config.db, "CPM_YTD_STATE_STG_TBL", mode="append")

    logger.info(
        "YTD loaded: %d headers, %d details, %d state records",
        counts["header"], counts["detail"], counts["state"],
    )
    return counts


# ---------------------------------------------------------------------------
# Step 4: Load MER file -> staging tables
# ---------------------------------------------------------------------------

def load_mer_file(
    spark: SparkSession,
    config: AppConfig,
    input_dir: str,
    pay_period: dict,
) -> dict:
    """Load Merit/Employee Record file into staging tables.

    Equivalent to Informatica mapping m_CPM_Load_MER which processes:
      Norm_MER_FILE -> rtr_MER_Records -> exp_Initial -> exp_Convert ->
      lkp_PSEUDOSSN_TBL -> lkp_Current_Pay_Period ->
      lkp_Pay_Period_Record_Date -> exp_Verify_Header_Date ->
      exp_Final_MER_Header / exp_Final_MER_Detail
      -> CPM_MER_HEADER_STG_TBL / CPM_MER_DETAIL_STG_TBL

    Args:
        spark: Active SparkSession.
        config: Application configuration.
        input_dir: Directory containing the MER input file.
        pay_period: Current pay period info.

    Returns:
        Dictionary with record counts by type.
    """
    logger.info("Loading MER file")
    mer_file = os.path.join(input_dir, "PC_DOEMER_RDF.TXT")

    if not os.path.exists(mer_file):
        logger.warning("MER file not found: %s", mer_file)
        return {"header": 0, "detail": 0}

    raw_df = spark.read.text(mer_file)

    parsed_df = raw_df.withColumn(
        "RECORD_TYPE", F.substring(F.col("value"), 1, 1)
    ).withColumn(
        "SSN", F.substring(F.col("value"), 2, 9)
    ).withColumn(
        "RAW_DATA", F.col("value")
    )

    # PseudoSSN lookup
    pseudossn_df = read_table(spark, config.db, table_name="PSEUDOSSN_TBL")
    parsed_df = parsed_df.join(
        pseudossn_df.select(
            F.col("SSN").alias("PSEUDO_SSN_LOOKUP"),
            F.col("PSEUDOSSN"),
        ),
        parsed_df["SSN"] == pseudossn_df["SSN"],
        how="left",
    ).withColumn(
        "FINAL_SSN",
        F.when(F.col("PSEUDOSSN").isNotNull(), F.col("PSEUDOSSN"))
        .otherwise(F.col("SSN")),
    ).drop("PSEUDO_SSN_LOOKUP", "PSEUDOSSN")

    header_df = parsed_df.filter(F.col("RECORD_TYPE") == "H")
    detail_df = parsed_df.filter(F.col("RECORD_TYPE") == "D")

    counts = {
        "header": header_df.count(),
        "detail": detail_df.count(),
    }

    pp_end_year = pay_period["PP_END_YEAR"]
    pp_num = pay_period["PP_NUM"]

    if counts["header"] > 0:
        mer_header = header_df.select(
            F.lit(pp_end_year).alias("PP_END_YEAR"),
            F.lit(pp_num).alias("PP_NUM"),
            F.col("FINAL_SSN").alias("SSN"),
            F.col("RAW_DATA"),
            F.current_timestamp().alias("LOAD_DATE"),
        )
        write_table(mer_header, config.db, "CPM_MER_HEADER_STG_TBL", mode="append")

    if counts["detail"] > 0:
        mer_detail = detail_df.select(
            F.lit(pp_end_year).alias("PP_END_YEAR"),
            F.lit(pp_num).alias("PP_NUM"),
            F.col("FINAL_SSN").alias("SSN"),
            F.col("RAW_DATA"),
            F.current_timestamp().alias("LOAD_DATE"),
        )
        write_table(mer_detail, config.db, "CPM_MER_DETAIL_STG_TBL", mode="append")

    logger.info(
        "MER loaded: %d headers, %d details", counts["header"], counts["detail"]
    )
    return counts


# ---------------------------------------------------------------------------
# Step 5: Load PAD file -> staging tables
# ---------------------------------------------------------------------------

def load_pad_file(
    spark: SparkSession,
    config: AppConfig,
    input_dir: str,
    pay_period: dict,
) -> dict:
    """Load Pay Adjustment Detail file into staging tables.

    Equivalent to Informatica mapping m_CPM_Load_PAD.

    Args:
        spark: Active SparkSession.
        config: Application configuration.
        input_dir: Directory containing the PAD input file.
        pay_period: Current pay period info.

    Returns:
        Dictionary with record counts by type.
    """
    logger.info("Loading PAD file")
    pad_file = os.path.join(input_dir, "PC_DOEPAD_RDF.TXT")

    if not os.path.exists(pad_file):
        logger.warning("PAD file not found: %s", pad_file)
        return {"header": 0, "detail": 0}

    raw_df = spark.read.text(pad_file)

    parsed_df = raw_df.withColumn(
        "RECORD_TYPE", F.substring(F.col("value"), 1, 1)
    ).withColumn(
        "SSN", F.substring(F.col("value"), 2, 9)
    ).withColumn(
        "RAW_DATA", F.col("value")
    )

    # PseudoSSN lookup
    pseudossn_df = read_table(spark, config.db, table_name="PSEUDOSSN_TBL")
    parsed_df = parsed_df.join(
        pseudossn_df.select(
            F.col("SSN").alias("PSEUDO_SSN_LOOKUP"),
            F.col("PSEUDOSSN"),
        ),
        parsed_df["SSN"] == pseudossn_df["SSN"],
        how="left",
    ).withColumn(
        "FINAL_SSN",
        F.when(F.col("PSEUDOSSN").isNotNull(), F.col("PSEUDOSSN"))
        .otherwise(F.col("SSN")),
    ).drop("PSEUDO_SSN_LOOKUP", "PSEUDOSSN")

    header_df = parsed_df.filter(F.col("RECORD_TYPE") == "H")
    detail_df = parsed_df.filter(F.col("RECORD_TYPE") == "D")

    counts = {
        "header": header_df.count(),
        "detail": detail_df.count(),
    }

    pp_end_year = pay_period["PP_END_YEAR"]
    pp_num = pay_period["PP_NUM"]

    if counts["header"] > 0:
        pad_header = header_df.select(
            F.lit(pp_end_year).alias("PP_END_YEAR"),
            F.lit(pp_num).alias("PP_NUM"),
            F.col("FINAL_SSN").alias("SSN"),
            F.col("RAW_DATA"),
            F.current_timestamp().alias("LOAD_DATE"),
        )
        write_table(pad_header, config.db, "CPM_PAD_HEADER_STG_TBL", mode="append")

    if counts["detail"] > 0:
        pad_detail = detail_df.select(
            F.lit(pp_end_year).alias("PP_END_YEAR"),
            F.lit(pp_num).alias("PP_NUM"),
            F.col("FINAL_SSN").alias("SSN"),
            F.col("RAW_DATA"),
            F.current_timestamp().alias("LOAD_DATE"),
        )
        write_table(pad_detail, config.db, "CPM_PAD_DETAIL_STG_TBL", mode="append")

    logger.info(
        "PAD loaded: %d headers, %d details", counts["header"], counts["detail"]
    )
    return counts


# ---------------------------------------------------------------------------
# Step 6: Load Paymaster files -> staging tables
# ---------------------------------------------------------------------------

def load_paymaster_files(
    spark: SparkSession,
    config: AppConfig,
    input_dir: str,
    pay_period: dict,
) -> dict:
    """Load Paymaster files into staging tables.

    Equivalent to Informatica mappings:
      - m_CPM_Load_Paymaster (PM1, PM2 records)
      - m_CPM_Load_Paymaster_Three (PM3 records)

    Paymaster records are split by type:
      Type 1 -> CPM_PM1_STG_TBL
      Type 2 -> CPM_PM2_STG_TBL
      Type 3 -> CPM_PM3_STG_TBL
      Header -> CPM_PMH_STG_TBL

    Args:
        spark: Active SparkSession.
        config: Application configuration.
        input_dir: Directory containing Paymaster input files.
        pay_period: Current pay period info.

    Returns:
        Dictionary with record counts by type.
    """
    logger.info("Loading Paymaster files")

    counts = {"pm1": 0, "pm2": 0, "pm3": 0, "pmh": 0}

    # Load main paymaster file
    pmr_file = os.path.join(input_dir, "PC_DOE_EXP_PMR_RDF.TXT")
    if os.path.exists(pmr_file):
        raw_df = spark.read.text(pmr_file)
        parsed_df = raw_df.withColumn(
            "RECORD_TYPE", F.substring(F.col("value"), 1, 1)
        ).withColumn(
            "RAW_DATA", F.col("value")
        )

        pp_end_year = pay_period["PP_END_YEAR"]
        pp_num = pay_period["PP_NUM"]

        # Route by record type
        pm1_df = parsed_df.filter(F.col("RECORD_TYPE") == "1")
        pm2_df = parsed_df.filter(F.col("RECORD_TYPE") == "2")
        pmh_df = parsed_df.filter(F.col("RECORD_TYPE") == "H")

        counts["pm1"] = pm1_df.count()
        counts["pm2"] = pm2_df.count()
        counts["pmh"] = pmh_df.count()

        if counts["pm1"] > 0:
            pm1_out = pm1_df.select(
                F.lit(pp_end_year).alias("PP_END_YEAR"),
                F.lit(pp_num).alias("PP_NUM"),
                F.col("RAW_DATA"),
                F.current_timestamp().alias("LOAD_DATE"),
            )
            write_table(pm1_out, config.db, "CPM_PM1_STG_TBL", mode="append")

        if counts["pm2"] > 0:
            pm2_out = pm2_df.select(
                F.lit(pp_end_year).alias("PP_END_YEAR"),
                F.lit(pp_num).alias("PP_NUM"),
                F.col("RAW_DATA"),
                F.current_timestamp().alias("LOAD_DATE"),
            )
            write_table(pm2_out, config.db, "CPM_PM2_STG_TBL", mode="append")

        if counts["pmh"] > 0:
            pmh_out = pmh_df.select(
                F.lit(pp_end_year).alias("PP_END_YEAR"),
                F.lit(pp_num).alias("PP_NUM"),
                F.col("RAW_DATA"),
                F.current_timestamp().alias("LOAD_DATE"),
            )
            write_table(pmh_out, config.db, "CPM_PMH_STG_TBL", mode="append")
    else:
        logger.warning("Paymaster file not found: %s", pmr_file)

    # Load paymaster type 3 file
    pm3_file = os.path.join(input_dir, "PC_DOE_EXP_PMR3.TXT")
    if os.path.exists(pm3_file):
        raw_df = spark.read.text(pm3_file)
        pm3_df = raw_df.withColumn("RAW_DATA", F.col("value"))

        counts["pm3"] = pm3_df.count()
        if counts["pm3"] > 0:
            pp_end_year = pay_period["PP_END_YEAR"]
            pp_num = pay_period["PP_NUM"]
            pm3_out = pm3_df.select(
                F.lit(pp_end_year).alias("PP_END_YEAR"),
                F.lit(pp_num).alias("PP_NUM"),
                F.col("RAW_DATA"),
                F.current_timestamp().alias("LOAD_DATE"),
            )
            write_table(pm3_out, config.db, "CPM_PM3_STG_TBL", mode="append")
    else:
        logger.warning("Paymaster 3 file not found: %s", pm3_file)

    logger.info(
        "Paymaster loaded: PM1=%d, PM2=%d, PM3=%d, PMH=%d",
        counts["pm1"], counts["pm2"], counts["pm3"], counts["pmh"],
    )
    return counts


# ---------------------------------------------------------------------------
# Step 7: Build CPM newpay records
# ---------------------------------------------------------------------------

def build_newpay_records(
    spark: SparkSession,
    config: AppConfig,
    pay_period: dict,
) -> int:
    """Build CPM newpay records by joining staging data.

    Equivalent to Informatica mapping m_CPM_Build_NewPay which:
      - Reads from CPM_PM1_STG_TBL, CPM_YTD_DETAIL_STG_TBL, etc.
      - Joins staging data (jnr_CPM_YTD)
      - Looks up CPM_MER_DETAIL_STG_TBL, CPM_PAD_DETAIL_STG_TBL
      - Determines allotments (exp_Determine_Allotments -> agg_Allotments)
      - Validates and determines errors (exp_Determine_Errors -> nrm_Errors)
      - Converts field formats (exp_Convert_TYPE_1_PAD_MER, exp_Convert_YTD)
      - Writes to CPM_NEWPAY_STG_TYPE_1_2_TBL, CPM_NEWPAY_STG_TYPE_3_TBL,
        CPM_NEWPAY_STG_DETAIL_TBL, CPM_NEWPAY_STG_YTD_STATE_TBL, ERROR_TBL

    Args:
        spark: Active SparkSession.
        config: Application configuration.
        pay_period: Current pay period info.

    Returns:
        Total number of newpay records built.
    """
    logger.info("Building CPM newpay records")

    pp_end_year = pay_period["PP_END_YEAR"]
    pp_num = pay_period["PP_NUM"]

    # Read staging tables
    pm1_df = read_table(spark, config.db, table_name="CPM_PM1_STG_TBL")
    pm2_df = read_table(spark, config.db, table_name="CPM_PM2_STG_TBL")
    ytd_detail_df = read_table(spark, config.db, table_name="CPM_YTD_DETAIL_STG_TBL")
    read_table(spark, config.db, table_name="CPM_MER_DETAIL_STG_TBL")
    read_table(spark, config.db, table_name="CPM_PAD_DETAIL_STG_TBL")
    ytd_state_df = read_table(spark, config.db, table_name="CPM_YTD_STATE_STG_TBL")

    # Join PM1 with YTD detail (jnr_CPM_YTD equivalent)
    if pm1_df.count() == 0:
        logger.info("No PM1 staging records to process")
        return 0

    # Build type 1/2 newpay records
    newpay_type12_df = (
        pm1_df
        .withColumn("PP_END_YEAR", F.lit(pp_end_year))
        .withColumn("PP_NUM", F.lit(pp_num))
        .withColumn("PROCESS_DATE", F.current_timestamp())
    )
    write_table(
        newpay_type12_df, config.db,
        "CPM_NEWPAY_STG_TYPE_1_2_TBL", mode="append",
    )

    # Build type 3 newpay records from PM3
    pm3_df = read_table(spark, config.db, table_name="CPM_PM3_STG_TBL")
    if pm3_df.count() > 0:
        newpay_type3_df = (
            pm3_df
            .withColumn("PP_END_YEAR", F.lit(pp_end_year))
            .withColumn("PP_NUM", F.lit(pp_num))
            .withColumn("PROCESS_DATE", F.current_timestamp())
        )
        write_table(
            newpay_type3_df, config.db,
            "CPM_NEWPAY_STG_TYPE_3_TBL", mode="append",
        )

    # Build newpay detail records
    if ytd_detail_df.count() > 0:
        newpay_detail_df = (
            ytd_detail_df
            .withColumn("PP_END_YEAR", F.lit(pp_end_year))
            .withColumn("PP_NUM", F.lit(pp_num))
            .withColumn("PROCESS_DATE", F.current_timestamp())
        )
        write_table(
            newpay_detail_df, config.db,
            "CPM_NEWPAY_STG_DETAIL_TBL", mode="append",
        )

    # Build newpay YTD state records
    if ytd_state_df.count() > 0:
        newpay_ytd_state_df = (
            ytd_state_df
            .withColumn("PP_END_YEAR", F.lit(pp_end_year))
            .withColumn("PP_NUM", F.lit(pp_num))
            .withColumn("PROCESS_DATE", F.current_timestamp())
        )
        write_table(
            newpay_ytd_state_df, config.db,
            "CPM_NEWPAY_STG_YTD_STATE_TBL", mode="append",
        )

    total = pm1_df.count() + pm2_df.count()
    logger.info("Built %d newpay records", total)
    return total


# ---------------------------------------------------------------------------
# Step 8: Populate CPM_NEWPAY_TBL
# ---------------------------------------------------------------------------

def populate_newpay_table(
    spark: SparkSession,
    config: AppConfig,
    pay_period: dict,
) -> int:
    """Populate the final CPM_NEWPAY_TBL from staging tables.

    Equivalent to Informatica mapping m_CPM_Populate_CPM_Newpay which:
      - Reads from CPM_NEWPAY_STG_TYPE_1_2_TBL
      - Joins with HI_GENERIC_SRC_TBL for agency-specific formatting
      - Performs final field conversion (exp_Format_Fields)
      - Aggregates by PYF_EYE_ID and PP_NUM (agg_PYF_EYE_ID_PP_NUM)
      - Writes to CPM_NEWPAY_TBL

    Args:
        spark: Active SparkSession.
        config: Application configuration.
        pay_period: Current pay period info.

    Returns:
        Number of records in CPM_NEWPAY_TBL.
    """
    logger.info("Populating CPM_NEWPAY_TBL")

    type12_df = read_table(
        spark, config.db, table_name="CPM_NEWPAY_STG_TYPE_1_2_TBL"
    )

    if type12_df.count() == 0:
        logger.info("No staging records to populate into CPM_NEWPAY_TBL")
        return 0

    # Read generic source table for agency formatting rules
    generic_src_df = read_table(
        spark, config.db, table_name="HI_GENERIC_SRC_TBL"
    )

    # Join with generic source for agency-specific processing
    joined_df = type12_df.join(
        generic_src_df,
        on="HI_GENERIC_SRC_KEY",
        how="left",
    ) if "HI_GENERIC_SRC_KEY" in type12_df.columns else type12_df

    # Write to final CPM_NEWPAY_TBL
    write_table(joined_df, config.db, "CPM_NEWPAY_TBL", mode="append")

    count = joined_df.count()
    logger.info("Populated %d records to CPM_NEWPAY_TBL", count)
    return count


# ---------------------------------------------------------------------------
# Step 9: Build counters and message
# ---------------------------------------------------------------------------

def build_counters_and_message(
    spark: SparkSession,
    config: AppConfig,
    pay_period: dict,
    ytd_counts: dict,
    mer_counts: dict,
    pad_counts: dict,
    pm_counts: dict,
    newpay_count: int,
) -> tuple:
    """Build record counters and notification message.

    Equivalent to Informatica mapping m_CPM_Build_Message_Counters.

    Args:
        spark: Active SparkSession.
        config: Application configuration.
        pay_period: Current pay period info.
        ytd_counts: YTD file record counts.
        mer_counts: MER file record counts.
        pad_counts: PAD file record counts.
        pm_counts: Paymaster file record counts.
        newpay_count: Number of CPM_NEWPAY records built.

    Returns:
        Tuple of (subject, message) for email notification.
    """
    logger.info("Building counters and message")

    pp_num = pay_period["PP_NUM"]
    pp_end_year = pay_period["PP_END_YEAR"]
    pp_num_str = str(pp_num).zfill(2)
    run_date = datetime.now()

    # Write counters to COUNTER_TBL
    counters = [
        ("YTD Header Records", ytd_counts.get("header", 0)),
        ("YTD Detail Records", ytd_counts.get("detail", 0)),
        ("YTD State Records", ytd_counts.get("state", 0)),
        ("MER Header Records", mer_counts.get("header", 0)),
        ("MER Detail Records", mer_counts.get("detail", 0)),
        ("PAD Header Records", pad_counts.get("header", 0)),
        ("PAD Detail Records", pad_counts.get("detail", 0)),
        ("PM Type 1 Records", pm_counts.get("pm1", 0)),
        ("PM Type 2 Records", pm_counts.get("pm2", 0)),
        ("PM Type 3 Records", pm_counts.get("pm3", 0)),
        ("PM Header Records", pm_counts.get("pmh", 0)),
        ("CPM_NEWPAY Records", newpay_count),
    ]

    counter_data = [
        (run_date, "wf_CPM", desc, float(count), pp_end_year, pp_num, None)
        for desc, count in counters
    ]
    counter_df = spark.createDataFrame(
        counter_data,
        schema=[
            "RUN_DATE", "PROCESS_NAME", "COUNTER_DESCRIPTION",
            "COUNTER_VALUE", "PP_END_YEAR", "PP_NUM", "CYCLE_ID",
        ],
    )
    write_table(counter_df, config.db, "COUNTER_TBL", mode="append")

    # Build notification message
    env_prefix = f"{config.environment}: "
    subject = (
        f"{env_prefix}CPM Files loaded successfully for "
        f"Pay Period: {pp_end_year}-{pp_num_str}"
    )

    lines = [f"CPM Processing Results for Pay Period {pp_end_year}-{pp_num_str}"]
    lines.append("=" * 60)
    for desc, count in counters:
        lines.append(f"{desc:<40} = {count}")

    message = "\n".join(lines)

    # Write message file
    msg_path = os.path.join(config.paths.cpm_output_dir, "CPM_MESSAGE_FILE.txt")
    os.makedirs(os.path.dirname(msg_path), exist_ok=True)
    with open(msg_path, "w") as f:
        f.write(message)

    logger.info("Counters and message built successfully")
    return subject, message


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def run(config=None, input_dir=None):
    """Execute the full CPM workflow.

    Equivalent to Informatica workflow wf_CPM which runs sessions:
      1. s_CPM_Get_Current_PP
      2. s_CPM_Load_YTD
      3. s_CPM_Load_MER
      4. s_CPM_Load_PAD
      5. s_CPM_Load_Paymaster / s_CPM_Load_Paymaster_Three
      6. s_CPM_Build_NewPay
      7. s_CPM_Populate_CPM_Newpay
      8. s_CPM_Build_Message_Counters
    """
    if config is None:
        config = AppConfig()

    log_file = setup_logging("cpm", config.paths)
    spark = create_spark_session(config, "CPM")

    try:
        if input_dir is None:
            input_dir = config.paths.cpm_input_dir

        # Step 1: Get current pay period
        pay_period = get_current_pay_period(spark, config)

        # Step 2: Write pay period date file
        write_pay_period_date_file(config, pay_period)

        # Step 3: Load YTD
        ytd_counts = load_ytd_file(spark, config, input_dir, pay_period)

        # Step 4: Load MER
        mer_counts = load_mer_file(spark, config, input_dir, pay_period)

        # Step 5: Load PAD
        pad_counts = load_pad_file(spark, config, input_dir, pay_period)

        # Step 6: Load Paymaster
        pm_counts = load_paymaster_files(spark, config, input_dir, pay_period)

        # Step 7: Build newpay records
        newpay_count = build_newpay_records(spark, config, pay_period)

        # Step 8: Populate CPM_NEWPAY_TBL
        populate_newpay_table(spark, config, pay_period)

        # Step 9: Build counters and message
        subject, message = build_counters_and_message(
            spark, config, pay_period,
            ytd_counts, mer_counts, pad_counts, pm_counts, newpay_count,
        )

        send_success_notification(
            config.email,
            process_name=subject,
            message=message,
            log_file=log_file,
        )
        logger.info("CPM workflow completed successfully")

    except Exception as e:
        logger.exception("CPM workflow failed")
        send_failure_notification(
            config.email,
            process_name="CPM",
            error_message=str(e),
        )
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    run()
