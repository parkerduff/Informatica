"""PySpark migration of Informatica workflow ``wf_COMPTIME``.

Source: ``XML/COMPTIME`` (Informatica PowerCenter 9.6.1).

Three sessions, each a function:

1. ``s_COMPTIME_Current_Pay_Period``        -> :func:`get_current_pay_period`
2. ``s_COMPTIME_Load_COMP_TIME_DAILY_TBL``  -> :func:`load_comp_time_daily`
3. ``s_COMPTIME_Build_Message_Counters``    -> :func:`build_message_counters`
"""
from __future__ import annotations

import argparse
from datetime import datetime
from typing import Any, Dict

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from utils import config as cfg
from utils import db

ENVIRONMENT = "TEST: "
MAPPING_NAME = "m_COMPTIME_Build_Message_Counters"
COUNTER_DESCRIPTION_1 = "Number of detail records from the COMP TIME file."

# Source definition U0287D01: 12 comma-delimited fields, in order.
SOURCE_FIELDS = [
    "SSN",
    "NAME",
    "CURRENT_ACCT",
    "CURRENT_ORG",
    "FLSA_STATUS",
    "COMP_TIME_CUR_BAL",
    "COMP_TIME_YEAR_EARNED",
    "PP_END_DATE",
    "DAILY_DATE_EARNED",
    "COMP_TIME_RATE",
    "COMP_TIME_HOURS",
    "COMP_TIME_UNDEF",
]


def _source_schema() -> StructType:
    # All read as strings first (flat file), converted downstream like Informatica.
    return StructType([StructField(name, StringType(), True) for name in SOURCE_FIELDS])


def read_source_csv(spark, file_path: str) -> DataFrame:
    """Read the comma-delimited, 12-field COMP TIME flat file as raw strings."""
    df = (
        spark.read.option("header", "false")
        .option("quote", '"')
        .option("delimiter", ",")
        .schema(_source_schema())
        .csv(file_path)
    )
    if len(df.columns) != len(SOURCE_FIELDS):  # pragma: no cover - schema enforces 12
        raise ValueError(
            f"Expected {len(SOURCE_FIELDS)} fields, got {len(df.columns)}"
        )
    return df


def _is_number_col(col):
    """Informatica ``IS_NUMBER`` on a string column -> boolean column."""
    trimmed = F.trim(col)
    return (
        trimmed.isNotNull()
        & (trimmed != "")
        & (trimmed.rlike(r"^[+-]?(\d+\.?\d*|\.\d+)$"))
    )


# ---------------------------------------------------------------------------
# Step 1 - m_COMPTIME_Current_Pay_Period
# ---------------------------------------------------------------------------
def get_current_pay_period(spark, config: Dict[str, Any]) -> Dict[str, Any]:
    """Read the current pay period (CURR_PP_FLAG='Y') and derive PP_YEAR_NUM."""
    rows = db.spark_df_from_query(
        spark,
        config,
        "SELECT PP_NUM, PP_END_YEAR FROM PAY_PERIOD WHERE CURR_PP_FLAG = 'Y'",
    ).collect()
    if not rows:
        raise ValueError("No current pay period set (CURR_PP_FLAG='Y').")
    pp_num = int(rows[0]["PP_NUM"])
    pp_end_year = int(rows[0]["PP_END_YEAR"])
    pp_year_num = int(f"{pp_end_year}{pp_num:02d}")
    return {"pp_num": pp_num, "pp_end_year": pp_end_year, "pp_year_num": pp_year_num}


# ---------------------------------------------------------------------------
# Step 2 - m_COMPTIME_Load_COMP_TIME_DAILY_TBL
# ---------------------------------------------------------------------------
def transform_comp_time_daily(spark, source_df: DataFrame, pay_period: Dict[str, Any]) -> DataFrame:
    """Apply exp_Initial / fil_Valid_Records / lkp_PAY_PERIOD / exp_Convert.

    Returns a DataFrame matching COMP_TIME_DAILY_TBL (15 columns).
    """
    # exp_Initial: VALID_RECORD_FLAG = IS_NUMBER(SSN)
    enriched = source_df.withColumn("o_VALID_RECORD_FLAG", _is_number_col(F.col("SSN")))
    # fil_Valid_Records
    valid = enriched.filter(F.col("o_VALID_RECORD_FLAG"))

    pp_num = pay_period["pp_num"]
    pp_end_year = pay_period["pp_end_year"]
    pp_year_num = pay_period["pp_year_num"]

    # exp_Convert + exp_Final -> target columns
    result = valid.select(
        F.lit(pp_end_year).cast("smallint").alias("PP_END_YEAR"),
        F.lit(pp_num).cast("smallint").alias("PP_NUM"),
        F.lit(pp_year_num).cast("int").alias("PP_YEAR_NUM"),
        F.col("SSN"),
        F.col("NAME"),
        F.col("CURRENT_ACCT"),
        F.col("CURRENT_ORG"),
        F.col("FLSA_STATUS"),
        F.col("COMP_TIME_CUR_BAL").cast("decimal(8,2)").alias("COMP_TIME_CUR_BAL"),
        F.col("COMP_TIME_YEAR_EARNED").cast("decimal(4,0)").alias("COMP_TIME_YEAR_EARNED"),
        _to_date_yyyymmdd(F.col("PP_END_DATE")).alias("PP_END_DATE"),
        _to_date_yyyymmdd(F.col("DAILY_DATE_EARNED")).alias("DAILY_DATE_EARNED"),
        F.col("COMP_TIME_RATE").cast("decimal(6,2)").alias("COMP_TIME_RATE"),
        F.col("COMP_TIME_HOURS").cast("decimal(8,2)").alias("COMP_TIME_HOURS"),
        F.col("COMP_TIME_UNDEF").cast("decimal(6,0)").alias("COMP_TIME_UNDEF"),
    )
    return result


def _to_date_yyyymmdd(col):
    """IIF(IS_DATE(x,'YYYYMMDD'), TO_DATE(x,'YYYYMMDD')) -> NULL on invalid.

    ``try_to_timestamp`` mirrors Informatica's IS_DATE guard by returning NULL
    for unparseable input instead of raising (Spark ANSI mode).
    """
    return F.try_to_timestamp(col, F.lit("yyyyMMdd"))


def load_comp_time_daily(
    spark, config: Dict[str, Any], file_path: str, pay_period: Dict[str, Any]
) -> int:
    """Load valid detail records into COMP_TIME_DAILY_TBL. Returns row count."""
    source_df = read_source_csv(spark, file_path)
    if source_df.count() == 0:
        raise ValueError(f"Empty source file: {file_path}")
    result = transform_comp_time_daily(spark, source_df, pay_period)
    rows = result.collect()
    _insert_comp_time_daily(config, rows)
    return len(rows)


def _insert_comp_time_daily(config: Dict[str, Any], rows) -> None:
    import pyodbc

    conn = db.get_pyodbc_connection(config)
    try:
        cursor = conn.cursor()
        for r in rows:
            cursor.execute(
                "INSERT INTO COMP_TIME_DAILY_TBL "
                "(PP_END_YEAR, PP_NUM, PP_YEAR_NUM, SSN, NAME, CURRENT_ACCT, CURRENT_ORG, "
                "FLSA_STATUS, COMP_TIME_CUR_BAL, COMP_TIME_YEAR_EARNED, PP_END_DATE, "
                "DAILY_DATE_EARNED, COMP_TIME_RATE, COMP_TIME_HOURS, COMP_TIME_UNDEF) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                r["PP_END_YEAR"], r["PP_NUM"], r["PP_YEAR_NUM"], r["SSN"], r["NAME"],
                r["CURRENT_ACCT"], r["CURRENT_ORG"], r["FLSA_STATUS"], r["COMP_TIME_CUR_BAL"],
                r["COMP_TIME_YEAR_EARNED"], r["PP_END_DATE"], r["DAILY_DATE_EARNED"],
                r["COMP_TIME_RATE"], r["COMP_TIME_HOURS"], r["COMP_TIME_UNDEF"],
            )
        cursor.close()
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Step 3 - m_COMPTIME_Build_Message_Counters
# ---------------------------------------------------------------------------
def count_detail_records(spark, source_df: DataFrame) -> int:
    """exp_Initial RECORD_TYPE_FLAG='D' + fil_Detail + agg COUNT(SSN)."""
    flagged = source_df.withColumn(
        "o_RECORD_TYPE_FLAG",
        F.when(_is_number_col(F.col("SSN")), F.lit("D")).otherwise(F.lit("NO")),
    )
    detail = flagged.filter(F.col("o_RECORD_TYPE_FLAG") == "D")
    agg = detail.agg(F.count("SSN").alias("o_DETAIL_RECORD_COUNT")).collect()
    return int(agg[0]["o_DETAIL_RECORD_COUNT"])


def build_message_counters(
    spark, config: Dict[str, Any], file_path: str, pay_period: Dict[str, Any]
) -> Dict[str, Any]:
    """Count detail records, write COUNTER_TBL, build notification message."""
    source_df = read_source_csv(spark, file_path)
    detail_count = count_detail_records(spark, source_df)

    run_date = datetime.now()
    _insert_counter(config, run_date, MAPPING_NAME, COUNTER_DESCRIPTION_1, detail_count,
                    pay_period)

    v_pp_num = f"{pay_period['pp_num']:02d}" if pay_period["pp_num"] < 10 else str(pay_period["pp_num"])
    subject = (
        f"{ENVIRONMENT}Comp Time File loaded successfully for Pay Period:  "
        f"{pay_period['pp_end_year']}-{v_pp_num}"
    )
    message = f"Number of Detail Records from Comp Time file\t= {detail_count}"
    return {
        "detail_count": detail_count,
        "subject": subject,
        "message": message,
        "counter_description": COUNTER_DESCRIPTION_1,
    }


def _insert_counter(config, run_date, process_name, description, value, pay_period) -> None:
    conn = db.get_pyodbc_connection(config)
    try:
        cursor = conn.cursor()
        cursor.execute(
            "INSERT INTO COUNTER_TBL "
            "(RUN_DATE, PROCESS_NAME, COUNTER_DESCRIPTION, COUNTER_VALUE, PP_END_YEAR, PP_NUM) "
            "VALUES (?,?,?,?,?,?)",
            run_date, process_name, description, value,
            pay_period["pp_end_year"], pay_period["pp_num"],
        )
        cursor.close()
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Main entry point - wf_COMPTIME
# ---------------------------------------------------------------------------
def run(spark, config: Dict[str, Any], file_path: str) -> Dict[str, Any]:
    pay_period = get_current_pay_period(spark, config)
    loaded = load_comp_time_daily(spark, config, file_path, pay_period)
    counters = build_message_counters(spark, config, file_path, pay_period)
    return {
        "status": "SUCCESS",
        "pp_num": pay_period["pp_num"],
        "pp_end_year": pay_period["pp_end_year"],
        "pp_year_num": pay_period["pp_year_num"],
        "records_loaded": loaded,
        "detail_count": counters["detail_count"],
        "message_subject": counters["subject"],
        "message_body": counters["message"],
    }


def _build_spark():
    from pyspark.sql import SparkSession

    return SparkSession.builder.master("local[*]").appName("comptime").getOrCreate()


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the COMPTIME PySpark job")
    parser.add_argument("--env", default="test")
    parser.add_argument("--file-path", required=True)
    args = parser.parse_args()

    config = cfg.load_config(args.env)
    spark = _build_spark()
    try:
        result = run(spark, config, args.file_path)
        print("COMPTIME job result:")
        for key, value in result.items():
            print(f"  {key}: {value}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
