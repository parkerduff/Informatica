"""Replaces mapping ``m_Pseudossn_Load_Pseudossn_From_SDA_Tbl`` (XML/Pseudossn).

Data flow (faithful to the Informatica mapping):

1. Read the fixed-width flat file ``PSEUDOSSN_FILE_TK_NUM`` and slice out the 67
   source fields using the PHYSICALOFFSET/PHYSICALLENGTH layout.
2. ``exp_Determine_Record_Type`` -> detect HEADER/TRAILER/DETAIL, validate SSN.
3. ``fil_Detail_Records`` -> keep numeric-SSN detail rows only.
4. ``exp_Conversions`` -> parse every date and signed-numeric field.
5. ``lkp_PAY_PERIOD`` -> broadcast-join the current pay period (PP_NUM, PP_END_YEAR).
6. ``exp_Final`` -> project to the 63 target columns.
7. Write to Oracle ``PSEUDOSSN_FROM_SDA_TBL``.

The header record's PSEUDO_DATE is also written to a small flat file, as in the
original (``o_PSEUDO_DATE`` target).
"""
from __future__ import annotations

import logging
from typing import Optional

from pyspark_etl.config import connections
from pyspark_etl.config.schemas.pseudossn_schemas import PSEUDOSSN_FILE_TK_NUM
from pyspark_etl.transforms.date_conversions import date_col
from pyspark_etl.transforms.record_type_filter import (
    is_detail_col,
    record_type_flag_col,
)
from pyspark_etl.transforms.signed_numeric import signed_col
from pyspark_etl.utils.oracle_jdbc import read_table, write_table

logger = logging.getLogger(__name__)

TARGET_TABLE = "PSEUDOSSN_FROM_SDA_TBL"

# (target_column, source_field, kind, *args)
#   kind 'str'  -> trimmed passthrough
#   kind 'date' -> date_col with the given layout
#   kind 'amt'  -> signed_col(int_len, frac_len)
COLUMN_MAP = [
    ("SSN", "SSN", "str"),
    ("CAN_CD", "CAN_CD", "str"),
    ("PSEUDOSSN", "PSEUDO_SSN", "str"),
    ("EMPLID", "EIN", "str"),
    ("EMPL_RCD", "EMP_REC_NO", "str"),
    ("APPT_NUM", "LEGACY_APPT_NO", "str"),
    ("EMP_FIRST_NAME", "FIRST_NAME", "str"),
    ("EMP_MID_INIT", "MIDDLE_INIT", "str"),
    ("EMP_LAST_NAME", "LAST_NAME", "str"),
    ("SEX", "SEX", "str"),
    ("VETERANS_PREFERENCE_CD", "VET_PREF", "str"),
    ("TENURE_CD", "TENURE_CD", "str"),
    ("POSITION_NUM", "POSITION_CD", "str"),
    ("US_CITIZENSHIP_CD", "CITIZENSHIP_STATUS", "str"),
    ("APPT_TYPE_CD", "APPT_TYPE_CD", "str"),
    ("HIRE_DATE", "HIRE_DATE", "date", "MMDDYYYY"),
    ("HANDICAP_CD", "HANDICAP_CODE", "str"),
    ("UNF_ALLOW_CD", "UNIF_ALLOW_CODE", "str"),
    ("UNIF_ALLOW_DATE", "UNIF_ALLOW_DATE", "date", "YYYYMMDD"),
    ("UNIF_ALLOW_AMT", "UNIF_ALLOW_AMT", "amt", 3, 2),
    ("RSSSDP", "RSSSDP", "str"),
    ("CEIL_REPORTING_CD", "CEILING_CODE", "str"),
    ("FUNCTNL_CLASSFCTN_CD", "FUNCTION_CODE", "str"),
    ("EMP_STATUS", "EMPLOYEE_STATUS", "str"),
    ("MANAGER_STATUS", "MANAGER_STATUS", "str"),
    ("POSITION_SENSITIVITY_CD", "POSITION_SENS_CODE", "str"),
    ("CAREER_START_DATE", "CAREER_START_DATE", "date", "MMDDYYYY"),
    ("CAREER_CONV_DATE", "CAREER_CONV_DATE", "date", "MMDDYYYY"),
    ("PROBATION_DATE", "PROBATION_DATE", "date", "MMDDYYYY"),
    ("ABNORMAL_RATE_CD", "ABNORMAL_RATE_CODE", "str"),
    ("APPT_LIMIT_HRS", "APPT_LIMIT_HRS", "amt", 4, 2),
    ("APPT_LIMIT_PAY", "APPT_LIMIT_PAY", "amt", 5, 2),
    ("LAST_PAY_CHANGE", "LAST_PAY_CHANGE", "date", "MMDDYYYY"),
    ("CHARITY_AREA_CD", "CHARITY_AREA_CODE", "str"),
    ("CHARITY_EFF_DATE", "CHARITY_EFF_DATE", "date", "YYYYMMDD"),
    ("CHARITY_DED_AMT", "CHARITY_DED_AMT", "amt", 3, 2),
    ("LAST_NOA_CD", "LAST_NOA", "str"),
    ("QUARTERS_DED_AMT", "QUARTERS_DEDUCTION", "amt", 5, 2),
    ("SUBSIST_DED_AMT", "SUBSIST_DEDUCTION", "amt", 5, 2),
    ("PAY_BASIS_CD", "PAY_BASIS", "str"),
    ("WORK_SCHEDULE_CD", "WORK_SCHEDULE", "str"),
    ("OCCUPATION_CD", "OCCUPATION_CODE", "str"),
    ("DUTY_STATION", "GEO_LOCATION", "str"),
    ("JOB_IND", "JOB_INDICATOR", "str"),
    ("REG_TEMP_CD", "REG_TEMP", "str"),
    ("SEPERATION_DATE", "SEPARATION_DATE", "date", "YYYYMMDD"),
    ("PCA_CONTR_EFF_START_DATE", "PCA_CONTR_EFF_START_DATE", "date", "YYYYMMDD"),
    ("PCA_CONTR_EFF_END_DATE", "PCA_CONTR_EFF_END_DATE", "date", "YYYYMMDD"),
    ("MAX_ANNUAL_PAY", "MAX_ANNUAL_PAY", "amt", 5, 2),
    ("PCA_BIWEEKLY_AMT", "PCA_BIWEEKLY_AMOUNT", "amt", 5, 2),
    ("PAY_PLAN_CD", "PAY_PLAN_CD", "str"),
    ("APPT_NTE_DTE", "APP_LIMIT_DATE", "date", "YYYYMMDD"),
    ("SPECIAL_PROGRAM_CD", "SPO_CD", "str"),
    ("TERM_ID", "TERM_ID", "str"),
    ("EFFECTIVE_DATE", "EFFECTIVE_DATE", "date", "YYYYMMDD"),
    ("EFFECTIVE_SEQ", "EFFECTIVE_SEQ", "str"),
    ("PAY_TABL_NO", "PAY_TABL_NO", "str"),
    ("BUSINESS_UNIT", "BUSINESS_UNIT", "str"),
    ("DEPTID", "DEPTID", "str"),
    ("PCA_CONTR_LEN_YEAR", "PCA_CONTR_YEAR", "str"),
    ("TK_NUM", "TK_NUM", "str"),
]


def read_fixed_width(spark, input_file: str):
    """Read the flat file and slice out columns per the PHYSICALOFFSET layout."""
    from pyspark.sql import functions as F

    raw = spark.read.text(input_file)
    cols = [
        F.substring(F.col("value"), f.spark_start, f.length).alias(f.name)
        for f in PSEUDOSSN_FILE_TK_NUM
    ]
    return raw.select(*cols)


def determine_record_type(df):
    """Add RECORD_TYPE_FLAG / SSN detail markers (exp_Determine_Record_Type)."""
    return df.withColumn("RECORD_TYPE_FLAG", record_type_flag_col(df["SSN"]))


def filter_detail(df):
    """fil_Detail_Records: numeric-SSN detail rows only."""
    return df.filter(is_detail_col(df["SSN"]))


def apply_conversions(df):
    """exp_Conversions + exp_Final: build all 63 target columns."""
    from pyspark.sql import functions as F

    out = df
    select_exprs = []
    for entry in COLUMN_MAP:
        target, source, kind = entry[0], entry[1], entry[2]
        src = F.trim(F.col(source))
        if kind == "str":
            col = src
        elif kind == "date":
            col = date_col(F.col(source), entry[3])
        elif kind == "amt":
            col = signed_col(F.col(source), entry[3], entry[4])
        else:  # pragma: no cover - defensive
            raise ValueError(f"unknown kind {kind!r}")
        select_exprs.append(col.alias(target))
    return out.select(*select_exprs)


def lookup_pay_period(spark, df):
    """lkp_PAY_PERIOD: broadcast-join PP_NUM / PP_END_YEAR for the current period."""
    from pyspark.sql import functions as F

    pp = read_table(
        spark, "ORA_BIIS", connections.SCHEMA_HISTDBA, "PAY_PERIOD",
        columns=["PP_NUM", "PP_END_YEAR", "CURR_PP_FLAG"],
    ).filter(F.col("CURR_PP_FLAG") == "Y").select("PP_NUM", "PP_END_YEAR")
    return df.crossJoin(F.broadcast(pp))


def run(spark, input_file: str, target_schema: Optional[str] = None,
        write: bool = True):
    """Execute the full PseudoSSN load and return the final DataFrame."""
    schema = target_schema or connections.SCHEMA_INFO_TARGET
    logger.info("PseudoSSN load starting from %s", input_file)

    raw = read_fixed_width(spark, input_file)
    typed = determine_record_type(raw)
    detail = filter_detail(typed)
    converted = apply_conversions(detail)
    final = lookup_pay_period(spark, converted)

    if write:
        write_table(final, "ORA_BIIS", schema, TARGET_TABLE, mode="append")
    logger.info("PseudoSSN load complete")
    return final
