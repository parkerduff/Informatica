"""Fixed-width schema for the PSEUDOSSN_FILE_TK_NUM flat file.

Auto-derived from the <SOURCE NAME="PSEUDOSSN_FILE_TK_NUM"> definition in the
Pseudossn Informatica export. PHYSICALOFFSET is 0-indexed; PySpark substring
is 1-indexed, so parse_fixed_width adds 1 to each offset.
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType

# (field_name, offset, length, target_datatype)
PSEUDOSSN_FIXED_WIDTH_FIELDS = [
    ("SSN", 0, 9, "string"),
    ("CAN_CD", 9, 8, "string"),
    ("PSEUDO_SSN", 17, 9, "string"),
    ("EIN", 26, 8, "string"),
    ("EMP_REC_NO", 34, 2, "string"),
    ("LEGACY_APPT_NO", 36, 2, "string"),
    ("FIRST_NAME", 38, 11, "string"),
    ("MIDDLE_INIT", 49, 1, "string"),
    ("LAST_NAME", 50, 16, "string"),
    ("SEX", 66, 1, "string"),
    ("VET_PREF", 67, 1, "string"),
    ("TENURE_CD", 68, 1, "string"),
    ("POSITION_CD", 69, 1, "string"),
    ("CITIZENSHIP_STATUS", 70, 1, "string"),
    ("APPT_TYPE_CD", 71, 2, "string"),
    ("HIRE_DATE", 73, 8, "string"),
    ("HANDICAP_CODE", 81, 2, "string"),
    ("UNIF_ALLOW_CODE", 83, 3, "string"),
    ("UNIF_ALLOW_DATE", 86, 8, "string"),
    ("UNIF_ALLOW_AMT", 94, 6, "string"),
    ("RSSSDP", 100, 4, "string"),
    ("CEILING_CODE", 104, 1, "string"),
    ("FUNCTION_CODE", 105, 1, "string"),
    ("EMPLOYEE_STATUS", 106, 1, "string"),
    ("MANAGER_STATUS", 107, 1, "string"),
    ("POSITION_SENS_CODE", 108, 1, "string"),
    ("CAREER_START_DATE", 109, 8, "string"),
    ("CAREER_CONV_DATE", 117, 8, "string"),
    ("PROBATION_DATE", 125, 8, "string"),
    ("ABNORMAL_RATE_CODE", 133, 1, "string"),
    ("APPT_LIMIT_HRS", 134, 7, "string"),
    ("APPT_LIMIT_PAY", 141, 8, "string"),
    ("LAST_PAY_CHANGE", 149, 8, "string"),
    ("CHARITY_AREA_CODE", 157, 3, "string"),
    ("CHARITY_EFF_DATE", 160, 8, "string"),
    ("CHARITY_DED_AMT", 168, 6, "string"),
    ("LAST_NOA", 174, 4, "string"),
    ("FILLER_1", 178, 4, "string"),
    ("QUARTERS_DEDUCTION", 182, 8, "string"),
    ("SUBSIST_DEDUCTION", 190, 8, "string"),
    ("PAY_BASIS", 198, 2, "string"),
    ("WORK_SCHEDULE", 200, 1, "string"),
    ("OCCUPATION_CODE", 201, 4, "string"),
    ("FILLER_2", 205, 3, "string"),
    ("GEO_LOCATION", 208, 9, "string"),
    ("JOB_INDICATOR", 217, 1, "string"),
    ("REG_TEMP", 218, 1, "string"),
    ("SEPARATION_DATE", 219, 8, "string"),
    ("FILLER_3", 227, 1, "string"),
    ("PCA_CONTR_EFF_START_DATE", 228, 8, "string"),
    ("PCA_CONTR_EFF_END_DATE", 236, 8, "string"),
    ("MAX_ANNUAL_PAY", 244, 8, "string"),
    ("PCA_BIWEEKLY_AMOUNT", 252, 8, "string"),
    ("FILLER_4", 260, 33, "string"),
    ("PAY_PLAN_CD", 293, 2, "string"),
    ("APP_LIMIT_DATE", 295, 8, "string"),
    ("SPO_CD", 303, 4, "string"),
    ("TERM_ID", 307, 2, "string"),
    ("EFFECTIVE_DATE", 309, 8, "string"),
    ("EFFECTIVE_SEQ", 317, 3, "string"),
    ("FILLER_5", 320, 9, "string"),
    ("PAY_TABL_NO", 329, 4, "string"),
    ("BUSINESS_UNIT", 333, 5, "string"),
    ("DEPTID", 338, 10, "string"),
    ("PCA_CONTR_YEAR", 348, 2, "string"),
    ("FILLER_6", 350, 150, "string"),
    ("TK_NUM", 500, 5, "string"),
]

PSEUDOSSN_FILE_SCHEMA = StructType(
    [StructField(name, StringType(), True) for name, _o, _l, _t in PSEUDOSSN_FIXED_WIDTH_FIELDS]
)


def parse_fixed_width(df: DataFrame) -> DataFrame:
    """Extract all PSEUDOSSN fixed-width columns from a single-column text
    DataFrame (the column must be named ``value``).

    PySpark ``F.substring`` is 1-indexed, so we pass ``offset + 1``.
    """
    cols = [
        F.substring(F.col("value"), offset + 1, length).alias(name)
        for name, offset, length, _dtype in PSEUDOSSN_FIXED_WIDTH_FIELDS
    ]
    return df.select(*cols)
