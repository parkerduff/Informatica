"""Header / Trailer / Detail record-type detection.

Mirrors the Pseudossn ``exp_Determine_Record_Type`` expression and the
``fil_Detail_Records`` filter:

    v_RECORD_TYPE_FLAG = DECODE(TRUE,
        SUBSTR(SSN,1,6) = 'HEADER', 'H',
        SUBSTR(SSN,1,7) = 'TRAILER', 'T',
        'D')

    o_RECORD_TYPE_FLAG = IIF(v_RECORD_TYPE_FLAG = 'D'
                             AND LENGTH(LTRIM(RTRIM(SSN))) > 0,
                             v_RECORD_TYPE_FLAG, 'R')

    o_SSN_FLAG = IS_NUMBER(SSN)

The detail filter keeps rows where ``RECORD_TYPE_FLAG = 'D'`` and the SSN is
numeric.
"""
from __future__ import annotations

from typing import Optional


def record_type_flag(ssn: Optional[str]) -> str:
    """Return ``'H'``, ``'T'``, ``'D'`` or ``'R'`` for the given SSN field.

    ``'R'`` (reject) is returned for empty/blank detail rows, matching the
    Informatica ``o_RECORD_TYPE_FLAG`` output.
    """
    raw = ssn or ""
    if raw[:6] == "HEADER":
        return "H"
    if raw[:7] == "TRAILER":
        return "T"
    if len(raw.strip()) > 0:
        return "D"
    return "R"


def is_number(value: Optional[str]) -> bool:
    """Informatica ``IS_NUMBER`` equivalent (accepts an optional sign/decimal)."""
    v = (value or "").strip()
    if not v:
        return False
    try:
        float(v)
        return True
    except ValueError:
        return False


def is_detail(ssn: Optional[str]) -> bool:
    """True when the row is a numeric-SSN detail record (passes fil_Detail_Records)."""
    return record_type_flag(ssn) == "D" and is_number(ssn)


# --- PySpark helpers ----------------------------------------------------------
def record_type_flag_col(column):
    from pyspark.sql.functions import udf
    from pyspark.sql.types import StringType

    return udf(record_type_flag, StringType())(column)


def is_detail_col(column):
    from pyspark.sql.functions import udf
    from pyspark.sql.types import BooleanType

    return udf(is_detail, BooleanType())(column)


def filter_detail_records(df, ssn_column: str = "SSN"):
    """Apply the ``fil_Detail_Records`` filter to a Spark DataFrame."""
    return df.filter(is_detail_col(df[ssn_column]))
