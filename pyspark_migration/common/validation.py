"""
Data Validation Utilities

Date/numeric validators and record type classifier.
Replaces Informatica Expression transformations for validation logic.
"""

import logging
from datetime import datetime, date

from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType, DateType, StringType

logger = logging.getLogger(__name__)


def _classify_record_type_impl(ssn_field):
    """
    Classify a record as Header, Trailer, or Detail based on SSN field.

    Replicates the DECODE logic from Pseudossn line 790:
        SUBSTR(SSN,1,6)='HEADER' -> 'H'
        SUBSTR(SSN,1,7)='TRAILER' -> 'T'
        else -> 'D'

    Also used in COMPTIME exp_Initial transformation.

    Parameters
    ----------
    ssn_field : str

    Returns
    -------
    str
        'H' for header, 'T' for trailer, 'D' for detail.
    """
    if ssn_field is None:
        return "D"

    upper = ssn_field.strip().upper()
    if upper[:6] == "HEADER":
        return "H"
    if upper[:7] == "TRAILER":
        return "T"
    return "D"


classify_record_type = F.udf(_classify_record_type_impl, StringType())


def _validate_and_convert_date_impl(date_string, fmt):
    """
    Validate and convert a date string to a date object.

    Supports formats as seen in Pseudossn lines 795-802:
        MMDDYYYY -> '%m%d%Y'
        YYYYMMDD -> '%Y%m%d'
        YYYYDDMM -> '%Y%d%m'

    Replicates IIF(IS_DATE(...), TO_DATE(...)) pattern.

    Parameters
    ----------
    date_string : str
    fmt : str
        One of 'MMDDYYYY', 'YYYYMMDD', 'YYYYDDMM'.

    Returns
    -------
    date or None
    """
    if date_string is None:
        return None

    cleaned = date_string.strip()
    if not cleaned or len(cleaned) != 8:
        return None

    format_map = {
        "MMDDYYYY": "%m%d%Y",
        "YYYYMMDD": "%Y%m%d",
        "YYYYDDMM": "%Y%d%m",
    }

    py_fmt = format_map.get(fmt)
    if py_fmt is None:
        return None

    try:
        return datetime.strptime(cleaned, py_fmt).date()
    except (ValueError, OverflowError):
        return None


validate_and_convert_date = F.udf(_validate_and_convert_date_impl, DateType())


def _is_valid_ssn_impl(ssn_string):
    """
    Check whether a string is a valid 9-digit SSN.

    Replicates SSN_FLAG = TRUE filter from Pseudossn line 724:
    SSN must be exactly 9 digits.

    Parameters
    ----------
    ssn_string : str

    Returns
    -------
    bool
    """
    if ssn_string is None:
        return False

    cleaned = ssn_string.strip()
    return len(cleaned) == 9 and cleaned.isdigit()


is_valid_ssn = F.udf(_is_valid_ssn_impl, BooleanType())


def _is_numeric_impl(value):
    """Check if a string is numeric (replicates IS_NUMBER)."""
    if value is None:
        return False
    try:
        float(value.strip())
        return True
    except (ValueError, TypeError):
        return False


is_numeric = F.udf(_is_numeric_impl, BooleanType())


def determine_output_flag(record_type_col, ssn_col):
    """
    Determine the output flag for a record.

    Replicates logic from Pseudossn line 791:
        IIF(v_RECORD_TYPE_FLAG = 'D' AND LENGTH(LTRIM(RTRIM(SSN))) > 0, 'D', 'R')

    Parameters
    ----------
    record_type_col : Column
        Column containing record type flag ('H', 'T', 'D').
    ssn_col : Column
        Column containing SSN value.

    Returns
    -------
    Column
        'D' for detail records with non-empty SSN, 'R' for rejected.
    """
    return F.when(
        (record_type_col == "D") & (F.length(F.trim(ssn_col)) > 0),
        F.lit("D"),
    ).otherwise(F.lit("R"))
