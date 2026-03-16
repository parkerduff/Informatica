"""
COBOL Signed Numeric Parsing UDFs

Replicates the COBOL signed numeric parsing from the Pseudossn file
(lines 832-834) and CPM VSAM file processing.

Mainframe COBOL files encode sign in the last byte of numeric fields.
"""

import logging
from decimal import Decimal, InvalidOperation

from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType, DoubleType

logger = logging.getLogger(__name__)

# COBOL overpunch sign encoding:
# Last character encodes both the digit and the sign.
# Positive: '{' = 0, 'A' = 1, 'B' = 2, ..., 'I' = 9
# Negative: '}' = 0, 'J' = 1, 'K' = 2, ..., 'R' = 9
# Also handles explicit '+' and '-' sign characters.

POSITIVE_OVERPUNCH = {
    "{": "0", "A": "1", "B": "2", "C": "3", "D": "4",
    "E": "5", "F": "6", "G": "7", "H": "8", "I": "9",
}
NEGATIVE_OVERPUNCH = {
    "}": "0", "J": "1", "K": "2", "L": "3", "M": "4",
    "N": "5", "O": "6", "P": "7", "Q": "8", "R": "9",
}


def _parse_signed_amount_impl(raw_string, decimal_places):
    """
    Parse a COBOL signed numeric field.

    Extracts sign character from last position, splits integer/decimal parts,
    applies sign.

    Examples:
        "12345+" with 2 decimal places -> 123.45
        "12345-" with 2 decimal places -> -123.45
        "12345{" with 2 decimal places -> 123.40  (overpunch)
        "12345J" with 2 decimal places -> -123.41 (overpunch)
        Non-numeric -> 0

    Parameters
    ----------
    raw_string : str
    decimal_places : int

    Returns
    -------
    float
    """
    if raw_string is None or len(raw_string) == 0:
        return 0.0

    raw_string = raw_string.strip()
    if len(raw_string) == 0:
        return 0.0

    last_char = raw_string[-1]
    body = raw_string[:-1]
    sign = 1
    last_digit = ""

    if last_char == "+":
        sign = 1
        last_digit = ""
    elif last_char == "-":
        sign = -1
        last_digit = ""
    elif last_char == " ":
        sign = 1
        last_digit = ""
    elif last_char in POSITIVE_OVERPUNCH:
        sign = 1
        last_digit = POSITIVE_OVERPUNCH[last_char]
    elif last_char in NEGATIVE_OVERPUNCH:
        sign = -1
        last_digit = NEGATIVE_OVERPUNCH[last_char]
    elif last_char.isdigit():
        body = raw_string
        last_digit = ""
        sign = 1
    else:
        return 0.0

    numeric_str = body + last_digit

    if not numeric_str:
        return 0.0

    cleaned = ""
    for ch in numeric_str:
        if ch.isdigit():
            cleaned += ch
        else:
            return 0.0

    if not cleaned:
        return 0.0

    try:
        if decimal_places > 0 and len(cleaned) > decimal_places:
            integer_part = cleaned[:-decimal_places]
            decimal_part = cleaned[-decimal_places:]
            value = float(f"{integer_part}.{decimal_part}")
        elif decimal_places > 0:
            cleaned = cleaned.zfill(decimal_places + 1)
            integer_part = cleaned[:-decimal_places]
            decimal_part = cleaned[-decimal_places:]
            value = float(f"{integer_part}.{decimal_part}")
        else:
            value = float(cleaned)

        return sign * value
    except (ValueError, InvalidOperation):
        return 0.0


def parse_signed_amount_udf(decimal_places=2):
    """
    Create a PySpark UDF for parsing COBOL signed numeric fields.

    Parameters
    ----------
    decimal_places : int
        Number of implied decimal places.

    Returns
    -------
    Column expression (UDF)
    """
    @F.udf(DoubleType())
    def _udf(raw_string):
        return _parse_signed_amount_impl(raw_string, decimal_places)

    return _udf


def parse_signed_amount(col_expr, decimal_places=2):
    """
    Apply COBOL signed numeric parsing to a column.

    Usage:
        df = df.withColumn("AMOUNT", parse_signed_amount(F.col("RAW_AMT"), 2))

    Parameters
    ----------
    col_expr : Column
        The raw string column to parse.
    decimal_places : int

    Returns
    -------
    Column
    """
    udf_func = parse_signed_amount_udf(decimal_places)
    return udf_func(col_expr)
