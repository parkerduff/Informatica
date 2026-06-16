"""Signed-numeric (VSAM-style) field parsing.

The COBOL/VSAM flat files store amounts as fixed-width numeric strings where the
last character is the sign (``+`` or ``-``) and a decimal point is *implied* a
fixed number of digits from the right. The Informatica ``exp_Conversions``
expression rebuilds each amount as ``<int>.<frac>`` then applies the sign, e.g.::

    UNIF_ALLOW_AMT (6 chars):  chars 1-3 + '.' + chars 4-5, sign = char 6
    QUARTERS_DEDUCTION (8):    chars 1-5 + '.' + chars 6-7, sign = char 8
    APPT_LIMIT_HRS (7):        chars 1-4 + '.' + chars 5-6, sign = char 7

If the rebuilt value is not numeric the original returns ``0`` (the DECODE
fallthrough), which is preserved here.
"""
from __future__ import annotations

from decimal import Decimal, InvalidOperation
from typing import Optional


def parse_signed(value: Optional[str], int_len: int, frac_len: int) -> Decimal:
    """Parse a signed-numeric fixed-width field.

    ``int_len`` and ``frac_len`` give the number of integer and fractional
    digits; the field is expected to be ``int_len + frac_len + 1`` chars long,
    where the final char is the sign. Returns ``Decimal('0')`` on bad input,
    matching the Informatica DECODE default.
    """
    v = (value or "")
    expected = int_len + frac_len + 1
    if len(v) < expected:
        return Decimal("0")
    int_part = v[0:int_len]
    frac_part = v[int_len:int_len + frac_len]
    sign_char = v[int_len + frac_len:int_len + frac_len + 1]
    magnitude = f"{int_part}.{frac_part}"
    try:
        amount = Decimal(magnitude)
    except (InvalidOperation, ValueError):
        return Decimal("0")
    if sign_char == "-":
        amount = -amount
    return amount


# Convenience wrappers for the exact field widths used in the Pseudossn mapping.
def parse_unif_allow_amt(value: Optional[str]) -> Decimal:
    """6-char field: 3 int + 2 frac + sign (UNIF_ALLOW_AMT, CHARITY_DED_AMT)."""
    return parse_signed(value, int_len=3, frac_len=2)


def parse_appt_limit_hrs(value: Optional[str]) -> Decimal:
    """7-char field: 4 int + 2 frac + sign (APPT_LIMIT_HRS)."""
    return parse_signed(value, int_len=4, frac_len=2)


def parse_8char_amt(value: Optional[str]) -> Decimal:
    """8-char field: 5 int + 2 frac + sign.

    Covers APPT_LIMIT_PAY, QUARTERS_DEDUCTION, SUBSIST_DEDUCTION,
    MAX_ANNUAL_PAY, PCA_BIWEEKLY_AMOUNT.
    """
    return parse_signed(value, int_len=5, frac_len=2)


# --- PySpark helper -----------------------------------------------------------
def signed_col(column, int_len: int, frac_len: int, scale: int = 2):
    """Return a ``DecimalType`` Spark Column parsing a signed-numeric field."""
    from pyspark.sql.functions import udf
    from pyspark.sql.types import DecimalType

    precision = int_len + frac_len + 5
    return udf(
        lambda v: parse_signed(v, int_len, frac_len),
        DecimalType(precision, scale),
    )(column)
