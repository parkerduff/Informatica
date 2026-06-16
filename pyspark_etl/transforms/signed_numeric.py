"""Signed fixed-width numeric parsing.

Mirrors the signed-amount patterns in the Pseudossn mapping (e.g. UNIF_ALLOW_AMT
at lines ~831-834, and the deduction/amount fields at ~851-900) where the last
character of a fixed-width field carries the sign and the preceding characters
are the integer and decimal digits.
"""
from pyspark.sql import Column
from pyspark.sql import functions as F


def parse_signed_amount(col: Column, integer_len: int, decimal_len: int) -> Column:
    """Parse a fixed-width signed numeric field.

    The last character is the sign (+/-), the preceding characters are digits:
    ``integer_len`` chars before the decimal point and ``decimal_len`` after.
    Total field length = ``integer_len + decimal_len + 1`` (the sign char).

    Examples::

        "12345+", integer_len=3, decimal_len=2 ->  123.45
        "12345-", integer_len=3, decimal_len=2 -> -123.45
    """
    total_len = integer_len + decimal_len
    sign_char = F.substring(col, total_len + 1, 1)
    numeric_str = F.concat(
        F.substring(col, 1, integer_len),
        F.lit("."),
        F.substring(col, integer_len + 1, decimal_len),
    )
    is_number = F.substring(col, 1, total_len).rlike("^[0-9]+$")
    amount = F.when(is_number, numeric_str.cast("decimal(10,2)")).otherwise(F.lit(0))
    return F.when(sign_char == F.lit("-"), amount * -1).otherwise(amount)
