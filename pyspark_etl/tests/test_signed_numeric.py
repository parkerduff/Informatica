from decimal import Decimal

import pytest

from pyspark_etl.transforms.signed_numeric import (
    parse_8char_amt,
    parse_appt_limit_hrs,
    parse_signed,
    parse_unif_allow_amt,
)


def test_unif_allow_amt_positive():
    # 6 chars: 3 int + 2 frac + sign. "12345+" -> 123.45
    assert parse_unif_allow_amt("12345+") == Decimal("123.45")


def test_unif_allow_amt_negative():
    assert parse_unif_allow_amt("12345-") == Decimal("-123.45")


def test_unif_allow_amt_unsigned_treated_positive():
    # Space sign char -> treated as positive (DECODE IS_NUMBER fallthrough).
    assert parse_unif_allow_amt("12345 ") == Decimal("123.45")


def test_appt_limit_hrs():
    # 7 chars: 4 int + 2 frac + sign. "001050+" -> 10.50
    assert parse_appt_limit_hrs("001050+") == Decimal("10.50")
    assert parse_appt_limit_hrs("001050-") == Decimal("-10.50")


def test_8char_amt():
    # 8 chars: 5 int + 2 frac + sign. "1234567+" -> 12345.67
    assert parse_8char_amt("1234567+") == Decimal("12345.67")
    assert parse_8char_amt("1234567-") == Decimal("-12345.67")


@pytest.mark.parametrize("bad", ["", None, "ab", "12"])
def test_too_short_returns_zero(bad):
    assert parse_unif_allow_amt(bad) == Decimal("0")


def test_non_numeric_returns_zero():
    assert parse_signed("ABCDE+", 3, 2) == Decimal("0")


def test_generic_widths():
    assert parse_signed("12345+", int_len=3, frac_len=2) == Decimal("123.45")
