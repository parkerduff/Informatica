"""Unit tests for the Informatica compatibility library semantics."""
import datetime as dt
import os
import sys
from decimal import Decimal

import pytest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from migration.lib import infa_compat as C


def test_substr_1_based():
    assert C.substr("ABCDEF", 1, 3) == "ABC"
    assert C.substr("ABCDEF", 4) == "DEF"
    assert C.substr(None, 1, 3) is None


def test_ltrim_rtrim():
    assert C.ltrim("  x ") == "x "
    assert C.rtrim(" x  ") == " x"


def test_iif_and_decode():
    assert C.iif(True, "a", "b") == "a"
    assert C.iif(False, "a", "b") == "b"
    assert C.decode(2, 1, "one", 2, "two", "other") == "two"
    assert C.decode(9, 1, "one", 2, "two", "other") == "other"


def test_is_date_and_is_number():
    assert C.is_date("20240115", "YYYYMMDD") is True
    assert C.is_date("20241315", "YYYYMMDD") is False   # month 13
    assert C.is_number("123.45") is True
    assert C.is_number("12x") is False


def test_to_date_formats():
    assert C.to_date("20240115", "YYYYMMDD") == dt.datetime(2024, 1, 15)
    # YYYYDDMM edge case noted in Pseudossn v_EFFECTIVE_DATE
    assert C.to_date("20241501", "YYYYDDMM") == dt.datetime(2024, 1, 15)


def test_to_decimal_scale_and_nonfinite():
    assert C.to_decimal("12.345", 2) == Decimal("12.35")
    assert C.to_decimal("abc") is None
    assert C.to_decimal("NaN") is None          # non-finite -> NULL
    assert C.to_decimal("Infinity") is None


def test_sign_handles_bad_input():
    assert C.sign("5") == 1
    assert C.sign("-3") == -1
    assert C.sign("0") == 0
    assert C.sign("NaN") is None                # must not raise


def test_lpad_rpad():
    assert C.lpad("7", 3, "0") == "007"
    assert C.rpad("7", 3, "0") == "700"


def test_fixed_width_slice():
    line = "HEADER  " + "X" * 4
    assert C.slice_fixed(line, 0, 6) == "HEADER"
    assert C.slice_fixed(line, 8, 4) == "XXXX"


def test_trailing_sign_decimal():
    # explicit trailing sign char + implied decimal (scale=2)
    assert C.decode_trailing_sign("123", "-", 2) == Decimal("-1.23")
    assert C.decode_trailing_sign("123", "+", 2) == Decimal("1.23")


def test_zoned_overpunch():
    # zoned overpunch: sign encoded in last digit ('}' == -0, 'J' == -1, etc.)
    assert C.decode_zoned_overpunch("012J", 0) == Decimal("-121")
    assert C.decode_zoned_overpunch("012{", 0) == Decimal("120")


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-v"]))
