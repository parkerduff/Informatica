"""
Unit Tests for COBOL Signed Numeric Parser

Tests the parse_signed_amount logic from common/cobol_parser.py.
"""

import pytest

from pyspark_migration.common.cobol_parser import _parse_signed_amount_impl


class TestParseSignedAmount:
    """Tests for COBOL signed numeric parsing."""

    def test_positive_signed_amount(self):
        """'12345+' with 2 decimal places -> 123.45"""
        result = _parse_signed_amount_impl("12345+", 2)
        assert result == 123.45

    def test_negative_signed_amount(self):
        """'12345-' with 2 decimal places -> -123.45"""
        result = _parse_signed_amount_impl("12345-", 2)
        assert result == -123.45

    def test_unsigned_amount(self):
        """'12345 ' with 2 decimal places -> 123.45 (trailing space = positive)"""
        result = _parse_signed_amount_impl("12345 ", 2)
        assert result == 123.45

    def test_non_numeric_amount(self):
        """'ABCDE+' -> 0 (non-numeric body returns 0)"""
        result = _parse_signed_amount_impl("ABCDE+", 2)
        assert result == 0.0

    def test_zero_amount(self):
        """'00000+' with 2 decimal places -> 0.0"""
        result = _parse_signed_amount_impl("00000+", 2)
        assert result == 0.0

    def test_positive_overpunch_brace(self):
        """'{' overpunch = positive 0"""
        result = _parse_signed_amount_impl("1234{", 2)
        assert result == 12.30

    def test_negative_overpunch_j(self):
        """'J' overpunch = negative 1"""
        result = _parse_signed_amount_impl("1234J", 2)
        assert result == -12.31

    def test_positive_overpunch_i(self):
        """'I' overpunch = positive 9"""
        result = _parse_signed_amount_impl("1234I", 2)
        assert result == 12.39

    def test_negative_overpunch_r(self):
        """'R' overpunch = negative 9"""
        result = _parse_signed_amount_impl("1234R", 2)
        assert result == -12.39

    def test_none_input(self):
        """None input -> 0.0"""
        result = _parse_signed_amount_impl(None, 2)
        assert result == 0.0

    def test_empty_string(self):
        """Empty string -> 0.0"""
        result = _parse_signed_amount_impl("", 2)
        assert result == 0.0

    def test_whitespace_only(self):
        """Whitespace-only string -> 0.0"""
        result = _parse_signed_amount_impl("   ", 2)
        assert result == 0.0

    def test_zero_decimal_places(self):
        """'12345+' with 0 decimal places -> 12345.0"""
        result = _parse_signed_amount_impl("12345+", 0)
        assert result == 12345.0

    def test_all_digits_no_sign(self):
        """Pure digits without sign character -> treated as positive."""
        result = _parse_signed_amount_impl("12345", 2)
        assert result == 123.45

    def test_large_number(self):
        """Large number with sign."""
        result = _parse_signed_amount_impl("999999999+", 2)
        assert result == 9999999.99

    def test_three_decimal_places(self):
        """'12345+' with 3 decimal places -> 12.345"""
        result = _parse_signed_amount_impl("12345+", 3)
        assert result == 12.345
