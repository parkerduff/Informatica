"""
Unit Tests for Validation Utilities

Tests record type classification, date conversion, and SSN validation
from common/validation.py.
"""

from datetime import date

import pytest

from pyspark_migration.common.validation import (
    _classify_record_type_impl,
    _is_valid_ssn_impl,
    _validate_and_convert_date_impl,
)


class TestClassifyRecordType:
    """Tests for record type classification (Pseudossn line 790)."""

    def test_classify_header(self):
        """SSN starting with 'HEADER' -> 'H'"""
        assert _classify_record_type_impl("HEADER001") == "H"

    def test_classify_header_lowercase(self):
        """Case-insensitive header detection."""
        assert _classify_record_type_impl("header001") == "H"

    def test_classify_trailer(self):
        """SSN starting with 'TRAILER' -> 'T'"""
        assert _classify_record_type_impl("TRAILER01") == "T"

    def test_classify_trailer_lowercase(self):
        """Case-insensitive trailer detection."""
        assert _classify_record_type_impl("trailer01") == "T"

    def test_classify_detail(self):
        """Numeric SSN -> 'D'"""
        assert _classify_record_type_impl("123456789") == "D"

    def test_classify_none(self):
        """None -> 'D' (default to detail)"""
        assert _classify_record_type_impl(None) == "D"

    def test_classify_empty(self):
        """Empty string -> 'D'"""
        assert _classify_record_type_impl("") == "D"

    def test_classify_partial_header(self):
        """'HEAD' without full 'HEADER' -> 'D'"""
        assert _classify_record_type_impl("HEAD1234") == "D"


class TestValidateAndConvertDate:
    """Tests for date validation and conversion (Pseudossn lines 795-802)."""

    def test_date_mmddyyyy_valid(self):
        """'01152024' in MMDDYYYY -> date(2024, 1, 15)"""
        result = _validate_and_convert_date_impl("01152024", "MMDDYYYY")
        assert result == date(2024, 1, 15)

    def test_date_yyyymmdd_valid(self):
        """'20240115' in YYYYMMDD -> date(2024, 1, 15)"""
        result = _validate_and_convert_date_impl("20240115", "YYYYMMDD")
        assert result == date(2024, 1, 15)

    def test_date_yyyyddmm_valid(self):
        """'20241501' in YYYYDDMM -> date(2024, 1, 15)"""
        result = _validate_and_convert_date_impl("20241501", "YYYYDDMM")
        assert result == date(2024, 1, 15)

    def test_date_invalid(self):
        """'99999999' -> None (invalid date)"""
        result = _validate_and_convert_date_impl("99999999", "YYYYMMDD")
        assert result is None

    def test_date_none(self):
        """None -> None"""
        result = _validate_and_convert_date_impl(None, "YYYYMMDD")
        assert result is None

    def test_date_empty(self):
        """Empty string -> None"""
        result = _validate_and_convert_date_impl("", "YYYYMMDD")
        assert result is None

    def test_date_short_string(self):
        """Short string -> None"""
        result = _validate_and_convert_date_impl("2024", "YYYYMMDD")
        assert result is None

    def test_date_unknown_format(self):
        """Unknown format -> None"""
        result = _validate_and_convert_date_impl("20240115", "DDMMYYYY")
        assert result is None

    def test_date_whitespace(self):
        """Whitespace-padded -> None (wrong length after strip)"""
        result = _validate_and_convert_date_impl("  20240115  ", "YYYYMMDD")
        assert result is None

    def test_date_leap_year(self):
        """Feb 29 on leap year should parse."""
        result = _validate_and_convert_date_impl("20240229", "YYYYMMDD")
        assert result == date(2024, 2, 29)

    def test_date_non_leap_feb29(self):
        """Feb 29 on non-leap year -> None."""
        result = _validate_and_convert_date_impl("20230229", "YYYYMMDD")
        assert result is None


class TestIsValidSSN:
    """Tests for SSN validation (Pseudossn line 724)."""

    def test_ssn_valid(self):
        """'123456789' -> True"""
        assert _is_valid_ssn_impl("123456789") is True

    def test_ssn_invalid_alpha(self):
        """'ABCDEFGHI' -> False"""
        assert _is_valid_ssn_impl("ABCDEFGHI") is False

    def test_ssn_empty(self):
        """'' -> False"""
        assert _is_valid_ssn_impl("") is False

    def test_ssn_none(self):
        """None -> False"""
        assert _is_valid_ssn_impl(None) is False

    def test_ssn_too_short(self):
        """'12345678' (8 digits) -> False"""
        assert _is_valid_ssn_impl("12345678") is False

    def test_ssn_too_long(self):
        """'1234567890' (10 digits) -> False"""
        assert _is_valid_ssn_impl("1234567890") is False

    def test_ssn_with_dashes(self):
        """'123-45-6789' -> False (not pure digits)"""
        assert _is_valid_ssn_impl("123-45-6789") is False

    def test_ssn_with_spaces(self):
        """'123 456 789' -> False"""
        assert _is_valid_ssn_impl("123 456 789") is False

    def test_ssn_all_zeros(self):
        """'000000000' -> True (9 digits)"""
        assert _is_valid_ssn_impl("000000000") is True

    def test_ssn_header_string(self):
        """'HEADER123' -> False"""
        assert _is_valid_ssn_impl("HEADER123") is False
