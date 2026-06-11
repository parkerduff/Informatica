"""Unit tests for the shared pure transformation helpers (jobs.common)."""
import hashlib

import pytest

from jobs.common import (build_record_text, hash_ssn, mmddyyyy_to_iso,
                         oracle_kind, parse_signed_decimal, pp_year_num,
                         scaled_cents, yyyymmdd_to_iso)

pytestmark = pytest.mark.unit


def test_oracle_kind_date():
    assert oracle_kind("date", None, None) == ("date", 19, 0)
    assert oracle_kind("timestamp", None, None) == ("date", 19, 0)


def test_oracle_kind_decimal_clamped():
    assert oracle_kind("number", "10", "2") == ("decimal", 10, 2)
    # precision floored to 1 and capped at 38
    assert oracle_kind("number", "0", "0") == ("decimal", 1, 0)
    assert oracle_kind("number", "99", "0") == ("decimal", 38, 0)
    # scale greater than precision bumps precision up
    assert oracle_kind("number", "2", "5") == ("decimal", 5, 5)


def test_oracle_kind_string_default():
    assert oracle_kind("varchar2", "30", "0") == ("string", 30, 0)
    assert oracle_kind("char", None, None) == ("string", 1, 0)


def test_hash_ssn_matches_sha256():
    assert hash_ssn("999000001") == hashlib.sha256(b"999000001").hexdigest()
    assert hash_ssn(None) is None


def test_pp_year_num_zero_pads():
    assert pp_year_num(2026, 3) == 202603
    assert pp_year_num(2026, 12) == 202612


def test_yyyymmdd_to_iso():
    assert yyyymmdd_to_iso("20260611") == "2026-06-11"
    assert yyyymmdd_to_iso("") is None
    assert yyyymmdd_to_iso("2026") is None
    assert yyyymmdd_to_iso("abcdefgh") is None


def test_mmddyyyy_to_iso():
    assert mmddyyyy_to_iso("06112026") == "2026-06-11"
    assert mmddyyyy_to_iso(None) is None
    assert mmddyyyy_to_iso("bad") is None


def test_parse_signed_decimal():
    assert parse_signed_decimal("12345", 2) == 123.45
    assert parse_signed_decimal("12345-", 2) == -123.45
    assert parse_signed_decimal("-12345", 2) == -123.45
    assert parse_signed_decimal("", 2) is None
    assert parse_signed_decimal(None, 2) is None
    assert parse_signed_decimal("12.3x", 2) is None


def test_scaled_cents():
    assert scaled_cents("1001.01") == 100101
    assert scaled_cents(0) == 0
    assert scaled_cents("") == 0
    assert scaled_cents(None) == 0


def test_build_record_text_layout():
    rec = build_record_text("999000001", "NIH", "1001.01", "950.00")
    assert rec == "999000001" + "NIH" + "00000100101" + "00000095000"
    assert len(rec) == 9 + 3 + 11 + 11
