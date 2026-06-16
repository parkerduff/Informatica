import datetime

import pytest

from pyspark_etl.transforms.date_conversions import (
    parse_date,
    parse_mmddyyyy,
    parse_yyyyddmm,
    parse_yyyymmdd,
)


def test_mmddyyyy_valid():
    assert parse_mmddyyyy("01312020") == datetime.date(2020, 1, 31)


def test_yyyymmdd_valid():
    assert parse_yyyymmdd("20200131") == datetime.date(2020, 1, 31)


def test_yyyyddmm_valid():
    # 2020, day=31, month=01
    assert parse_yyyyddmm("20203101") == datetime.date(2020, 1, 31)


@pytest.mark.parametrize("parser", [parse_mmddyyyy, parse_yyyymmdd, parse_yyyyddmm])
@pytest.mark.parametrize("bad", ["", "   ", None, "abcd1234", "1234", "00000000", "13312020"])
def test_invalid_returns_none(parser, bad):
    assert parser(bad) is None


def test_invalid_month_day():
    # month 13 is invalid in MMDDYYYY
    assert parse_mmddyyyy("13012020") is None
    # day 32 invalid in YYYYMMDD
    assert parse_yyyymmdd("20200132") is None


def test_whitespace_is_trimmed():
    assert parse_mmddyyyy(" 01312020 ") == datetime.date(2020, 1, 31)


def test_dispatch():
    assert parse_date("01312020", "MMDDYYYY") == datetime.date(2020, 1, 31)
    assert parse_date("20200131", "YYYYMMDD") == datetime.date(2020, 1, 31)
    with pytest.raises(ValueError):
        parse_date("20200131", "NOPE")
