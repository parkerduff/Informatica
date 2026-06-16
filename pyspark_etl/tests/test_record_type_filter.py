from pyspark_etl.transforms.record_type_filter import (
    is_detail,
    is_number,
    record_type_flag,
)


def test_header_detection():
    assert record_type_flag("HEADER12") == "H"


def test_trailer_detection():
    assert record_type_flag("TRAILER9") == "T"


def test_detail_detection():
    assert record_type_flag("123456789") == "D"


def test_blank_is_reject():
    assert record_type_flag("   ") == "R"
    assert record_type_flag("") == "R"


def test_is_number():
    assert is_number("123456789")
    assert is_number(" 42 ")
    assert not is_number("12A45")
    assert not is_number("")
    assert not is_number(None)


def test_is_detail_requires_numeric_ssn():
    assert is_detail("123456789")
    assert not is_detail("HEADER12")
    assert not is_detail("TRAILER9")
    assert not is_detail("ABCDEFGHI")
    assert not is_detail("   ")
