"""Unit tests for jobs/pseudossn.py."""
import pytest

from jobs import pseudossn


def test_signed_decimal_conversion_negative():
    # "1234}" -> -12340 (trailing '}' is digit 0, negative sign)
    assert pseudossn.parse_overpunch("1234}") == -12340


def test_signed_decimal_conversion_positive():
    assert pseudossn.parse_overpunch("1234{") == 12340
    assert pseudossn.parse_overpunch("123A") == 1231  # 'A' -> 1, positive


def test_overpunch_plain_digits():
    assert pseudossn.parse_overpunch("00500") == 500


def test_overpunch_invalid_char_raises():
    with pytest.raises(ValueError):
        pseudossn.parse_overpunch("123*")


def test_date_mmddyyyy_conversion():
    assert pseudossn.parse_date_mmddyyyy("06112026") == "2026-06-11"


def test_date_blank_returns_none():
    assert pseudossn.parse_date_mmddyyyy("        ") is None
    assert pseudossn.parse_date_mmddyyyy("00000000") is None


def _raw_df(spark, lines):
    return spark.createDataFrame([(line,) for line in lines], ["value"])


def _detail(rt="D", pseudo="900111222", real="900999888",
            last="SMITH", first="ALEX", eff="06112026", amt="0001000{"):
    return (rt + pseudo.ljust(9) + real.ljust(9) + last.ljust(30)
            + first.ljust(20) + eff.ljust(8) + amt.rjust(11, "0"))


def test_header_trailer_filtered(spark, tmp_path):
    dat = tmp_path / "in.dat"
    dat.write_text("H" + "x" * 87 + "\n" + _detail() + "\n" + "T" + "x" * 87 + "\n")
    raw = pseudossn.parse_fixed_width(spark, str(dat))
    details = pseudossn.filter_details(raw)
    assert details.count() == 1
    assert details.collect()[0]["RECORD_TYPE"] == "D"


def test_fixed_width_parsing_offsets(spark, tmp_path):
    dat = tmp_path / "in.dat"
    dat.write_text(_detail(pseudo="901234567") + "\n")
    raw = pseudossn.parse_fixed_width(spark, str(dat))
    row = raw.collect()[0]
    assert row["PSEUDO_SSN"] == "901234567"
    assert row["LAST_NAME"] == "SMITH"


def test_dedup_keeps_latest_effective_date(spark, tmp_path):
    dat = tmp_path / "in.dat"
    dat.write_text(
        _detail(pseudo="900111222", eff="01012020") + "\n"
        + _detail(pseudo="900111222", eff="06112026") + "\n")
    raw = pseudossn.parse_fixed_width(spark, str(dat))
    converted = pseudossn.convert(pseudossn.filter_details(raw))
    deduped = pseudossn.dedup_latest(converted).collect()
    assert len(deduped) == 1
    assert deduped[0]["EFFECTIVE_DATE"] == "2026-06-11"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
