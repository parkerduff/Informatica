"""Unit tests for the PseudoSSN fixed-width parsing and dedup logic."""
import pytest

from jobs.pseudossn import (build_layout, deduplicate, parse_details,
                            record_width)

pytestmark = pytest.mark.unit


def test_build_layout_offsets_are_contiguous():
    layout = build_layout()
    # first detail field starts right after the 1-char record-type indicator
    assert layout[0][1] == 2
    pos = 2
    for _, start, width, _, _ in layout:
        assert start == pos
        pos += width
    assert record_width(layout) == pos - 1


def test_parse_details_reads_only_detail_records(spark, golden_dir):
    layout = build_layout()
    df = parse_details(spark, str(golden_dir / "pseudossn_input.dat"), layout)
    assert df.count() == 50
    assert "PSEUDOSSN" in df.columns
    # No header/trailer leaked in: every PSEUDOSSN is non-empty.
    assert df.filter("PSEUDOSSN IS NULL OR PSEUDOSSN = ''").count() == 0


def test_deduplicate_keeps_latest_per_pseudossn(spark, golden_dir):
    layout = build_layout()
    df = parse_details(spark, str(golden_dir / "pseudossn_input.dat"), layout)
    deduped = deduplicate(df)
    assert deduped.count() == df.select("PSEUDOSSN").distinct().count()
    # one row per pseudo SSN
    assert deduped.groupBy("PSEUDOSSN").count().filter("count > 1").count() == 0
