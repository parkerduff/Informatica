"""Performance baselines via pytest-benchmark. Informational only; no hard gates."""
import hashlib

import pytest

pytest.importorskip("pytest_benchmark")

from jobs.cpm import cpm_common  # noqa: E402


def test_render_fixed_width_benchmark(benchmark):
    layout = cpm_common.agency_layout("CPM_NIH", "nihtest_NIH_PAYROLL_MASTER")
    row = {f["name"]: "1" for f in layout}
    line = benchmark(cpm_common.render_fixed_width, row, layout)
    assert len(line) == cpm_common.record_length(layout)


def test_sha256_hash_benchmark(benchmark):
    def hash_batch():
        return [hashlib.sha256(f"{999000001 + i}".encode()).hexdigest()
                for i in range(1000)]

    result = benchmark(hash_batch)
    assert len(result) == 1000


def test_header_trailer_benchmark(benchmark):
    pp = {"pp_num": 12, "pp_end_year": 2026}

    def build():
        return (cpm_common.build_header(pp, "NIH"),
                cpm_common.build_trailer(12345, "NIH"))

    h, t = benchmark(build)
    assert h.startswith("H") and t.startswith("T")


def test_schema_lookup_benchmark(benchmark):
    from utils import schemas

    fields = benchmark(schemas.get_table_fields, "CPM_NIH", "CPM_NEWPAY_TBL", "sources")
    assert len(fields) == 501
