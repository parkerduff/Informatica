"""Benchmark each job against the local SQL Server with golden data.

No hard time gate (local mode is not representative of EMR); these capture
baselines for trending via ``--benchmark-json``.
"""
import datetime as dt

import pytest

pytestmark = pytest.mark.performance

RUN_DATE = dt.date(2026, 6, 11)


def test_benchmark_pay_calendar(benchmark, config, seeded, spark):
    from jobs import pay_calendar
    benchmark(lambda: pay_calendar.run(config, RUN_DATE))


def test_benchmark_comptime(benchmark, config, seeded, spark, golden_dir):
    from jobs import comptime
    path = str(golden_dir / "comptime_input.csv")
    benchmark(lambda: comptime.run(config, path))


def test_benchmark_cpm_nih(benchmark, config, seeded, spark):
    from jobs.cpm import cpm_common
    benchmark(lambda: cpm_common.run_agency("nih", config))
