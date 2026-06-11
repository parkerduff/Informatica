"""Performance benchmarks vs recorded Informatica baselines.

Local Spark (``local[*]``) is slower per-record than the production cluster, so
the gate allows 2x the Informatica baseline.  Each job runs once (pedantic,
1 round) against the seeded test DB.
"""
import datetime as dt
import os

import pytest

from scripts import apply_ddl, seed_golden_data

# Informatica wall-clock baselines (seconds), manually recorded.
BASELINES = {
    "pay_calendar": 30,
    "comptime": 60,
    "pseudossn": 120,
    "fda_leave": 180,
    "ehrp2biis": 600,
    "cpm_nih": 900,
}
TOLERANCE = 2.0  # allow 2x baseline for local mode
RD = dt.date(2026, 6, 11)


@pytest.fixture(scope="module")
def perf_env(spark):
    from tests.conftest import GOLDEN_DIR
    from jobs import pay_calendar

    apply_ddl.apply("test")
    seed_golden_data.seed("test", GOLDEN_DIR)
    pay_calendar.run(env="test", run_date=RD, spark=spark)  # precondition for others
    return GOLDEN_DIR


def _runner(module, spark, golden):
    from jobs import comptime, fda_leave, pay_calendar, pseudossn
    from jobs.cpm import cpm_nih
    from jobs.ehrp2biis import afterload, etl, preload

    def pay_calendar_run():
        pay_calendar.run(env="test", run_date=RD, spark=spark)

    def comptime_run():
        comptime.run(env="test", file_path=os.path.join(golden, "comptime_input.csv"),
                     run_date="2026-06-11", spark=spark)

    def pseudossn_run():
        pseudossn.run(env="test", file_path=os.path.join(golden, "pseudossn_input.dat"),
                      run_date="2026-06-11", spark=spark)

    def fda_leave_run():
        fda_leave.run(env="test", run_date="2026-06-11", spark=spark)

    def ehrp2biis_run():
        preload.run(env="test", run_date=RD)
        etl.run(env="test", run_date=RD, spark=spark)
        afterload.run(env="test", run_date=RD)

    def cpm_nih_run():
        cpm_nih.run(env="test", run_date="2026-06-11", spark=spark)

    return {
        "pay_calendar": pay_calendar_run,
        "comptime": comptime_run,
        "pseudossn": pseudossn_run,
        "fda_leave": fda_leave_run,
        "ehrp2biis": ehrp2biis_run,
        "cpm_nih": cpm_nih_run,
    }[module]


@pytest.mark.parametrize("module,baseline", BASELINES.items())
def test_performance_within_baseline(perf_env, spark, benchmark, module, baseline):
    fn = _runner(module, spark, perf_env)
    benchmark.pedantic(fn, rounds=1, iterations=1)
    assert benchmark.stats.stats.mean < baseline * TOLERANCE, (
        f"{module} took {benchmark.stats.stats.mean:.1f}s, baseline {baseline}s "
        f"(allowed {baseline * TOLERANCE:.0f}s)")


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
