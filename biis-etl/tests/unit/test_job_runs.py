"""run() level tests with DB access stubbed out (coverage of orchestration paths)."""
import contextlib
import datetime
from decimal import Decimal

from jobs import comptime
from jobs.cpm import cpm_cdc, cpm_common, cpm_nih, cpm_oig
from jobs.ehrp2biis import etl, preload


class FakeConn:
    def commit(self):
        pass


@contextlib.contextmanager
def fake_conn(*a, **k):
    yield FakeConn()


def test_comptime_run(monkeypatch, spark, tmp_path):
    path = tmp_path / "comptime.csv"
    path.write_text(
        "SSN,NAME,CURRENT_ACCT,CURRENT_ORG,FLSA_STATUS,COMP_TIME_CUR_BAL,"
        "COMP_TIME_YEAR_EARNED,PP_END_DATE,DAILY_DATE_EARNED,COMP_TIME_RATE,"
        "COMP_TIME_HOURS,COMP_TIME_UNDEF\n"
        "999000001,EMP,A,O,N,12.50,2026,2026-06-13,2026-06-01,1.50,4.25,3\n")
    monkeypatch.setattr(comptime, "get_spark", lambda name: spark)
    monkeypatch.setattr(spark, "stop", lambda: None)
    monkeypatch.setattr(comptime, "get_current_pay_period",
                        lambda *a: {"pp_num": 12, "pp_end_year": 2026})
    written = []
    monkeypatch.setattr(comptime, "write_table",
                        lambda df, t, c, s, mode: written.append(t))
    monkeypatch.setattr(comptime, "pyodbc_connection", fake_conn)
    monkeypatch.setattr(comptime, "log_row_count", lambda *a: None)
    monkeypatch.setattr(comptime, "send_notification", lambda *a: None)
    assert comptime.run("test", str(path)) == 1
    assert written == ["COMP_TIME_DAILY_TBL"]


def test_etl_run(monkeypatch, spark):
    from utils import schemas

    ts = datetime.datetime(2026, 1, 15)
    actions = spark.createDataFrame(
        [("100000001", Decimal(0), ts, Decimal(1))],
        "EMPLID string, EMPL_RCD decimal(38,0), EFFDT timestamp, EFFSEQ decimal(38,0)")
    job_fields = schemas.get_table_fields("EHRP2BIIS_UPDATE", "PS_GVT_JOB")
    job_cols = [f["name"] for f in job_fields]
    ddl = ", ".join(f"`{f['name']}` {schemas.spark_type_for(f)}" for f in job_fields)
    jrow = {c: None for c in job_cols}
    jrow.update({"EMPLID": "100000001", "EMPL_RCD": Decimal(0), "EFFDT": ts,
                 "EFFSEQ": Decimal(1), "GVT_WIP_STATUS": "P"})
    ps_gvt_job = spark.createDataFrame([tuple(jrow[c] for c in job_cols)], schema=ddl)
    tables = {"NWK_NEW_EHRP_ACTIONS_TBL": actions, "PS_GVT_JOB": ps_gvt_job}

    monkeypatch.setattr(etl, "get_spark", lambda name: spark)
    monkeypatch.setattr(spark, "stop", lambda: None)
    monkeypatch.setattr(etl, "read_table",
                        lambda sp, t, c, s, columns=None: tables[t])

    class SeqConn(FakeConn):
        def cursor(self):
            class Cur:
                def execute(self, sql):
                    return self

                def fetchone(self):
                    return (1000,)
            return Cur()

    @contextlib.contextmanager
    def seq_conn(*a, **k):
        yield SeqConn()

    monkeypatch.setattr(etl, "pyodbc_connection", seq_conn)
    written = []
    monkeypatch.setattr(etl, "write_table",
                        lambda df, t, c, s, mode: written.append((t, df.count())))
    monkeypatch.setattr(etl, "send_notification", lambda *a: None)
    result = etl.run("test")
    assert result == {"events": 1}
    assert [w[0] for w in written] == [
        "NWK_ACTION_PRIMARY_TBL", "NWK_ACTION_SECONDARY_TBL", "EHRP_RECS_TRACKING_TBL"]


def test_preload_run(monkeypatch):
    class Cur:
        def execute(self, sql):
            return self

        def fetchone(self):
            return (10,)

    class Conn(FakeConn):
        def cursor(self):
            return Cur()

    @contextlib.contextmanager
    def conn(*a, **k):
        yield Conn()

    monkeypatch.setattr(preload, "pyodbc_connection", conn)
    sent = []
    monkeypatch.setattr(preload, "send_notification", lambda s, b, c: sent.append(s))
    preload.run("test")
    assert sent


def _run_cpm(monkeypatch, spark, tmp_path, module):
    df = spark.createDataFrame(
        [(2026, 12, "600000001", "1"), (2026, 12, "600000002", "1"), (2025, 1, "600000003", "1")],
        ["PP_END_YEAR", "PP_NUM", "DFAS_PSEUDO_SSN", "LINE_TYPE"],
    )
    monkeypatch.setattr(module, "get_spark", lambda name: spark)
    monkeypatch.setattr(spark, "stop", lambda: None)
    monkeypatch.setattr(cpm_common, "get_pay_period",
                        lambda *a: {"pp_num": 12, "pp_end_year": 2026})
    monkeypatch.setattr(cpm_common, "read_newpay", lambda *a, **k: df)
    staged = []
    monkeypatch.setattr(cpm_common, "load_staging_table",
                        lambda c, s, t, lines, types: staged.append((t, len(lines))) or len(lines))
    monkeypatch.setattr(module, "send_notification", lambda *a: None)
    monkeypatch.setenv("BIIS_ENV", "test")
    monkeypatch.setattr(module, "load_config", lambda env: {
        "paths": {"staging": str(tmp_path)},
        "database": {"host": "h", "port": 1, "name": "d", "user": "u",
                     "password": "p", "driver": "x"},
        "notifications": {"provider": "log"},
    })
    n = module.run("test")
    assert n == 2  # only current pay period rows
    assert staged == [(module.STAGING_TABLE, 4)]  # H + 2 D + T
    out = (tmp_path / module.OUTPUT_FILE).read_text().splitlines()
    assert out[0].startswith(f"H{module.AGENCY}")
    assert out[-1].startswith(f"T{module.AGENCY}")
    assert len(out) == 4


def test_cpm_nih_run(monkeypatch, spark, tmp_path):
    _run_cpm(monkeypatch, spark, tmp_path, cpm_nih)


def test_cpm_oig_run(monkeypatch, spark, tmp_path):
    _run_cpm(monkeypatch, spark, tmp_path, cpm_oig)


def test_cpm_cdc_run(monkeypatch, spark, tmp_path):
    _run_cpm(monkeypatch, spark, tmp_path, cpm_cdc)


def test_spark_common_importable():
    from jobs import spark_common

    assert callable(spark_common.get_spark)


def test_mains(monkeypatch):
    from jobs import fda_leave, pay_calendar, pseudossn
    from jobs.ehrp2biis import afterload, etl as ehrp_etl, preload

    for mod, argv in (
        (comptime, ["x", "--env", "test", "--file-path", "/tmp/f.csv"]),
        (pseudossn, ["x", "--env", "test", "--file-path", "/tmp/f.dat"]),
        (fda_leave, ["x", "--env", "test"]),
        (pay_calendar, ["x", "--env", "test"]),
        (preload, ["x", "--env", "test"]),
        (ehrp_etl, ["x", "--env", "test"]),
        (afterload, ["x", "--env", "test"]),
        (cpm_nih, ["x", "--env", "test"]),
        (cpm_oig, ["x", "--env", "test"]),
        (cpm_cdc, ["x", "--env", "test"]),
    ):
        called = []
        monkeypatch.setattr(mod, "run", lambda *a, **k: called.append(1))
        monkeypatch.setattr("sys.argv", argv)
        assert mod.main() == 0
        assert called == [1]
