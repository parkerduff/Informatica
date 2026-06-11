import datetime

from jobs.ehrp2biis import afterload, etl


class FakeCursor:
    def __init__(self, fetch=None):
        self.calls = []
        self.rowcount = 1
        self._fetch = fetch

    def execute(self, sql, *params):
        self.calls.append((" ".join(sql.split()), params))
        return self

    def fetchone(self):
        return self._fetch

    def nextset(self):
        return False


class FakeConn:
    def __init__(self, cursor):
        self._cursor = cursor
        self.commits = 0

    def cursor(self):
        return self._cursor

    def commit(self):
        self.commits += 1


def test_lookup_old_sequence_number():
    assert etl.lookup_old_sequence_number(FakeConn(FakeCursor(fetch=(1000,)))) == 1000
    assert etl.lookup_old_sequence_number(FakeConn(FakeCursor(fetch=(None,)))) == 0


def _joined(spark):
    ts = datetime.datetime(2026, 1, 15)
    return spark.createDataFrame(
        [("100000002", 0, ts, 1, "P", "GS", "U1"),
         ("100000001", 0, ts, 1, "P", "GS", "U1")],
        ["EMPLID", "EMPL_RCD", "EFFDT", "EFFSEQ", "GVT_WIP_STATUS", "PAYGROUP", "UNION_CD"],
    )


def test_build_target_primary(spark):
    from utils import schemas

    fields = schemas.get_table_fields("EHRP2BIIS_UPDATE", "NWK_ACTION_PRIMARY_TBL", "targets")
    out = etl.build_target(_joined(spark), fields, 1000, etl.PRIMARY_EXPLICIT)
    rows = sorted(out.collect(), key=lambda r: r["EVENT_ID"])
    assert [int(r["EVENT_ID"]) for r in rows] == [1001, 1002]
    # row_number ordered by EMPLID: smallest EMPLID gets 1001
    assert rows[0]["SSN"] == "100000001"
    assert rows[0]["GVT_WIP_STATUS"] == "P"
    assert str(rows[0]["EVENT_EFF_DTE"]).startswith("2026-01-15")
    assert rows[0]["LOAD_DATE"] is not None
    assert out.columns == [f["name"] for f in fields]


def test_build_target_secondary_unmapped_null(spark):
    from utils import schemas

    fields = schemas.get_table_fields("EHRP2BIIS_UPDATE", "NWK_ACTION_SECONDARY_TBL", "targets")
    out = etl.build_target(_joined(spark), fields, 1000)
    row = out.filter("EVENT_ID = 1001").collect()[0]
    assert row["PAYGROUP"] == "GS"
    assert row["RETND1_STEP_CD"] is None


def test_step04_clear_retained():
    cur = FakeCursor()
    afterload.step04_clear_retained(cur, datetime.date(2026, 6, 11))
    sql, params = cur.calls[0]
    assert "RETND1_STEP_CD = NULL" in sql
    assert "EVENT_ID < 9000000000" in sql
    assert params == (datetime.date(2026, 6, 11),)


def test_step05_run_procs():
    cur = FakeCursor()
    afterload.step05_run_procs(cur, datetime.date(2026, 6, 11))
    procs = [c[0] for c in cur.calls]
    assert len(procs) == len(afterload.PROCS_IN_ORDER)
    for proc, sql in zip(afterload.PROCS_IN_ORDER, procs):
        assert f"EXEC dbo.{proc}" in sql


def test_step05_process_table():
    cur = FakeCursor()
    afterload.step05_process_table(cur, datetime.date(2026, 6, 11))
    sqls = [c[0] for c in cur.calls]
    assert "SET P_STARTDT = NULL" in sqls[0]
    assert "SELECT TOP 1" in sqls[1]
    assert "DATEADD(DAY, 10000" in sqls[2]
    assert "chk_ehrp2biis_wip_status_p" in sqls[3]


def test_step05_refresh_all_tables():
    cur = FakeCursor()
    conn = FakeConn(cur)
    afterload.step05_refresh_all_tables(conn, datetime.date(2026, 6, 11))
    sqls = [c[0] for c in cur.calls]
    assert len(sqls) == 6  # delete+insert x 3 tables
    assert "DELETE FROM ACTION_REMARKS_ALL" in sqls[0]
    assert "INSERT INTO ACTION_SECONDARY_ALL" in sqls[5]


def test_afterload_run(monkeypatch):
    import contextlib

    cur = FakeCursor(fetch=(1,))
    conn = FakeConn(cur)

    @contextlib.contextmanager
    def fake_conn(*a, **k):
        assert k.get("autocommit") is False
        yield conn

    monkeypatch.setattr(afterload, "pyodbc_connection", fake_conn)
    sent = []
    monkeypatch.setattr(afterload, "send_notification", lambda s, b, c: sent.append(s))
    afterload.run("test", "2026-06-11")
    assert conn.commits == 4
    assert any("TRUNCATE TABLE NWK_NEW_EHRP_ACTIONS_TBL" in c[0] for c in cur.calls)
    assert any("completed" in s for s in sent)
