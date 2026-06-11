from jobs import fda_leave


def _dfs(spark):
    tatran = spark.createDataFrame(
        [("B1", "TK1", "E001", "2026", "12", "L", "1", "D1"),
         ("B1", "TK2", "E002", "2026", "12", "L", "2", "D2"),
         ("B1", "TK3", "E003", "2026", "12", "L", "3", "D3")],
        fda_leave.TATRAN_COLUMNS,
    )
    full = spark.createDataFrame(
        [("E001", "2026", "12"), ("E002", "2026", "12"), ("E003", "2026", "12")],
        fda_leave.STG_COLUMNS,
    )
    partial = spark.createDataFrame(
        [("E001", "2026", "12"), ("E002", "2026", "12")], fda_leave.STG_COLUMNS)
    return tatran, full, partial


def test_build_errors_none(spark):
    tatran, full, _ = _dfs(spark)
    errors = fda_leave.build_errors(tatran, full, full, full)
    assert errors.count() == 0


def test_build_errors_missing_one_table(spark):
    tatran, full, partial = _dfs(spark)
    errors = fda_leave.build_errors(tatran, partial, full, full).collect()
    assert len(errors) == 1
    row = errors[0]
    assert row["ERROR_MESSAGE"] == "Missing CPM YTD detail record"
    assert row["SOURCE_KEY"] == "E003|2026|12"


def test_build_errors_missing_all_tables(spark):
    tatran, full, partial = _dfs(spark)
    errors = fda_leave.build_errors(tatran, partial, partial, partial).collect()
    assert len(errors) == 1
    assert errors[0]["ERROR_MESSAGE"] == (
        "Missing CPM YTD detail record; Missing CPM PAD detail record; "
        "Missing CPM MER detail record")


def test_run(monkeypatch, spark):
    tatran, full, partial = _dfs(spark)
    tables = {
        "HI_PM_FDA_TATRAN_TBL": tatran,
        "CPM_YTD_DETAIL_STG_TBL": partial,
        "CPM_PAD_DETAIL_STG_TBL": full,
        "CPM_MER_DETAIL_STG_TBL": full,
    }
    monkeypatch.setattr(fda_leave, "get_spark", lambda name: spark)
    monkeypatch.setattr(spark, "stop", lambda: None)
    monkeypatch.setattr(fda_leave, "get_current_pay_period",
                        lambda *a: {"pp_num": 12, "pp_end_year": 2026})
    monkeypatch.setattr(fda_leave, "read_table",
                        lambda sp, table, cfg, sec, columns=None: tables[table])
    written = []
    monkeypatch.setattr(fda_leave, "write_table",
                        lambda df, table, cfg, sec, mode: written.append((table, df.count())))
    logged = []
    monkeypatch.setattr(fda_leave, "log_row_count",
                        lambda conn, t, p, n, pp: logged.append(n))

    import contextlib

    class FakeConn:
        def commit(self):
            pass

    @contextlib.contextmanager
    def fake_conn(*a, **k):
        yield FakeConn()

    monkeypatch.setattr(fda_leave, "pyodbc_connection", fake_conn)
    monkeypatch.setattr(fda_leave, "send_notification", lambda *a: None)
    n = fda_leave.run("test")
    assert n == 1
    assert written == [("ERROR_TBL", 1)]
    assert logged == [1]
