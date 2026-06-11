from jobs.cpm import cpm_cdc, cpm_common, cpm_nih, cpm_oig


def test_newpay_columns():
    cols = cpm_common.newpay_columns()
    assert len(cols) == 501
    assert cols[:4] == ["PP_END_YEAR", "PP_NUM", "DFAS_PSEUDO_SSN", "LINE_TYPE"]


def test_agency_layouts_and_record_length():
    for module, definition in (
        ("CPM_NIH", "nihtest_NIH_PAYROLL_MASTER"),
        ("CPM_OIG", "oigsgndec_SKPAYROLL_MASTER"),
        ("CPM_CDC", "cdcskel_WS_PAY_OUT_REC"),
    ):
        layout = cpm_common.agency_layout(module, definition)
        assert layout
        assert cpm_common.record_length(layout) > 0
    assert cpm_common.record_length([]) == 0


def test_render_fixed_width_basic():
    layout = [{"name": "A", "offset": 0, "length": 5},
              {"name": "B", "offset": 5, "length": 3}]
    line = cpm_common.render_fixed_width({"A": "XY", "B": None}, layout)
    assert line == "XY      "
    assert len(line) == 8


def test_render_fixed_width_signed():
    layout = [{"name": "AMT", "offset": 0, "length": 8}]
    pos = cpm_common.render_fixed_width({"AMT": "12.50"}, layout, {"AMT": 2})
    assert pos == "0001250+"
    neg = cpm_common.render_fixed_width({"AMT": "-12.50"}, layout, {"AMT": 2})
    assert neg == "0001250-"


def test_render_fixed_width_truncates_long_values():
    layout = [{"name": "A", "offset": 0, "length": 3}]
    assert cpm_common.render_fixed_width({"A": "ABCDEF"}, layout) == "ABC"


def test_build_header_trailer():
    pp = {"pp_num": 12, "pp_end_year": 2026}
    assert cpm_common.build_header(pp, "NIH") == "HNIH 202612"
    assert cpm_common.build_trailer(5, "OIG") == "TOIG 000000005"


def test_layout_offsets_are_contiguous():
    layout = cpm_common.agency_layout("CPM_OIG", "oigsgndec_SKPAYROLL_MASTER")
    cursor = 0
    for f in layout:
        assert f["offset"] == cursor
        assert f["length"] > 0
        cursor += f["length"]


def test_agency_constants():
    assert cpm_nih.AGENCY == "NIH" and cpm_nih.OUTPUT_FILE == "cpm_nih_payroll.txt"
    assert cpm_oig.AGENCY == "OIG" and cpm_oig.STAGING_TABLE == "CPM_OIG_STG_TBL"
    assert cpm_cdc.AGENCY == "CDC" and cpm_cdc.MASTER_DEF == "cdcskel_WS_PAY_OUT_REC"


def test_load_staging_table(monkeypatch):
    import contextlib

    class FakeCursor:
        def __init__(self):
            self.calls = []
            self.fast_executemany = False

        def execute(self, sql, *p):
            self.calls.append(sql)

        def executemany(self, sql, rows):
            self.calls.append((sql, rows))

    class FakeConn:
        def __init__(self):
            self.cur = FakeCursor()

        def cursor(self):
            return self.cur

        def commit(self):
            pass

    conn = FakeConn()

    @contextlib.contextmanager
    def fake_conn(*a, **k):
        yield conn

    monkeypatch.setattr(cpm_common, "pyodbc_connection", fake_conn)
    n = cpm_common.load_staging_table({}, {}, "CPM_NIH_STG_TBL",
                                      ["H123", "D456", "T789"], ["H", "D", "T"])
    assert n == 3
    assert "DELETE FROM [dbo].[CPM_NIH_STG_TBL]" in conn.cur.calls[0]
    sql, rows = conn.cur.calls[1]
    assert rows == [(1, "H", "H123"), (2, "D", "D456"), (3, "T", "T789")]
