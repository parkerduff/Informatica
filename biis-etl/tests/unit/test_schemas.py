from utils import schemas


def test_all_modules_load():
    mods = schemas.all_modules()
    for name in ["Pay_Calendar", "COMPTIME", "Pseudossn", "FDA_Leave",
                 "EHRP2BIIS_UPDATE", "CPM_NIH", "CPM_OIG", "CPM_CDC"]:
        assert name in mods


def test_get_table_fields():
    fields = schemas.get_table_fields("Pay_Calendar", "PAY_PERIOD", "targets")
    names = [f["name"] for f in fields]
    assert "PP_NUM" in names and "CURR_PP_FLAG" in names


def test_oracle_type_to_sqlserver():
    assert schemas.oracle_type_to_sqlserver("number(p,s)", "10", "2") == "DECIMAL(10,2)"
    assert schemas.oracle_type_to_sqlserver("number", "50", "0") == "DECIMAL(38,0)"
    assert schemas.oracle_type_to_sqlserver("varchar2", "100", "0") == "NVARCHAR(100)"
    assert schemas.oracle_type_to_sqlserver("varchar2", "9999", "0") == "NVARCHAR(4000)"
    assert schemas.oracle_type_to_sqlserver("char", "5", None) == "NCHAR(5)"
    assert schemas.oracle_type_to_sqlserver("date", "19", "0") == "DATETIME2"
    assert schemas.oracle_type_to_sqlserver("weird", None, None) == "NVARCHAR(255)"


def test_sqlserver_column_def():
    d = schemas.sqlserver_column_def(
        {"name": "X", "datatype": "varchar2", "precision": "10", "scale": "0",
         "nullable": "NOTNULL"})
    assert d == "[X] NVARCHAR(10) NOT NULL"


def test_spark_type_for():
    assert schemas.spark_type_for({"datatype": "number(p,s)", "precision": "8", "scale": "2"}) == "decimal(8,2)"
    assert schemas.spark_type_for({"datatype": "date"}) == "timestamp"
    assert schemas.spark_type_for({"datatype": "varchar2"}) == "string"


def test_fixed_width_layout_offsets():
    layout = schemas.fixed_width_layout("Pseudossn", "PSEUDOSSN_FILE")
    by_name = {f["name"]: f for f in layout}
    assert by_name["SSN"]["offset"] == 0 and by_name["SSN"]["length"] == 9
    assert by_name["PSEUDO_SSN"]["offset"] == 17


def test_column_names():
    cols = schemas.column_names("Pseudossn", "PSEUDOSSN_FROM_SDA_TBL", "targets")
    assert cols[-2:] == ["PP_NUM", "PP_END_YEAR"] or "PP_NUM" in cols
    assert len(cols) == 63
