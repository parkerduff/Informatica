from pyspark_etl.config.schemas import FixedField
from pyspark_etl.config.schemas.cpm_schemas import CPM_NEWPAY_TBL_COLUMNS
from pyspark_etl.config.schemas.oracle_tables import TABLE_COLUMNS
from pyspark_etl.config.schemas.pseudossn_schemas import PSEUDOSSN_FILE_TK_NUM


def test_fixedfield_spark_start_is_one_indexed():
    f = FixedField("X", offset=0, length=9)
    assert f.spark_start == 1


def test_pseudossn_known_offsets():
    by_name = {f.name: f for f in PSEUDOSSN_FILE_TK_NUM}
    assert by_name["SSN"].offset == 0 and by_name["SSN"].length == 9
    assert by_name["CAN_CD"].offset == 9 and by_name["CAN_CD"].length == 8
    assert by_name["PSEUDO_SSN"].offset == 17
    assert by_name["HIRE_DATE"].offset == 73
    assert by_name["UNIF_ALLOW_AMT"].offset == 94 and by_name["UNIF_ALLOW_AMT"].length == 6


def test_pseudossn_offsets_are_contiguous():
    # Each field starts where the previous ends (fixed-width, no gaps).
    fields = PSEUDOSSN_FILE_TK_NUM
    for prev, cur in zip(fields, fields[1:]):
        assert cur.offset == prev.offset + prev.length


def test_cpm_has_many_columns():
    assert len(CPM_NEWPAY_TBL_COLUMNS) > 400
    assert "PP_NUM" in CPM_NEWPAY_TBL_COLUMNS
    assert "BUSINESS_UNIT" in CPM_NEWPAY_TBL_COLUMNS


def test_target_table_columns_present():
    assert "PSEUDOSSN_FROM_SDA_TBL" in TABLE_COLUMNS
    assert "SSN" in TABLE_COLUMNS["PSEUDOSSN_FROM_SDA_TBL"]
    assert len(TABLE_COLUMNS["NWK_ACTION_PRIMARY_TBL"]) == 260
