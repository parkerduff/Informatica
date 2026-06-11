"""Functional checks against the live Docker SQL Server after `make run-all`."""
import os

import pytest

pytestmark = pytest.mark.usefixtures("seed_tables")


def _scalar(conn, sql, *params):
    cur = conn.cursor()
    cur.execute(sql, *params)
    return cur.fetchone()[0]


def test_exactly_one_current_pay_period(db_connection):
    n = _scalar(db_connection,
                "SELECT COUNT(*) FROM PAY_PERIOD WHERE CURR_PP_FLAG = 'Y'")
    assert n == 1


def test_comptime_loaded(db_connection):
    assert _scalar(db_connection, "SELECT COUNT(*) FROM COMP_TIME_DAILY_TBL") == 100
    n = _scalar(db_connection,
                "SELECT COUNT(*) FROM COMP_TIME_DAILY_TBL WHERE SSN_HASH IS NULL")
    assert n == 0


def test_counter_rows_logged(db_connection):
    for proc in ("COMPTIME", "PSEUDOSSN", "FDA_LEAVE"):
        n = _scalar(db_connection,
                    "SELECT COUNT(*) FROM COUNTER_TBL WHERE PROCESS_NAME = ?", proc)
        assert n >= 1, f"no COUNTER_TBL row for {proc}"


def test_pseudossn_loaded_and_deduped(db_connection):
    n_all = _scalar(db_connection, "SELECT COUNT(*) FROM PSEUDOSSN_FROM_SDA_TBL")
    n_dedup = _scalar(db_connection, "SELECT COUNT(*) FROM PSEUDOSSN_TBL")
    assert n_all == 50
    assert n_dedup == 45
    dup = _scalar(db_connection,
                  "SELECT COUNT(*) FROM (SELECT PSEUDOSSN FROM PSEUDOSSN_TBL "
                  "GROUP BY PSEUDOSSN HAVING COUNT(*) > 1) d")
    assert dup == 0


def test_fda_errors(db_connection):
    n = _scalar(db_connection,
                "SELECT COUNT(*) FROM ERROR_TBL WHERE PROCESS_NAME = 'FDA_LEAVE'")
    assert n == 5


def test_ehrp_action_all_tables(db_connection):
    n_prim = _scalar(db_connection, "SELECT COUNT(*) FROM ACTION_PRIMARY_ALL")
    n_sec = _scalar(db_connection, "SELECT COUNT(*) FROM ACTION_SECONDARY_ALL")
    assert n_prim == 10 and n_sec == 10
    staged = _scalar(db_connection, "SELECT COUNT(*) FROM NWK_NEW_EHRP_ACTIONS_TBL")
    assert staged == 0  # truncated by afterload


def test_cpm_staging_tables(db_connection):
    for table in ("CPM_NIH_STG_TBL", "CPM_OIG_STG_TBL", "CPM_CDC_STG_TBL"):
        n = _scalar(db_connection, f"SELECT COUNT(*) FROM {table}")
        assert n == 7, f"{table} expected 7 rows (H + 5 D + T), got {n}"
        h = _scalar(db_connection,
                    f"SELECT COUNT(*) FROM {table} WHERE RECORD_TYPE = 'H'")
        t = _scalar(db_connection,
                    f"SELECT COUNT(*) FROM {table} WHERE RECORD_TYPE = 'T'")
        assert h == 1 and t == 1


def test_cpm_output_files_exist(config):
    staging = config["paths"]["staging"]
    for fname in ("cpm_nih_payroll.txt", "cpm_oig_payroll.txt", "cpm_cdc_payroll.txt"):
        path = os.path.join(staging, fname)
        assert os.path.exists(path), f"missing {path}"
        lines = open(path).read().splitlines()
        assert lines[0].startswith("H") and lines[-1].startswith("T")


def test_stored_procs_exist(db_connection):
    procs = ["update_sequence_number_tbl_p", "updt_erp2biis_cre8_remarks01_p",
             "update_erp2biis_no900s01_p", "erp2biis_cre8_remarks_900s01",
             "update_erp2biis_900sonly01_p", "updt_orig_cancelled_trans01_p",
             "chk_ehrp2biis_wip_status_p"]
    for proc in procs:
        n = _scalar(db_connection,
                    "SELECT COUNT(*) FROM sys.procedures WHERE name = ?", proc)
        assert n == 1, f"missing stored procedure {proc}"
