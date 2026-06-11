"""Generate PII-safe synthetic golden datasets for the BIIS ETL harness.

Produces, under ``tests/fixtures/golden/``:

* ``<module>_input_*.csv`` / ``*.dat`` - source data the jobs read.
* ``<module>_expected_*.csv``         - expected target output, produced by
  *bootstrapping*: a throwaway sqlite DB is created, the inputs are loaded, the
  real PySpark jobs are run, and the resulting tables are dumped.

This guarantees the committed golden output is exactly what the current jobs
produce, so the regression suite then guards against future logic drift.  Once
production captures exist (see ``capture_golden_data.py``) they replace these.

All identifiers are fake: SSNs start 900-999, names are synthetic.
"""
from __future__ import annotations

import datetime as dt
import os
import sys
import tempfile

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd  # noqa: E402

from scripts._common import GOLDEN_DIR, dump_table_to_csv  # noqa: E402
from utils.schemas import column_names  # noqa: E402

RUN_DATE = "2026-06-11"
FAKE_NAMES = ["SMITH", "JOHNSON", "WILLIAMS", "BROWN", "JONES", "GARCIA", "MILLER",
              "DAVIS", "RODRIGUEZ", "MARTINEZ", "HERNANDEZ", "LOPEZ", "WILSON"]
FIRST_NAMES = ["ALEX", "JORDAN", "TAYLOR", "MORGAN", "CASEY", "RILEY", "JAMIE"]


def fake_ssn(i: int) -> str:
    return f"9{i % 100:02d}{(i * 7) % 1000:03d}{(i * 13) % 100:02d}"[:9].ljust(9, "0")


# --------------------------------------------------------------------------- #
# Input builders
# --------------------------------------------------------------------------- #
def build_pay_period() -> pd.DataFrame:
    start = dt.date(2026, 1, 4)
    rows = []
    for i in range(26):
        s = start + dt.timedelta(days=14 * i)
        e = s + dt.timedelta(days=13)
        rows.append({
            "PP_NUM": i + 1, "PP_END_YEAR": 2026,
            "PP_START_DTE": s.isoformat(), "PP_END_DTE": e.isoformat(),
            "LV_NUM": i + 1, "LV_YEAR": 2026,
            "PAY_DTE": (e + dt.timedelta(days=5)).isoformat(),
            "CURR_PP_FLAG": "", "HOLIDAY_1": "", "HOLIDAY_2": "",
        })
    return pd.DataFrame(rows)


def build_comptime() -> pd.DataFrame:
    rows = []
    for i in range(100):
        rows.append({
            "SSN": fake_ssn(i),
            "NAME": f"{FAKE_NAMES[i % len(FAKE_NAMES)]},{FIRST_NAMES[i % len(FIRST_NAMES)]}",
            "CURRENT_ACCT": f"{100000 + i}",
            "CURRENT_ORG": f"ORG{i % 9}",
            "FLSA_STATUS": "E" if i % 2 else "N",
            "COMP_TIME_CUR_BAL": f"{(i * 1.25) % 80:.2f}",
            "COMP_TIME_YEAR_EARNED": "2026",
            "PP_END_DATE": "2026-06-13",
            "DAILY_DATE_EARNED": f"2026-06-{(i % 13) + 1:02d}",
            "COMP_TIME_RATE": "1.50",
            "COMP_TIME_HOURS": f"{(i % 8) + 1:.2f}",
            "COMP_TIME_UNDEF": "0",
        })
    return pd.DataFrame(rows)


def build_pseudossn_dat() -> str:
    """Return the fixed-width .dat content (H + 50 D + T)."""
    def detail(pseudo, real, last, first, eff_mmddyyyy, amount_overpunch):
        return ("D"
                + pseudo.ljust(9)[:9]
                + real.ljust(9)[:9]
                + last.ljust(30)[:30]
                + first.ljust(20)[:20]
                + eff_mmddyyyy.ljust(8)[:8]
                + amount_overpunch.rjust(11, "0")[:11])

    lines = ["H" + "PSEUDOSSN FEED".ljust(87)]
    overpunch = ["A", "B", "C", "{", "}", "J", "0", "5", "9"]
    for i in range(50):
        pseudo = fake_ssn(i)
        # duplicate every 10th pseudo with an earlier effective date (dedup test)
        if i >= 45:
            pseudo = fake_ssn(i - 45)
            eff = f"0101202{i % 5}"
        else:
            eff = f"06{(i % 12) + 1:02d}2026"
        op = overpunch[i % len(overpunch)]
        amount = f"{1000 + i}{op}"
        lines.append(detail(pseudo, fake_ssn(i + 500),
                            FAKE_NAMES[i % len(FAKE_NAMES)],
                            FIRST_NAMES[i % len(FIRST_NAMES)], eff, amount))
    lines.append("T" + f"{50:08d}".ljust(87))
    return "\n".join(lines) + "\n"


def build_fda_inputs():
    tatran, ytd, pad, mer = [], [], [], []
    for i in range(30):
        emp = fake_ssn(i)
        tatran.append({"FDA_BATCH_ID": "B2026PP13", "FDA_TK_NO": f"TK{i:04d}",
                       "FDA_EMP_ID": emp, "PP_END_YEAR": 2026, "PP_NUM": 13,
                       "LEAVE_HOURS": f"{(i % 8) + 1:.2f}", "LEAVE_TYPE": "SICK"})
        detail = {"FDA_EMP_ID": emp, "PP_END_YEAR": 2026, "PP_NUM": 13,
                  "DETAIL_AMT": f"{i * 10:.2f}"}
        # employees 0-19 have all three; 20-29 each miss exactly one
        if i < 20:
            ytd.append(detail)
            pad.append(detail)
            mer.append(detail)
        else:
            miss = i % 3  # 0 -> miss ytd, 1 -> miss pad, 2 -> miss mer
            if miss != 0:
                ytd.append(detail)
            if miss != 1:
                pad.append(detail)
            if miss != 2:
                mer.append(detail)
    return (pd.DataFrame(tatran), pd.DataFrame(ytd), pd.DataFrame(pad), pd.DataFrame(mer))


def build_ehrp_inputs():
    actions, gvt = [], []
    for i in range(50):
        emplid = f"E{100000 + i}"
        rec = {"EMPLID": emplid, "EMPL_RCD": 0,
               "EFFDT": f"2026-06-{(i % 13) + 1:02d}", "EFFSEQ": 0}
        actions.append(rec)
        gvt_row = {c: "" for c in column_names("PS_GVT_JOB")}
        gvt_row.update(rec)
        gvt_row["GVT_WIP_STATUS"] = "P"
        gvt.append(gvt_row)
    # add 10 non-matching gvt rows
    for i in range(50, 60):
        gvt_row = {c: "" for c in column_names("PS_GVT_JOB")}
        gvt_row.update({"EMPLID": f"E{200000 + i}", "EMPL_RCD": 0,
                        "EFFDT": "2026-01-01", "EFFSEQ": 0, "GVT_WIP_STATUS": "C"})
        gvt.append(gvt_row)
    seq = pd.DataFrame([{"SEQ_NAME": "EVENT_ID", "SEQ_VALUE": 1000}])
    return pd.DataFrame(actions), pd.DataFrame(gvt), seq


def build_cpm_newpay() -> pd.DataFrame:
    cols = column_names("CPM_NEWPAY_TBL")
    rows = []
    plan = ([("NIH00", "")] * 60 + [("OIG00", "")] * 40
            + [("CDC00", "")] * 30 + [("ATSDR", "")] * 10
            + [("ZZZ00", "ANC34")] * 10 + [("ZZZ00", "OTHER")] * 50)
    for i, (bu, org) in enumerate(plan):
        row = {c: 0 for c in cols}
        for c in cols:
            row[c] = 0
        row["PP_END_YEAR"] = 2026
        row["PP_NUM"] = 26
        row["DFAS_PSEUDO_SSN"] = fake_ssn(i)
        row["BUSINESS_UNIT"] = bu
        row["ORG_CDE"] = org
        row["MP_POOL_DES"] = ""
        row["SOC_SEC_NO"] = fake_ssn(i + 900)
        rows.append(row)
    return pd.DataFrame(rows)[cols]


# --------------------------------------------------------------------------- #
# Orchestration
# --------------------------------------------------------------------------- #
def write_inputs():
    os.makedirs(GOLDEN_DIR, exist_ok=True)
    g = GOLDEN_DIR

    build_pay_period().to_csv(os.path.join(g, "pay_calendar_input_pay_period.csv"), index=False)
    build_comptime().to_csv(os.path.join(g, "comptime_input.csv"), index=False)
    with open(os.path.join(g, "pseudossn_input.dat"), "w") as fh:
        fh.write(build_pseudossn_dat())

    tatran, ytd, pad, mer = build_fda_inputs()
    tatran.to_csv(os.path.join(g, "fda_leave_input_hi_pm_fda_tatran_tbl.csv"), index=False)
    ytd.to_csv(os.path.join(g, "fda_leave_input_cpm_ytd_detail_stg_tbl.csv"), index=False)
    pad.to_csv(os.path.join(g, "fda_leave_input_cpm_pad_detail_stg_tbl.csv"), index=False)
    mer.to_csv(os.path.join(g, "fda_leave_input_cpm_mer_detail_stg_tbl.csv"), index=False)

    actions, gvt, seq = build_ehrp_inputs()
    actions.to_csv(os.path.join(g, "ehrp2biis_input_nwk_new_ehrp_actions_tbl.csv"), index=False)
    gvt.to_csv(os.path.join(g, "ehrp2biis_input_ps_gvt_job.csv"), index=False)
    seq.to_csv(os.path.join(g, "ehrp2biis_input_sequence_num_tbl.csv"), index=False)

    build_cpm_newpay().to_csv(os.path.join(g, "cpm_input_cpm_newpay_tbl.csv"), index=False)
    print(f"Wrote input fixtures to {g}")


def bootstrap_expected():
    """Run the full pipeline against a throwaway DB and dump expected outputs."""
    tmp = tempfile.mkdtemp(prefix="biis-golden-")
    os.environ["BIIS_TEST_ROOT"] = tmp

    # imports happen after BIIS_TEST_ROOT is set so config points at the temp DB
    from scripts import apply_ddl, seed_golden_data
    from jobs import comptime, fda_leave, pay_calendar, pseudossn
    from jobs.cpm import cpm_cdc, cpm_nih, cpm_oig
    from jobs.ehrp2biis import afterload, etl, preload
    from utils.config import get_config
    from utils.spark import get_spark

    cfg = get_config("test")
    apply_ddl.apply("test")
    seed_golden_data.seed("test", GOLDEN_DIR)

    spark = get_spark("golden-bootstrap")
    rd = dt.date.fromisoformat(RUN_DATE)

    pay_calendar.run(env="test", run_date=rd, spark=spark)
    comptime.run(env="test", file_path=os.path.join(GOLDEN_DIR, "comptime_input.csv"),
                 run_date=RUN_DATE, spark=spark)
    pseudossn.run(env="test", file_path=os.path.join(GOLDEN_DIR, "pseudossn_input.dat"),
                  run_date=RUN_DATE, spark=spark)
    fda_leave.run(env="test", run_date=RUN_DATE, spark=spark)
    preload.run(env="test", run_date=rd)
    etl.run(env="test", run_date=rd, spark=spark)
    afterload.run(env="test", run_date=rd)
    cpm_nih.run(env="test", run_date=RUN_DATE, spark=spark)
    cpm_oig.run(env="test", run_date=RUN_DATE, spark=spark)
    cpm_cdc.run(env="test", run_date=RUN_DATE, spark=spark)

    expected = {
        "pay_calendar": ["PAY_PERIOD"],
        "comptime": ["COMP_TIME_DAILY_TBL", "COUNTER_TBL"],
        "pseudossn": ["PSEUDOSSN_FROM_SDA_TBL", "PSEUDOSSN_TBL"],
        "fda_leave": ["ERROR_TBL"],
        "ehrp2biis": ["ACTION_PRIMARY_ALL", "ACTION_SECONDARY_ALL", "ACTION_REMARKS_ALL"],
        "cpm_nih": ["CPM_NIH_STG_TBL"],
        "cpm_oig": ["CPM_OIG_STG_TBL"],
        "cpm_cdc": ["CPM_CDC_STG_TBL"],
    }
    for module, tables in expected.items():
        for table in tables:
            path = os.path.join(GOLDEN_DIR, f"{module}_expected_{table.lower()}.csv")
            n = dump_table_to_csv(cfg, table, path)
            print(f"  expected {module}/{table}: {n} rows -> {os.path.basename(path)}")
    spark.stop()
    print("Bootstrapped expected golden datasets")


def main():
    write_inputs()
    bootstrap_expected()


if __name__ == "__main__":
    main()
