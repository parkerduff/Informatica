"""Generate deterministic synthetic golden fixtures (inputs + expected outputs).

Inputs and expected outputs are produced from the same synthetic dataset by
replaying each job's transformation rules in pure Python, so a clean run of
``make run-all`` followed by ``make validate`` reconciles with ZERO diffs.

Fixtures land in tests/fixtures/golden/ with the filenames expected by
scripts/seed_golden_data.py (inputs) and scripts/reconcile.py (expected).
"""
import argparse
import csv
import datetime
import hashlib
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils import schemas  # noqa: E402

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
GOLDEN_DIR = os.path.join(BASE, "tests", "fixtures", "golden")

TS = "%Y-%m-%d %H:%M:%S"


def write_csv(name, columns, rows):
    path = os.path.join(GOLDEN_DIR, name)
    with open(path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(columns)
        for r in rows:
            w.writerow(["" if r.get(c) is None else r.get(c) for c in columns])
    print(f"wrote {name} ({len(rows)} rows)")


def ts(d: datetime.date) -> str:
    return datetime.datetime(d.year, d.month, d.day).strftime(TS)


def fmt_decimal(value, scale: int) -> str:
    return f"{float(value):.{scale}f}" if scale else str(int(round(float(value))))


# ---------------------------------------------------------------- pay_calendar
def gen_pay_calendar(run_date):
    cols = ["PP_NUM", "PP_END_YEAR", "PP_START_DTE", "PP_END_DTE",
            "LV_NUM", "LV_YEAR", "PAY_DTE", "CURR_PP_FLAG"]
    anchor = run_date - datetime.timedelta(days=14 * 11 + 7)
    inputs, expected = [], []
    for i in range(26):
        start = anchor + datetime.timedelta(days=14 * i)
        end = start + datetime.timedelta(days=13)
        pp = i + 1
        row = {
            "PP_NUM": pp, "PP_END_YEAR": end.year,
            "PP_START_DTE": ts(start), "PP_END_DTE": ts(end),
            "LV_NUM": pp, "LV_YEAR": end.year,
            "PAY_DTE": ts(end + datetime.timedelta(days=8)),
            "CURR_PP_FLAG": None,
        }
        inputs.append(dict(row))
        exp = dict(row)
        if start <= run_date <= end:
            exp["CURR_PP_FLAG"] = "Y"
        expected.append(exp)
    current = next(r for r in expected if r["CURR_PP_FLAG"] == "Y")
    write_csv("pay_calendar_input_pay_period.csv", cols, inputs)
    write_csv("pay_calendar_expected_pay_period.csv", cols, expected)
    pp_start = datetime.datetime.strptime(current["PP_START_DTE"], TS).date()
    pp_end = datetime.datetime.strptime(current["PP_END_DTE"], TS).date()
    return {"pp_num": int(current["PP_NUM"]), "pp_end_year": int(current["PP_END_YEAR"]),
            "pp_start": pp_start, "pp_end": pp_end}


# -------------------------------------------------------------------- comptime
def gen_comptime(pp):
    in_cols = ["SSN", "NAME", "CURRENT_ACCT", "CURRENT_ORG", "FLSA_STATUS",
               "COMP_TIME_CUR_BAL", "COMP_TIME_YEAR_EARNED", "PP_END_DATE",
               "DAILY_DATE_EARNED", "COMP_TIME_RATE", "COMP_TIME_HOURS",
               "COMP_TIME_UNDEF"]
    exp_cols = ["PP_END_YEAR", "PP_NUM", "PP_YEAR_NUM", "SSN", "NAME",
                "CURRENT_ACCT", "CURRENT_ORG", "FLSA_STATUS", "COMP_TIME_CUR_BAL",
                "COMP_TIME_YEAR_EARNED", "PP_END_DATE", "DAILY_DATE_EARNED",
                "COMP_TIME_RATE", "COMP_TIME_HOURS", "COMP_TIME_UNDEF", "SSN_HASH"]
    pp_year_num = int(f"{pp['pp_end_year']}{pp['pp_num']:02d}")
    inputs, expected = [], []
    for i in range(100):
        ssn = f"{999000001 + i}"
        bal = 10.0 + (i % 40) * 0.25
        rate = 1.0 + (i % 3) * 0.5
        hours = 2.0 + (i % 8) * 0.75
        earned_date = pp["pp_start"] + datetime.timedelta(days=i % 14)
        row = {
            "SSN": ssn, "NAME": f"EMPLOYEE {i + 1:03d}",
            "CURRENT_ACCT": f"ACCT{i % 7}", "CURRENT_ORG": f"ORG{i % 5}",
            "FLSA_STATUS": "E" if i % 2 else "N",
            "COMP_TIME_CUR_BAL": f"{bal:.2f}",
            "COMP_TIME_YEAR_EARNED": pp["pp_end_year"],
            "PP_END_DATE": pp["pp_end"].isoformat(),
            "DAILY_DATE_EARNED": earned_date.isoformat(),
            "COMP_TIME_RATE": f"{rate:.2f}",
            "COMP_TIME_HOURS": f"{hours:.2f}",
            "COMP_TIME_UNDEF": str(i % 10),
        }
        inputs.append(row)
        expected.append({
            "PP_END_YEAR": pp["pp_end_year"], "PP_NUM": pp["pp_num"],
            "PP_YEAR_NUM": pp_year_num, "SSN": ssn, "NAME": row["NAME"],
            "CURRENT_ACCT": row["CURRENT_ACCT"], "CURRENT_ORG": row["CURRENT_ORG"],
            "FLSA_STATUS": row["FLSA_STATUS"],
            "COMP_TIME_CUR_BAL": f"{bal:.2f}",
            "COMP_TIME_YEAR_EARNED": pp["pp_end_year"],
            "PP_END_DATE": ts(pp["pp_end"]),
            "DAILY_DATE_EARNED": ts(earned_date),
            "COMP_TIME_RATE": f"{rate:.2f}", "COMP_TIME_HOURS": f"{hours:.2f}",
            "COMP_TIME_UNDEF": str(i % 10),
            "SSN_HASH": hashlib.sha256(ssn.encode()).hexdigest(),
        })
    with open(os.path.join(GOLDEN_DIR, "comptime_input.csv"), "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=in_cols)
        w.writeheader()
        w.writerows(inputs)
    print("wrote comptime_input.csv (100 rows)")
    write_csv("comptime_expected_comp_time_daily_tbl.csv", exp_cols, expected)

    counter_cols = ["PROCESS_NAME", "COUNTER_DESCRIPTION", "COUNTER_VALUE",
                    "PP_END_YEAR", "PP_NUM", "CYCLE_ID"]
    counters = [
        {"PROCESS_NAME": "COMPTIME",
         "COUNTER_DESCRIPTION": "Rows loaded into COMP_TIME_DAILY_TBL",
         "COUNTER_VALUE": 100, "PP_END_YEAR": pp["pp_end_year"],
         "PP_NUM": pp["pp_num"], "CYCLE_ID": 1},
        {"PROCESS_NAME": "PSEUDOSSN",
         "COUNTER_DESCRIPTION": "Rows loaded into PSEUDOSSN_FROM_SDA_TBL",
         "COUNTER_VALUE": 50, "PP_END_YEAR": pp["pp_end_year"],
         "PP_NUM": pp["pp_num"], "CYCLE_ID": 1},
        {"PROCESS_NAME": "FDA_LEAVE",
         "COUNTER_DESCRIPTION": "Rows loaded into ERROR_TBL",
         "COUNTER_VALUE": 5, "PP_END_YEAR": pp["pp_end_year"],
         "PP_NUM": pp["pp_num"], "CYCLE_ID": 1},
    ]
    write_csv("comptime_expected_counter_tbl.csv", counter_cols, counters)


# ------------------------------------------------------------------- pseudossn
PSEUDOSSN_RECORD_LEN = 500


def _render_fixed(values: dict, layout) -> str:
    buf = [" "] * PSEUDOSSN_RECORD_LEN
    for f in layout:
        raw = values.get(f["name"])
        if raw is None:
            continue
        text = str(raw)[:f["length"]].ljust(f["length"])
        buf[f["offset"]:f["offset"] + f["length"]] = list(text)
    return "".join(buf)


def gen_pseudossn(pp):
    layout = [f for f in schemas.fixed_width_layout("Pseudossn", "PSEUDOSSN_FILE")
              if not f["name"].startswith("FILLER")]
    tgt_fields = schemas.get_table_fields("Pseudossn", "PSEUDOSSN_FROM_SDA_TBL", "targets")
    tgt_names = [f["name"] for f in tgt_fields]
    tgt_by_name = {f["name"]: f for f in tgt_fields}
    mapping = dict(zip([f["name"] for f in layout], tgt_names))

    records = []
    for i in range(50):
        uniq = i if i < 45 else i - 45  # last 5 duplicate the first 5 pseudo SSNs
        eff = datetime.date(pp["pp_end_year"], 1, 10) + datetime.timedelta(days=uniq)
        if i >= 45:
            eff = eff - datetime.timedelta(days=30)  # older duplicate
        rec = {
            "SSN": f"{900000001 + i}",
            "CAN_CD": f"{10000000 + uniq}",
            "PSEUDO_SSN": f"{800000001 + uniq}",
            "EIN": f"{20000001 + uniq}",
            "EMP_REC_NO": "00",
            "LEGACY_APPT_NO": "01",
            "FIRST_NAME": f"FIRST{uniq:03d}",
            "MIDDLE_INIT": "M",
            "LAST_NAME": f"LAST{uniq:03d}",
            "HIRE_DATE": f"06{15 if uniq % 2 else 1:02d}19{80 + uniq % 20}",
            "UNIF_ALLOW_AMT": f"{1000 + uniq * 25:05d}" + ("-" if uniq % 10 == 0 else "+"),
            "EFFECTIVE_DATE": eff.strftime("%Y%m%d"),
            "EFFECTIVE_SEQ": "001",
            "PAY_TABL_NO": "0001",
            "BUSINESS_UNIT": "HHS",
            "DEPTID": f"D{uniq:05d}",
        }
        records.append(rec)

    header = "H" + "PSEUDOSSN SDA EXTRACT".ljust(PSEUDOSSN_RECORD_LEN - 1)
    trailer = "T" + f"{len(records):09d}".ljust(PSEUDOSSN_RECORD_LEN - 1)
    lines = [header] + [_render_fixed(r, layout) for r in records] + [trailer]
    with open(os.path.join(GOLDEN_DIR, "pseudossn_input.dat"), "w") as f:
        f.write("\n".join(lines) + "\n")
    print(f"wrote pseudossn_input.dat ({len(records)} detail rows)")

    date_fields = {"HIRE_DATE", "UNIF_ALLOW_DATE", "CAREER_START_DATE", "CAREER_CONV_DATE",
                   "PROBATION_DATE", "LAST_PAY_CHANGE", "CHARITY_EFF_DATE", "SEPARATION_DATE",
                   "PCA_CONTR_EFF_START_DATE", "PCA_CONTR_EFF_END_DATE", "APP_LIMIT_DATE",
                   "EFFECTIVE_DATE"}
    signed = {"UNIF_ALLOW_AMT": 2, "APPT_LIMIT_HRS": 2, "APPT_LIMIT_PAY": 2,
              "CHARITY_DED_AMT": 2, "QUARTERS_DEDUCTION": 2, "SUBSIST_DEDUCTION": 2,
              "MAX_ANNUAL_PAY": 2, "PCA_BIWEEKLY_AMOUNT": 2}
    integer_fields = {"EFFECTIVE_SEQ", "PCA_CONTR_YEAR"}

    def convert(file_name, raw):
        if raw is None or str(raw).strip() == "":
            return None
        raw = str(raw).strip()
        if file_name in date_fields:
            if set(raw) == {"0"}:
                return None
            if len(raw) == 8 and raw[:2] in ("19", "20"):
                d = datetime.datetime.strptime(raw, "%Y%m%d")
            else:
                d = datetime.datetime.strptime(raw, "%m%d%Y")
            return d.strftime(TS)
        if file_name in signed:
            sign = 1
            if raw[-1] in "+-":
                sign = -1 if raw[-1] == "-" else 1
                raw = raw[:-1]
            return f"{sign * int(raw) / (10 ** signed[file_name]):.{signed[file_name]}f}"
        if file_name in integer_fields:
            return str(int(raw))
        return raw

    expected_all = []
    for rec in records:
        out = {}
        for f in layout:
            tgt = mapping[f["name"]]
            out[tgt] = convert(f["name"], rec.get(f["name"]))
        out["PP_NUM"] = pp["pp_num"]
        out["PP_END_YEAR"] = pp["pp_end_year"]
        for c in tgt_names:
            out.setdefault(c, None)
        expected_all.append(out)

    write_csv("pseudossn_expected_pseudossn_from_sda_tbl.csv", tgt_names, expected_all)

    best = {}
    for row in expected_all:
        key = row["PSEUDOSSN"]
        rank = (row["EFFECTIVE_DATE"] or "", str(row["EFFECTIVE_SEQ"] or ""))
        if key not in best or rank > best[key][0]:
            best[key] = (rank, row)
    deduped = [v[1] for v in best.values()]
    write_csv("pseudossn_expected_pseudossn_tbl.csv", tgt_names, deduped)
    assert tgt_by_name["PSEUDOSSN"], "schema sanity"


# ------------------------------------------------------------------- fda_leave
def gen_fda_leave(pp):
    tatran_cols = ["FDA_BATCH_ID", "FDA_TK_NO", "FDA_EMP_ID", "FDA_PP_YEAR",
                   "FDA_PP_NUM", "FDA_REC_TYPE", "FDA_SEQ", "FDA_DATA"]
    stg_cols = ["EMP_ID", "PP_YEAR", "PP_NUM", "REC_TYPE"]
    tatran, stg = [], []
    for i in range(20):
        emp = f"{700000001 + i}"
        tatran.append({
            "FDA_BATCH_ID": "B0001", "FDA_TK_NO": f"TK{i:03d}", "FDA_EMP_ID": emp,
            "FDA_PP_YEAR": pp["pp_end_year"], "FDA_PP_NUM": pp["pp_num"],
            "FDA_REC_TYPE": "L", "FDA_SEQ": i + 1, "FDA_DATA": f"LEAVE DATA {i + 1}",
        })
        if i < 15:
            stg.append({"EMP_ID": emp, "PP_YEAR": pp["pp_end_year"],
                        "PP_NUM": pp["pp_num"], "REC_TYPE": "D"})
    write_csv("fda_leave_input_hi_pm_fda_tatran_tbl.csv", tatran_cols, tatran)
    for kind in ("ytd", "pad", "mer"):
        write_csv(f"fda_leave_input_cpm_{kind}_detail_stg_tbl.csv", stg_cols, stg)

    err_cols = ["PROCESS_NAME", "ERROR_MESSAGE", "SOURCE_KEY", "PP_END_YEAR", "PP_NUM"]
    msg = ("Missing CPM YTD detail record; Missing CPM PAD detail record; "
           "Missing CPM MER detail record")
    errors = []
    for i in range(15, 20):
        emp = f"{700000001 + i}"
        errors.append({
            "PROCESS_NAME": "FDA_LEAVE", "ERROR_MESSAGE": msg,
            "SOURCE_KEY": f"{emp}|{pp['pp_end_year']}|{pp['pp_num']}",
            "PP_END_YEAR": pp["pp_end_year"], "PP_NUM": pp["pp_num"],
        })
    write_csv("fda_leave_expected_error_tbl.csv", err_cols, errors)


# ------------------------------------------------------------------- ehrp2biis
def gen_ehrp2biis(run_date):
    effdt = ts(datetime.date(run_date.year, 1, 15))
    base_seq = 1000
    action_cols = ["EMPLID", "EMPL_RCD", "EFFDT", "EFFSEQ"]
    job_fields = schemas.get_table_fields("EHRP2BIIS_UPDATE", "PS_GVT_JOB", "sources")
    job_cols = [f["name"] for f in job_fields]
    actions, jobs = [], []
    for i in range(10):
        emplid = f"{100000001 + i}"
        actions.append({"EMPLID": emplid, "EMPL_RCD": 0, "EFFDT": effdt, "EFFSEQ": 1})
        jrow = {c: None for c in job_cols}
        jrow.update({"EMPLID": emplid, "EMPL_RCD": 0, "EFFDT": effdt, "EFFSEQ": 1,
                     "GVT_WIP_STATUS": "P", "PAYGROUP": "GS", "UNION_CD": "U1"})
        jobs.append(jrow)
    write_csv("ehrp2biis_input_nwk_new_ehrp_actions_tbl.csv", action_cols, actions)
    write_csv("ehrp2biis_input_ps_gvt_job.csv", job_cols, jobs)
    write_csv("ehrp2biis_input_sequence_num_tbl.csv",
              ["SEQ_NAME", "OLD_SEQUENCE_NUMBER", "NEW_SEQUENCE_NUMBER"],
              [{"SEQ_NAME": "EVENT_ID", "OLD_SEQUENCE_NUMBER": base_seq,
                "NEW_SEQUENCE_NUMBER": base_seq}])

    prim_fields = schemas.get_table_fields("EHRP2BIIS_UPDATE", "NWK_ACTION_PRIMARY_TBL", "targets")
    sec_fields = schemas.get_table_fields("EHRP2BIIS_UPDATE", "NWK_ACTION_SECONDARY_TBL", "targets")
    prim_names = [f["name"] for f in prim_fields]
    sec_names = [f["name"] for f in sec_fields]
    join_keys = {"EMPLID", "EMPL_RCD", "EFFDT", "EFFSEQ"}
    explicit = {"SSN": "EMPLID", "EVENT_EFF_DTE": "EFFDT", "EFFSEQ": "EFFSEQ",
                "EMPL_REC_NO": "EMPL_RCD"}

    def fmt(field, value):
        if value is None:
            return None
        dt = (field.get("datatype") or "").lower()
        if dt in ("date", "timestamp"):
            return value
        if dt.startswith("number"):
            return fmt_decimal(value, int(field.get("scale") or 0))
        return str(value)

    prim_rows, sec_rows = [], []
    for i, (act, jrow) in enumerate(zip(actions, jobs)):
        event_id = base_seq + i + 1  # window ordered by EMPLID asc
        source = dict(jrow)
        prow = {}
        for f in prim_fields:
            name = f["name"]
            if name == "EVENT_ID":
                prow[name] = event_id
            elif name == "LOAD_DATE":
                prow[name] = None
            elif name in explicit:
                prow[name] = fmt(f, source.get(explicit[name]))
            elif name in source or name in join_keys:
                prow[name] = fmt(f, source.get(name))
            else:
                prow[name] = None
        prim_rows.append(prow)
        srow = {}
        for f in sec_fields:
            name = f["name"]
            if name == "EVENT_ID":
                srow[name] = event_id
            elif name == "LOAD_DATE":
                srow[name] = None
            elif name in source or name in join_keys:
                srow[name] = fmt(f, source.get(name))
            else:
                srow[name] = None
        sec_rows.append(srow)

    exp_prim_cols = [c for c in prim_names if c != "LOAD_DATE"]
    exp_sec_cols = [c for c in sec_names if c != "LOAD_DATE"]
    write_csv("ehrp2biis_expected_action_primary_all.csv", exp_prim_cols, prim_rows)
    write_csv("ehrp2biis_expected_action_secondary_all.csv", exp_sec_cols, sec_rows)
    write_csv("ehrp2biis_expected_action_remarks_all.csv",
              ["EVENT_ID", "REMARK_SEQ", "REMARK_CD", "REMARK_TEXT"], [])


# ------------------------------------------------------------------------- cpm
def gen_cpm(pp):
    newpay_fields = schemas.get_table_fields("CPM_NIH", "CPM_NEWPAY_TBL", "sources")
    newpay_cols = [f["name"] for f in newpay_fields]
    field_by_name = {f["name"]: f for f in newpay_fields}

    rows = []
    for i in range(5):
        row = {c: None for c in newpay_cols}
        row["PP_END_YEAR"] = pp["pp_end_year"]
        row["PP_NUM"] = pp["pp_num"]
        row["DFAS_PSEUDO_SSN"] = f"{600000001 + i}"
        row["LINE_TYPE"] = "1"
        row["SURNAME_3"] = "DOE"
        row["MID_INIT"] = "X"
        rows.append(row)
    write_csv("cpm_input_cpm_newpay_tbl.csv", newpay_cols, rows)

    def db_str(name, value):
        """How the value comes back from SQL Server through Spark collect()."""
        if value is None:
            return None
        f = field_by_name.get(name)
        dt = (f.get("datatype") or "").lower() if f else ""
        if dt.startswith("number"):
            return fmt_decimal(value, int(f.get("scale") or 0))
        if dt in ("date", "timestamp"):
            return str(value)
        return str(value)

    def render(row, layout, signed=None):
        signed = signed or {}
        length = max((f["offset"] + f["length"]) for f in layout)
        buf = [" "] * length
        for f in layout:
            raw = row.get(f["name"])
            raw = db_str(f["name"], raw)
            if raw is None:
                text = " " * f["length"]
            elif f["name"] in signed:
                scale = signed[f["name"]]
                v = int(round(float(raw) * (10 ** scale)))
                text = f"{abs(v):0{f['length'] - 1}d}" + ("-" if v < 0 else "+")
            else:
                text = str(raw)
            text = text[:f["length"]].ljust(f["length"])
            buf[f["offset"]:f["offset"] + f["length"]] = list(text)
        return "".join(buf)

    specs = [
        ("NIH", "CPM_NIH", "nihtest_NIH_PAYROLL_MASTER", "cpm_nih_expected_staging.csv", False),
        ("OIG", "CPM_OIG", "oigsgndec_SKPAYROLL_MASTER", "cpm_oig_expected_staging.csv", True),
        ("CDC", "CPM_CDC", "cdcskel_WS_PAY_OUT_REC", "cpm_cdc_expected_staging.csv", False),
    ]
    ordered = sorted(rows, key=lambda r: (r["DFAS_PSEUDO_SSN"], r["LINE_TYPE"]))
    for agency, module, master, fname, use_signed in specs:
        layout = schemas.fixed_width_layout(module, master, "targets")
        signed = {}
        if use_signed:
            signed = {
                f["name"]: int(f.get("scale") or 0)
                for f in schemas.get_table_fields(module, master, "targets")
                if (f.get("datatype") or "").startswith("number")
                and int(f.get("scale") or 0) > 0
            }
        lines = [f"H{agency:<4}{pp['pp_end_year']:04d}{pp['pp_num']:02d}"]
        types = ["H"]
        for row in ordered:
            lines.append(render(row, layout, signed))
            types.append("D")
        lines.append(f"T{agency:<4}{len(ordered):09d}")
        types.append("T")
        out = [{"RECORD_NUM": i + 1, "RECORD_TYPE": t, "RECORD_DATA": line}
               for i, (t, line) in enumerate(zip(types, lines))]
        write_csv(fname, ["RECORD_NUM", "RECORD_TYPE", "RECORD_DATA"], out)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-date", default=None)
    args = parser.parse_args()
    run_date = (
        datetime.datetime.strptime(args.run_date, "%Y-%m-%d").date()
        if args.run_date else datetime.datetime.utcnow().date()
    )
    os.makedirs(GOLDEN_DIR, exist_ok=True)
    pp = gen_pay_calendar(run_date)
    print(f"Current pay period: PP{pp['pp_num']} {pp['pp_end_year']} "
          f"({pp['pp_start']}..{pp['pp_end']})")
    gen_comptime(pp)
    gen_pseudossn(pp)
    gen_fda_leave(pp)
    gen_ehrp2biis(run_date)
    gen_cpm(pp)
    print("Golden fixture generation complete.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
