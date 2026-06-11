#!/usr/bin/env python3
"""Generate structurally valid, non-PII golden fixtures for every module.

The expected outputs are produced with exactly the same business rules as the
PySpark jobs (shared helpers in ``jobs.common`` / ``jobs.cpm.cpm_common`` /
``jobs.ehrp2biis.mapping`` / ``jobs.pseudossn``), which is what lets the
reconciliation gate assert zero diffs.

This is a bootstrap: replace these synthetic captures with PII-scrubbed
production extracts when available. All SSNs use the reserved 9xx test range.

Usage: python scripts/generate_synthetic_golden.py [--out tests/fixtures/golden]
"""
from __future__ import annotations

import argparse
import csv
import datetime as dt
from pathlib import Path
from typing import Dict, List, Sequence

from jobs.common import build_record_text, hash_ssn, oracle_kind, pp_year_num
from jobs.ehrp2biis import mapping as M
from jobs.pseudossn import RECTYPE_WIDTH, build_layout
from jobs.schemas import PS_GVT_JOB_COLS

# Canonical run context for the synthetic data.
RUN_DATE = dt.date(2026, 6, 11)
PP_NUM = 12
PP_END_YEAR = 2026
PP_YEAR_NUM = pp_year_num(PP_END_YEAR, PP_NUM)
PP_START0 = dt.date(2026, 1, 4)  # start of pay period 1


def write_csv(path: Path, header: Sequence[str], rows: Sequence[Sequence]) -> None:
    with path.open("w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(header)
        for r in rows:
            w.writerow(["" if v is None else v for v in r])
    print(f"wrote {path.name} ({len(rows)} rows)")


# --------------------------------------------------------------------------- #
# Pay Calendar
# --------------------------------------------------------------------------- #
def gen_pay_calendar(out: Path) -> None:
    header = ["PP_NUM", "PP_END_YEAR", "PP_START_DTE", "PP_END_DTE", "LV_NUM",
              "LV_YEAR", "PAY_DTE", "CURR_PP_FLAG", "HOLIDAY_1", "HOLIDAY_2"]
    rows = []
    for n in range(1, 27):
        start = PP_START0 + dt.timedelta(days=14 * (n - 1))
        end = start + dt.timedelta(days=13)
        pay = end + dt.timedelta(days=5)
        curr = "Y" if start <= RUN_DATE <= end else ""
        rows.append([n, PP_END_YEAR, start.isoformat(), end.isoformat(), n,
                     PP_END_YEAR, pay.isoformat(), curr, "", ""])
    write_csv(out / "pay_calendar_input_pay_period.csv", header, rows)

    exp_header = ["PP_NUM", "PP_END_YEAR", "CURR_PP_FLAG"]
    exp_rows = [[r[0], r[1], r[7]] for r in rows]
    write_csv(out / "pay_calendar_expected_pay_period.csv", exp_header, exp_rows)


# --------------------------------------------------------------------------- #
# COMPTIME
# --------------------------------------------------------------------------- #
def gen_comptime(out: Path) -> None:
    in_header = ["SSN", "NAME", "CURRENT_ACCT", "CURRENT_ORG", "FLSA_STATUS",
                 "COMP_TIME_CUR_BAL", "COMP_TIME_YEAR_EARNED", "PP_END_DATE",
                 "DAILY_DATE_EARNED", "COMP_TIME_RATE", "COMP_TIME_HOURS",
                 "COMP_TIME_UNDEF"]
    pp_end = "20260620"
    daily = "20260605"
    in_rows, exp_rows = [], []
    for i in range(1, 101):
        ssn = f"999000{i:03d}"
        name = f"NAME{i:05d}"
        bal = f"{40 + i}.00"
        in_rows.append([ssn, name, f"ACT{i:03d}", f"ORG{i:02d}", "N", bal,
                        "2026", pp_end, daily, "1.00", "8.00", "0"])
        exp_rows.append([ssn, name, bal, PP_END_YEAR, PP_NUM, PP_YEAR_NUM,
                         hash_ssn(ssn), "2026-06-20", "2026-06-05"])
    write_csv(out / "comptime_input.csv", in_header, in_rows)
    write_csv(
        out / "comptime_expected_comp_time_daily_tbl.csv",
        ["SSN", "NAME", "COMP_TIME_CUR_BAL", "PP_END_YEAR", "PP_NUM",
         "PP_YEAR_NUM", "SSN_HASH", "PP_END_DATE", "DAILY_DATE_EARNED"],
        exp_rows,
    )
    write_csv(out / "comptime_expected_counter_tbl.csv",
              ["PROCESS_NAME", "COUNTER_VALUE"], [["COMPTIME", 100]])


# --------------------------------------------------------------------------- #
# PseudoSSN (fixed-width SDA file)
# --------------------------------------------------------------------------- #
def _encode_field(value, kind: str, width: int, scale: int) -> str:
    if kind == "date":
        if not value:
            return " " * width
        d = dt.date.fromisoformat(value)
        return d.strftime("%Y%m%d").ljust(width)[:width]
    if kind == "decimal":
        scaled = int(round(float(value or 0) * (10 ** scale)))
        return str(scaled).zfill(width)[:width]
    return str(value or "").ljust(width)[:width]


def gen_pseudossn(out: Path) -> None:
    layout = build_layout()
    # Build 50 canonical detail records: 45 unique + 5 later-dated duplicates.
    details: List[Dict] = []
    for i in range(45):
        details.append({
            "PSEUDOSSN": f"{900000001 + i}",
            "SSN": f"{700000001 + i}",
            "EMP_LAST_NAME": f"L{i:04d}",
            "EFFECTIVE_DATE": (dt.date(2026, 1, 1) + dt.timedelta(days=i)).isoformat(),
            "EFFECTIVE_SEQ": 1,
        })
    for i in range(5):  # duplicates of first 5 pseudo SSNs, later effective date
        details.append({
            "PSEUDOSSN": f"{900000001 + i}",
            "SSN": f"{700000001 + i}",
            "EMP_LAST_NAME": f"D{i:04d}",
            "EFFECTIVE_DATE": (dt.date(2027, 1, 1) + dt.timedelta(days=i)).isoformat(),
            "EFFECTIVE_SEQ": 2,
        })

    width = RECTYPE_WIDTH + sum(w for _, _, w, _, _ in layout)
    lines = ["H" + " " * (width - 1)]
    for rec in details:
        line = "D"
        for name, _start, w, kind, scale in layout:
            line += _encode_field(rec.get(name), kind, w, scale)
        lines.append(line)
    lines.append("T" + f"{len(details):09d}".ljust(width - 1)[:width - 1])
    (out / "pseudossn_input.dat").write_text("\n".join(lines) + "\n", encoding="ascii")
    print(f"wrote pseudossn_input.dat ({len(details)} detail records, width={width})")

    cols = ["PSEUDOSSN", "EFFECTIVE_DATE", "EFFECTIVE_SEQ", "SSN", "EMP_LAST_NAME"]
    sda_rows = [[r["PSEUDOSSN"], r["EFFECTIVE_DATE"], r["EFFECTIVE_SEQ"],
                 r["SSN"], r["EMP_LAST_NAME"]] for r in details]
    write_csv(out / "pseudossn_expected_pseudossn_from_sda_tbl.csv", cols, sda_rows)

    # Dedup: latest EFFECTIVE_DATE (then EFFECTIVE_SEQ) per PSEUDOSSN.
    best: Dict[str, Dict] = {}
    for r in details:
        key = r["PSEUDOSSN"]
        cur = best.get(key)
        if cur is None or (r["EFFECTIVE_DATE"], r["EFFECTIVE_SEQ"]) > (
                cur["EFFECTIVE_DATE"], cur["EFFECTIVE_SEQ"]):
            best[key] = r
    dedup_rows = [[r["PSEUDOSSN"], r["EFFECTIVE_DATE"], r["EFFECTIVE_SEQ"],
                   r["SSN"], r["EMP_LAST_NAME"]] for r in best.values()]
    write_csv(out / "pseudossn_expected_pseudossn_tbl.csv", cols, dedup_rows)


# --------------------------------------------------------------------------- #
# FDA Leave
# --------------------------------------------------------------------------- #
def gen_fda_leave(out: Path) -> None:
    emp_ids = [f"999100{i:03d}" for i in range(1, 21)]
    present = emp_ids[:15]
    missing = emp_ids[15:]

    tatran_header = ["FDA_BATCH_ID", "FDA_TK_NO", "FDA_EMP_ID", "FDA_PP_YEAR",
                     "FDA_PP_NUM", "FDA_REC_TYPE", "FDA_SEQ", "FDA_DATA"]
    tatran_rows = []
    for i, emp in enumerate(emp_ids, start=1):
        tatran_rows.append([i, f"TK{i:05d}", emp, str(PP_END_YEAR), str(PP_NUM),
                            "01", i, f"DATA{i:03d}"])
    write_csv(out / "fda_leave_input_hi_pm_fda_tatran_tbl.csv", tatran_header, tatran_rows)

    staging = [
        ("fda_leave_input_cpm_ytd_detail_stg_tbl.csv",
         ["PP_END_YEAR", "PP_NUM", "DYD_SSN_1"]),
        ("fda_leave_input_cpm_pad_detail_stg_tbl.csv",
         ["PP_END_YEAR", "PP_NUM", "PAD_SOC_SEC_NO"]),
        ("fda_leave_input_cpm_mer_detail_stg_tbl.csv",
         ["PP_END_YEAR", "PP_NUM", "MER_SSN"]),
    ]
    for fname, hdr in staging:
        rows = [[PP_END_YEAR, PP_NUM, emp] for emp in present]
        write_csv(out / fname, hdr, rows)

    exp_header = ["PROCESS_NAME", "ERROR_MESSAGE", "SOURCE_KEY", "PP_END_YEAR",
                  "PP_NUM", "CYCLE_ID", "ERROR_CODE"]
    exp_rows = [["FDA_LEAVE", "Missing CPM detail: YTD,PAD,MER", emp,
                 PP_END_YEAR, PP_NUM, 1, "FDA_LKP_MISS"] for emp in missing]
    write_csv(out / "fda_leave_expected_error_tbl.csv", exp_header, exp_rows)


# --------------------------------------------------------------------------- #
# EHRP2BIIS
# --------------------------------------------------------------------------- #
def gen_ehrp2biis(out: Path) -> None:
    base_seq = 1000000
    n = 10
    actions = []
    for i in range(n):
        actions.append({
            "EMPLID": f"{i + 1:08d}",
            "EMPL_RCD": "0",
            "EFFDT": f"2026-02-{i + 1:02d}",
            "EFFSEQ": "0",
            "ACTION": ["HIR", "PRO", "REA", "TER", "CON",
                       "DET", "RTN", "PAY", "AWD", "LWP"][i],
            "ACTION_REASON": f"R{i:02d}",
            "UNION_CD": "000",
            "GVT_WIP_STATUS": "WIP",
            "GVT_STATUS_TYPE": "ACT",
            "REPORTS_TO": f"RPT{i:05d}",
            "POSITION_ENTRY_DT": f"2026-01-{i + 1:02d}",
            "PAYGROUP": "BIW",
        })

    write_csv(
        out / "ehrp2biis_input_nwk_new_ehrp_actions_tbl.csv",
        ["EMPLID", "EMPL_RCD", "EFFDT", "EFFSEQ"],
        [[a["EMPLID"], a["EMPL_RCD"], a["EFFDT"], a["EFFSEQ"]] for a in actions],
    )
    write_csv(out / "ehrp2biis_input_sequence_num_tbl.csv",
              ["SEQ_NAME", "CURRENT_VALUE"], [[M.SEQ_NAME, base_seq]])

    # Full PS_GVT_JOB rows: fill every column (229 NOT NULL) with defaults,
    # overriding the join keys + the fields we reconcile on.
    gvt_cols = [c[0] for c in PS_GVT_JOB_COLS]
    gvt_rows = []
    for a in actions:
        row = []
        for name, dt_, p, s in PS_GVT_JOB_COLS:
            if name in a:
                row.append(a[name])
                continue
            kind, _p, _s = oracle_kind(dt_, p, s)
            if kind == "date":
                row.append("2026-01-01")
            elif kind == "decimal":
                row.append("0")
            else:
                row.append("X")
        gvt_rows.append(row)
    write_csv(out / "ehrp2biis_input_ps_gvt_job.csv", gvt_cols, gvt_rows)

    # Expected outputs (EVENT_ID = base + 1-based row number in key order).
    prim_header = ["EVENT_ID", "EFFSEQ", "GVT_WIP_STATUS", "GVT_STATUS_TYPE",
                   "REPORTS_TO", "UNION_CD", "POSITION_ENTRY_DT"]
    sec_header = ["EVENT_ID", "PAYGROUP"]
    rem_header = ["EVENT_ID", "REMARK_SEQ", "REMARK_CD", "REMARK_TEXT"]
    prim_rows, sec_rows, rem_rows = [], [], []
    for i, a in enumerate(actions):
        event_id = base_seq + i + 1
        prim_rows.append([event_id, a["EFFSEQ"], a["GVT_WIP_STATUS"],
                          a["GVT_STATUS_TYPE"], a["REPORTS_TO"], a["UNION_CD"],
                          a["POSITION_ENTRY_DT"]])
        sec_rows.append([event_id, a["PAYGROUP"]])
        rem_rows.append([event_id, 1, a["ACTION"],
                         f"{a['ACTION']}-{a['ACTION_REASON']}"])
    write_csv(out / "ehrp2biis_expected_action_primary_all.csv", prim_header, prim_rows)
    write_csv(out / "ehrp2biis_expected_action_secondary_all.csv", sec_header, sec_rows)
    write_csv(out / "ehrp2biis_expected_action_remarks_all.csv", rem_header, rem_rows)


# --------------------------------------------------------------------------- #
# CPM
# --------------------------------------------------------------------------- #
def gen_cpm(out: Path) -> None:
    in_header = ["PP_END_YEAR", "PP_NUM", "DFAS_PSEUDO_SSN", "LINE_TYPE",
                 "SOC_SEC_NO", "ADJ_GROSS_PAY", "ADJ_NET_PAY"]
    rows = []
    data = []
    for i in range(1, 6):
        ssn = f"999200{i:03d}"
        gross = f"{1000 + i}.{i:02d}"
        net = f"{800 + i}.{i:02d}"
        rows.append([PP_END_YEAR, PP_NUM, f"{800000000 + i}", "D", ssn, gross, net])
        data.append((ssn, gross, net))
    write_csv(out / "cpm_input_cpm_newpay_tbl.csv", in_header, rows)

    exp_header = ["SSN", "AGENCY_CD", "GROSS_PAY", "NET_PAY", "RECORD_TEXT",
                  "PP_END_YEAR", "PP_NUM"]
    for agency, code in (("nih", "NIH"), ("oig", "OIG"), ("cdc", "CDC")):
        exp_rows = []
        for ssn, gross, net in data:
            rec = build_record_text(ssn, code, gross, net)
            exp_rows.append([ssn, code, gross, net, rec, PP_END_YEAR, PP_NUM])
        write_csv(out / f"cpm_{agency}_expected_staging.csv", exp_header, exp_rows)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="tests/fixtures/golden")
    args = ap.parse_args()
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)

    gen_pay_calendar(out)
    gen_comptime(out)
    gen_pseudossn(out)
    gen_fda_leave(out)
    gen_ehrp2biis(out)
    gen_cpm(out)
    print(f"\nGolden fixtures written to {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
