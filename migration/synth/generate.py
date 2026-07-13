#!/usr/bin/env python3
"""
synth.generate -- schema-accurate synthetic data generator.

Reads each folder's SOURCE spec and produces:
  * the primary source feed (fixed-width with correct byte offsets + FILLER
    padding, or delimited, or a relational CSV), with valid Header/Trailer/Detail
    structure and an accurate trailer detail-count,
  * every lookup / secondary source (PAY_PERIOD, PS_GVT_JOB, ...) as a CSV,
  * two volumes: a small *functional* set (hand-verifiable edge cases incl.
    deliberately-invalid dates / non-numeric keys / dupes to exercise the
    IS_DATE / ERROR / dedup paths) and a large *performance* set.

No real PII is ever emitted -- SSNs / pseudo-SSNs are synthesised.
Files are written in the source's declared codepage (Latin1 / MS1252).
"""
from __future__ import annotations

import argparse
import csv
import datetime as dt
import os
import random
import sys
from typing import Any, Dict, List, Optional

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from migration.jobs import configs
from migration.lib import infa_compat as C
from migration.lib.pipeline import Column, SourceDef

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
DEFAULT_OUT = os.path.join(REPO_ROOT, "migration", "local", "data")

CURR_PP_NUM = 13
CURR_PP_YEAR = 2019


# ---------------------------------------------------------------------------
# Field value synthesis
# ---------------------------------------------------------------------------


def _rand_date(rng: random.Random, fmt: str = "%Y%m%d") -> str:
    d = dt.date(2018, 1, 1) + dt.timedelta(days=rng.randint(0, 800))
    return d.strftime(fmt)


def _signed_numeric(rng: random.Random, length: int, scale: int = 2) -> str:
    """digits filling (length-1) with a trailing +/- sign (overpunch style)."""
    digits = length - 1
    if digits < 1:
        digits = 1
    value = "".join(str(rng.randint(0, 9)) for _ in range(digits))
    sign = rng.choice("+-")
    return (value + sign)[:length].ljust(length)


def gen_field(col: Column, rng: random.Random, invalid: bool = False) -> str:
    name = (col.name or "").upper()
    ln = col.length or col.precision or 8
    if name.startswith("FILLER") or name == "NOTHING":
        return " " * ln
    if "SSN" in name:
        if invalid:
            return ("ABC" + "".join(str(rng.randint(0, 9)) for _ in range(6)))[:ln].ljust(ln)
        return "".join(str(rng.randint(0, 9)) for _ in range(9))[:ln].ljust(ln)
    if "EFFECTIVE_DATE" == name:
        # source format YYYYDDMM
        if invalid:
            return "9999XXYY"[:ln].ljust(ln)
        d = dt.date(2019, 1, 1) + dt.timedelta(days=rng.randint(0, 300))
        return f"{d.year:04d}{d.day:02d}{d.month:02d}"[:ln].ljust(ln)
    if name.endswith("DATE") or "_DATE" in name or name.endswith("_DTE"):
        if invalid:
            return ("00000000")[:ln].ljust(ln)
        return _rand_date(rng)[:ln].ljust(ln)
    if any(k in name for k in ("AMT", "_PAY", "HRS", "DED", "RATE", "_BAL", "AMOUNT", "DEDUCTION")):
        return _signed_numeric(rng, ln, col.scale or 2)
    if any(k in name for k in ("FLAG", "_IND", "STATUS", "_CD", "CODE", "SEX", "_NO", "_NUM")):
        pool = "ABCDEFGHJKLMNPQRSTUVWXYZ0123456789"
        return "".join(rng.choice(pool) for _ in range(ln))[:ln].ljust(ln)
    if "NAME" in name:
        pool = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        k = rng.randint(1, max(1, ln))
        return "".join(rng.choice(pool) for _ in range(k))[:ln].ljust(ln)
    if any(k in name for k in ("YEAR", "SEQ", "COUNT", "ID", "NUM")):
        return "".join(str(rng.randint(0, 9)) for _ in range(ln))[:ln].ljust(ln)
    pool = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789 "
    return "".join(rng.choice(pool) for _ in range(ln))[:ln].ljust(ln)


# ---------------------------------------------------------------------------
# Fixed-width record assembly
# ---------------------------------------------------------------------------


def _record_length(cols: List[Column]) -> int:
    return max((c.offset or 0) + (c.length or 0) for c in cols)


def _assemble_fixed(cols: List[Column], values: Dict[str, str], total: int) -> str:
    buf = [" "] * total
    for c in cols:
        if c.offset is None or c.length is None:
            continue
        val = values.get(c.name, "")[: c.length].ljust(c.length)
        buf[c.offset: c.offset + c.length] = list(val)
    return "".join(buf)


def gen_fixed_source(src: SourceDef, path: str, n: int, rng: random.Random,
                     record_type: Optional[Dict[str, Any]], invalid_rate: float,
                     dup_rate: float) -> Dict[str, int]:
    total = _record_length(src.columns)
    lines: List[str] = []
    stats = {"detail": 0, "invalid": 0, "dupes": 0}
    prev_key: Dict[str, str] = {}
    for i in range(n):
        invalid = rng.random() < invalid_rate
        values = {c.name: gen_field(c, rng, invalid=invalid) for c in src.columns}
        # occasionally duplicate the dedup/key value to exercise "latest wins"
        if record_type and prev_key and rng.random() < dup_rate:
            kf = record_type.get("key_field")
            if kf and kf in values:
                values[kf] = prev_key[kf]
                stats["dupes"] += 1
        if record_type and record_type.get("key_field"):
            prev_key = {record_type["key_field"]: values[record_type["key_field"]]}
        lines.append(_assemble_fixed(src.columns, values, total))
        stats["detail"] += 1
        if invalid:
            stats["invalid"] += 1

    out_lines: List[str] = []
    if record_type:
        first = src.columns[0]
        hdr_vals = {c.name: " " * (c.length or 0) for c in src.columns}
        hdr_vals[first.name] = record_type.get("header", "HEADER")[: first.length].ljust(first.length)
        # header date lands in the 2nd field (e.g. CAN_CD) as YYYY-MM-DD
        if len(src.columns) > 1:
            c2 = src.columns[1]
            hdr_vals[c2.name] = dt.date(CURR_PP_YEAR, 6, 20).isoformat()[: c2.length].ljust(c2.length)
        out_lines.append(_assemble_fixed(src.columns, hdr_vals, total))
        out_lines.extend(lines)
        tr_vals = {c.name: " " * (c.length or 0) for c in src.columns}
        tr_vals[first.name] = record_type.get("trailer", "TRAILER")[: first.length].ljust(first.length)
        if len(src.columns) > 1:
            c2 = src.columns[1]
            tr_vals[c2.name] = str(stats["detail"]).rjust(c2.length or 8, "0")
        out_lines.append(_assemble_fixed(src.columns, tr_vals, total))
    else:
        out_lines = lines

    cp = C.resolve_codepage(src.codepage)
    with open(path, "w", encoding=cp, errors="replace", newline="\n") as fh:
        for ln in out_lines:
            fh.write(ln + "\n")
    return stats


def gen_delimited_source(src: SourceDef, path: str, n: int, rng: random.Random,
                         invalid_rate: float) -> Dict[str, int]:
    stats = {"detail": 0, "invalid": 0}
    cp = C.resolve_codepage(src.codepage)
    with open(path, "w", encoding=cp, errors="replace", newline="") as fh:
        w = csv.writer(fh, delimiter=src.delimiter or ",")
        for i in range(n):
            invalid = rng.random() < invalid_rate
            row = [gen_field(c, rng, invalid=invalid).strip() for c in src.columns]
            w.writerow(row)
            stats["detail"] += 1
            if invalid:
                stats["invalid"] += 1
    return stats


def gen_relational_source(src: SourceDef, path: str, n: int, rng: random.Random,
                          invalid_rate: float, overrides=None) -> Dict[str, int]:
    stats = {"detail": 0, "invalid": 0}
    overrides = overrides or (lambda i, rng: {})
    with open(path, "w", encoding="utf-8", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=[c.name for c in src.columns])
        w.writeheader()
        for i in range(n):
            invalid = rng.random() < invalid_rate
            row = {c.name: gen_field(c, rng, invalid=invalid).strip() for c in src.columns}
            row.update(overrides(i, rng))
            w.writerow(row)
            stats["detail"] += 1
            if invalid:
                stats["invalid"] += 1
    return stats


def gen_pay_period(src: SourceDef, path: str) -> None:
    with open(path, "w", encoding="utf-8", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=[c.name for c in src.columns])
        w.writeheader()
        for i in range(6):
            row = {c.name: "" for c in src.columns}
            row["PP_NUM"] = CURR_PP_NUM - 5 + i
            row["PP_END_YEAR"] = CURR_PP_YEAR
            row["CURR_PP_FLAG"] = "Y" if (i == 5) else "N"
            if "PP_START_DTE" in row:
                row["PP_START_DTE"] = (dt.date(2019, 1, 1) + dt.timedelta(days=14 * i)).isoformat()
            if "PP_END_DTE" in row:
                row["PP_END_DTE"] = (dt.date(2019, 1, 14) + dt.timedelta(days=14 * i)).isoformat()
            w.writerow(row)


# ---------------------------------------------------------------------------
# Driver
# ---------------------------------------------------------------------------


VOLUMES = {"functional": 200, "performance": 50000}
WIDE_PERF = {"CPM_NIH": 8000, "CPM_CDC": 5000, "CPM_OIG": 8000}  # very wide -> fewer rows


def generate_folder(folder: str, mode: str, out_dir: str, seed: int = 42,
                    rows: Optional[int] = None) -> Dict[str, Any]:
    rng = random.Random(f"{folder}:{mode}:{seed}")
    pdef = configs.build(folder)
    n = rows if rows is not None else VOLUMES[mode]
    if mode == "performance" and folder in WIDE_PERF:
        n = WIDE_PERF[folder]
    invalid_rate = 0.05 if mode == "functional" else 0.01
    dup_rate = 0.10 if mode == "functional" else 0.02

    fdir = os.path.join(out_dir, mode, folder)
    os.makedirs(fdir, exist_ok=True)
    manifest: Dict[str, Any] = {"folder": folder, "mode": mode, "rows": n, "files": {}}

    src = pdef.primary_source
    ppath = os.path.join(fdir, f"{src.name}.dat")
    if src.kind == "fixedwidth":
        stats = gen_fixed_source(src, ppath, n, rng, pdef.record_type, invalid_rate, dup_rate)
    elif src.kind == "delimited":
        stats = gen_delimited_source(src, ppath, n, rng, invalid_rate)
    else:
        ppath = os.path.join(fdir, f"{src.name}.csv")
        stats = gen_relational_source(src, ppath, n, rng, invalid_rate)
    manifest["files"][src.name] = os.path.relpath(ppath, out_dir)
    manifest["primary_stats"] = stats

    for extra in pdef.extra_sources:
        epath = os.path.join(fdir, f"{extra.name}.csv")
        if extra.name == "PAY_PERIOD":
            gen_pay_period(extra, epath)
        else:
            # keep join keys aligned with the primary feed's EMPLID space
            gen_relational_source(extra, epath, max(50, n // 5), rng, 0.0)
        manifest["files"][extra.name] = os.path.relpath(epath, out_dir)

    # always emit a PAY_PERIOD reference for lookups / audit context
    if "PAY_PERIOD" not in manifest["files"]:
        from migration.jobs.configs import _mk_source
        try:
            pp = _mk_source(configs.load_spec(folder), "PAY_PERIOD")
            epath = os.path.join(fdir, "PAY_PERIOD.csv")
            gen_pay_period(pp, epath)
            manifest["files"]["PAY_PERIOD"] = os.path.relpath(epath, out_dir)
        except Exception:
            pass
    return manifest


def main(argv: List[str]) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--folders", nargs="*", default=configs.ALL_FOLDERS)
    ap.add_argument("--mode", choices=["functional", "performance", "both"], default="both")
    ap.add_argument("--out", default=DEFAULT_OUT)
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--rows", type=int, default=None)
    args = ap.parse_args(argv[1:])
    modes = ["functional", "performance"] if args.mode == "both" else [args.mode]
    import json
    all_manifests = []
    for mode in modes:
        for folder in args.folders:
            m = generate_folder(folder, mode, args.out, args.seed, args.rows)
            all_manifests.append(m)
            print(f"[synth] {mode:11s} {folder:18s} rows={m['rows']:>7} "
                  f"files={list(m['files'])}")
    os.makedirs(args.out, exist_ok=True)
    with open(os.path.join(args.out, "manifest.json"), "w") as fh:
        json.dump(all_manifests, fh, indent=2)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
