#!/usr/bin/env python3
"""
engine_pandas -- pure-Python executor for a ``PipelineDef``.

This is the runtime behind the **Glue Python Shell** jobs (light reference /
validation feeds).  It executes the whole mapping single-node:

  read source -> record-type flag -> derived ports -> broadcast lookups ->
  error rules -> Sorter dedup -> project to target / ERROR_TBL / COUNTER_TBL.

Transformation semantics come from infa_compat / infa_expr; only orchestration
of the mapping lives here.
"""
from __future__ import annotations

import time
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any, Callable, Dict, List, Optional

from . import infa_compat as C
from . import infa_expr as E
from . import records as R
from .pipeline import Column, PipelineDef, SourceDef


@dataclass
class Result:
    main: List[Dict[str, Any]] = field(default_factory=list)
    error: List[Dict[str, Any]] = field(default_factory=list)
    counters: List[Dict[str, Any]] = field(default_factory=list)
    metrics: Dict[str, Any] = field(default_factory=dict)


@dataclass
class RunContext:
    run_ts: str = "2019-06-20 19:07:03"
    pp_end_year: Optional[int] = 2019
    pp_num: Optional[int] = 13
    cycle_id: Optional[int] = 1


def _load_lookups(pdef: PipelineDef, resolver: Dict[str, str]) -> Dict[str, Dict]:
    tables: Dict[str, Dict] = {}
    for lk in pdef.lookups:
        # the lookup source must be provided by the resolver + a SourceDef
        src = _find_source(pdef, lk.source)
        rows = R.read_source(src, resolver[lk.source]) if lk.source in resolver else []
        index: Dict[tuple, Dict[str, Any]] = {}
        for row in rows:
            key = tuple(C._to_str(row.get(rf, "")).strip() for rf, _ in lk.on)
            index[key] = row
        tables[lk.name] = index
    return tables


def _find_source(pdef: PipelineDef, name: str) -> SourceDef:
    if pdef.primary_source.name == name:
        return pdef.primary_source
    for s in pdef.extra_sources:
        if s.name == name:
            return s
    raise KeyError(name)


def run(pdef: PipelineDef, resolver: Dict[str, str],
        ctx: Optional[RunContext] = None) -> Result:
    ctx = ctx or RunContext()
    t0 = time.time()
    C.set_sessstarttime(_parse_ts(ctx.run_ts))

    rows = R.read_source(pdef.primary_source, resolver[pdef.primary_source.name])
    lookups = _load_lookups(pdef, resolver)

    compiled = [(dp.name, E.compile_expr(dp.expr)) for dp in pdef.derived_ports]
    err_compiled = [
        (er["name"], er.get("message", "transformation error"),
         er.get("code", "RULE"), E.compile_expr(er["expr"]))
        for er in pdef.error_rules
    ]

    rt = pdef.record_type
    header_count = trailer_declared = 0
    detail_rows: List[Dict[str, Any]] = []
    error_rows: List[Dict[str, Any]] = []

    for row in rows:
        flag = "D"
        if rt:
            flag = C.record_type_flag(
                row.get(rt["field"], ""),
                rt.get("header", "HEADER"),
                rt.get("trailer", "TRAILER"),
            )
            if flag == "H":
                header_count += 1
                continue
            if flag == "T":
                if rt.get("trailer_count_expr"):
                    dv = E.evaluate(rt["trailer_count_expr"], row)
                    trailer_declared = C.to_integer(dv) or 0
                continue
            if flag not in pdef.detail_flags:
                continue

        # broadcast lookups (constant keys select the "current" reference row)
        row["__const_curr"] = "Y"
        for lk in pdef.lookups:
            key = tuple(C._to_str(row.get(rf, "")).strip() for rf, _ in lk.on)
            match = lookups.get(lk.name, {}).get(key)
            for out_name, lk_field in lk.select:
                row[out_name] = match.get(lk_field) if match else None

        # derived ports (Informatica variable/output ports, evaluated in order)
        err_msg = err_code = None
        for name, fn in compiled:
            try:
                row[name] = fn(row)
            except C.TransformationError as te:
                err_msg, err_code = te.message, "XFORM"
                row[name] = None
                break

        # declarative error rules
        if err_msg is None:
            for _n, msg, code, fn in err_compiled:
                try:
                    if C._truthy(fn(row)):
                        err_msg, err_code = msg, code
                        break
                except C.TransformationError as te:
                    err_msg, err_code = te.message, "XFORM"
                    break

        if err_msg is not None:
            error_rows.append(_error_row(pdef, row, err_msg, err_code, ctx))
        else:
            detail_rows.append(row)

    # Sorter 'latest record wins' dedup
    if pdef.dedup:
        detail_rows = _dedup(detail_rows, pdef.dedup)

    main = [_project(pdef, row) for row in detail_rows]

    counters = _counters(pdef, ctx, total=len(rows), header=header_count,
                         detail=len(main), errors=len(error_rows),
                         trailer_declared=trailer_declared)

    res = Result(main=main, error=error_rows, counters=counters)
    res.metrics = {
        "engine": "python_shell",
        "folder": pdef.folder,
        "mapping": pdef.mapping,
        "input_rows": len(rows),
        "detail_rows": len(main),
        "error_rows": len(error_rows),
        "runtime_sec": round(time.time() - t0, 4),
    }
    rt_sec = res.metrics["runtime_sec"] or 1e-9
    res.metrics["rows_per_sec"] = round(len(rows) / rt_sec, 1)
    return res


def _dedup(rows: List[Dict[str, Any]], dedup) -> List[Dict[str, Any]]:
    def sort_key(r):
        parts = []
        for fld, direction in dedup.order:
            v = r.get(fld)
            parts.append(_ordkey(v, direction))
        parts.append((r.get(R.SEQ_COL, 0),))  # stable tiebreak
        return tuple(parts)

    ordered = sorted(rows, key=sort_key)
    seen = set()
    out = []
    for r in ordered:
        key = tuple(C._to_str(r.get(k, "")).strip() for k in dedup.keys)
        if key in seen:
            continue
        seen.add(key)
        out.append(r)
    return out


def _ordkey(value, direction):
    # produce a comparable tuple that sorts asc/desc, NULLs last
    if value is None:
        base = (1, "")
    elif isinstance(value, (int, float, Decimal)):
        base = (0, float(value))
    else:
        base = (0, C._to_str(value))
    if direction == "desc":
        if base[0] == 0 and isinstance(base[1], float):
            return (base[0], -base[1])
        if base[0] == 0:
            return (base[0], _invert_str(base[1]))
    return base


def _invert_str(s: str):
    # invert string ordering for desc by mapping to complement code points
    return tuple(-ord(ch) for ch in s)


def _project(pdef: PipelineDef, row: Dict[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    tgt = pdef.target
    for col in tgt.columns:
        src_field = tgt.field_map.get(col.name, col.name)
        out[col.name] = R.format_value(row.get(src_field), col)
    return out


def _error_row(pdef, row, msg, code, ctx) -> Dict[str, Any]:
    key_field = (pdef.record_type or {}).get("key_field")
    source_key = None
    if key_field:
        source_key = C._to_str(row.get(key_field, "")).strip()
    return {
        "PROCESS_NAME": pdef.mapping,
        "ERROR_MESSAGE": msg,
        "SOURCE_KEY": source_key,
        "ERROR_DATE": ctx.run_ts,
        "PP_END_YEAR": ctx.pp_end_year,
        "PP_NUM": ctx.pp_num,
        "CYCLE_ID": ctx.cycle_id,
        "ERROR_CODE": code,
    }


def _counters(pdef, ctx, total, header, detail, errors, trailer_declared) -> List[Dict[str, Any]]:
    descs = [
        ("TOTAL_RECORD_COUNT", total),
        ("HEADER_RECORD_COUNT", header),
        ("DETAIL_RECORD_COUNT", detail),
        ("ERROR_RECORD_COUNT", errors),
    ]
    if pdef.record_type and pdef.record_type.get("trailer_count_expr"):
        descs.append(("TRAILER_DECLARED_COUNT", trailer_declared))
        descs.append(("TRAILER_MATCH", 1 if trailer_declared == detail + errors else 0))
    out = []
    for name, val in descs:
        out.append({
            "RUN_DATE": ctx.run_ts,
            "PROCESS_NAME": pdef.mapping,
            "COUNTER_DESCRIPTION": name,
            "COUNTER_VALUE": val,
            "PP_END_YEAR": ctx.pp_end_year,
            "PP_NUM": ctx.pp_num,
            "CYCLE_ID": ctx.cycle_id,
        })
    return out


def _parse_ts(ts: str):
    import datetime
    for fmt in ("%Y-%m-%d %H:%M:%S", "%m/%d/%Y %H:%M:%S"):
        try:
            return datetime.datetime.strptime(ts, fmt)
        except ValueError:
            continue
    return datetime.datetime.now()
