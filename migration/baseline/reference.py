#!/usr/bin/env python3
"""
baseline.reference -- the spec-derived GOLDEN baseline.

This is an independent, single-threaded reference implementation of a
``PipelineDef``.  It is coded separately from the converted job engines
(``engine_pandas`` / ``engine_spark``) and shares only the transformation
*semantics* library (``infa_compat`` / ``infa_expr``), which is unit-tested in
its own right.  It produces the expected ("golden") main / error / counter
outputs that the converted jobs are reconciled against.

Assumption (documented in the reports): the live Informatica PowerCenter engine
is unavailable, so the golden dataset is derived strictly from the XML mapping
expressions rather than from a live PowerCenter run.
"""
from __future__ import annotations

import os
import sys
from decimal import Decimal
from typing import Any, Dict, List, Optional

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from migration.lib import infa_compat as C
from migration.lib import infa_expr as E
from migration.lib import records as R
from migration.lib.engine_pandas import Result, RunContext, _parse_ts
from migration.lib.pipeline import PipelineDef, SourceDef


def _read(src: SourceDef, path: str) -> List[Dict[str, Any]]:
    return R.read_source(src, path)


def run(pdef: PipelineDef, resolver: Dict[str, str],
        ctx: Optional[RunContext] = None) -> Result:
    ctx = ctx or RunContext()
    C.set_sessstarttime(_parse_ts(ctx.run_ts))

    raw = _read(pdef.primary_source, resolver[pdef.primary_source.name])

    # phase 1: build lookup dictionaries (broadcast reference tables)
    lut: Dict[str, Dict] = {}
    src_by_name = {pdef.primary_source.name: pdef.primary_source}
    for s in pdef.extra_sources:
        src_by_name[s.name] = s
    for lk in pdef.lookups:
        rows = _read(src_by_name[lk.source], resolver[lk.source]) if lk.source in resolver else []
        d = {}
        for r in rows:
            k = tuple(C._to_str(r.get(f, "")).strip() for f, _ in lk.on)
            d[k] = r
        lut[lk.name] = d

    # phase 2: classify record types and split header/trailer from detail
    rt = pdef.record_type
    detail: List[Dict[str, Any]] = []
    n_header = trailer_declared = 0
    for r in raw:
        if rt:
            flag = C.record_type_flag(r.get(rt["field"], ""),
                                      rt.get("header", "HEADER"),
                                      rt.get("trailer", "TRAILER"))
            if flag == "H":
                n_header += 1
                continue
            if flag == "T":
                if rt.get("trailer_count_expr"):
                    trailer_declared = C.to_integer(E.evaluate(rt["trailer_count_expr"], r)) or 0
                continue
            if flag not in pdef.detail_flags:
                continue
        detail.append(r)

    # phase 3: enrich (lookups) + evaluate derived ports + apply error rules
    good: List[Dict[str, Any]] = []
    errors: List[Dict[str, Any]] = []
    port_asts = [(dp.name, E.parse(dp.expr)) for dp in pdef.derived_ports]
    rule_asts = [(er.get("message", "transformation error"), er.get("code", "RULE"),
                  E.parse(er["expr"])) for er in pdef.error_rules]

    for r in detail:
        ns = dict(r)
        ns["__const_curr"] = "Y"
        for lk in pdef.lookups:
            k = tuple(C._to_str(ns.get(f, "")).strip() for f, _ in lk.on)
            hit = lut.get(lk.name, {}).get(k)
            for out_name, lk_field in lk.select:
                ns[out_name] = hit.get(lk_field) if hit else None
        err = None
        for name, ast in port_asts:
            try:
                ns[name] = E._eval(ast, ns)
            except C.TransformationError as te:
                err = (te.message, "XFORM")
                ns[name] = None
                break
        if err is None:
            for msg, code, ast in rule_asts:
                try:
                    if C._truthy(E._eval(ast, ns)):
                        err = (msg, code)
                        break
                except C.TransformationError as te:
                    err = (te.message, "XFORM")
                    break
        if err is None:
            good.append(ns)
        else:
            errors.append(_err_row(pdef, ns, err[0], err[1], ctx))

    # phase 4: Sorter "latest record wins" dedup
    if pdef.dedup:
        good = _dedup(good, pdef.dedup)

    # phase 5: project to the target contract
    main = []
    for ns in good:
        out = {}
        for col in pdef.target.columns:
            src_field = pdef.target.field_map.get(col.name, col.name)
            out[col.name] = R.format_value(ns.get(src_field), col)
        main.append(out)

    counters = _counters(pdef, ctx, len(raw), n_header, len(main), len(errors),
                         trailer_declared)
    res = Result(main=main, error=errors, counters=counters)
    res.metrics = {"engine": "baseline", "folder": pdef.folder,
                   "input_rows": len(raw), "detail_rows": len(main),
                   "error_rows": len(errors)}
    return res


def _dedup(rows, dedup):
    def key_order(r):
        parts = []
        for fld, direction in dedup.order:
            v = r.get(fld)
            if v is None:
                parts.append((2,))
            elif isinstance(v, (int, float, Decimal)):
                num = float(v)
                parts.append((0, -num if direction == "desc" else num))
            else:
                s = C._to_str(v)
                parts.append((0, tuple(-ord(c) for c in s) if direction == "desc" else s))
        parts.append((r.get(R.SEQ_COL, 0),))
        return tuple(parts)
    ordered = sorted(rows, key=key_order)
    keep, seen = [], set()
    for r in ordered:
        k = tuple(C._to_str(r.get(x, "")).strip() for x in dedup.keys)
        if k in seen:
            continue
        seen.add(k)
        keep.append(r)
    return keep


def _err_row(pdef, ns, msg, code, ctx):
    kf = (pdef.record_type or {}).get("key_field")
    return {
        "PROCESS_NAME": pdef.mapping,
        "ERROR_MESSAGE": msg,
        "SOURCE_KEY": C._to_str(ns.get(kf, "")).strip() if kf else None,
        "ERROR_DATE": ctx.run_ts,
        "PP_END_YEAR": ctx.pp_end_year,
        "PP_NUM": ctx.pp_num,
        "CYCLE_ID": ctx.cycle_id,
        "ERROR_CODE": code,
    }


def _counters(pdef, ctx, total, header, detail, errors, trailer_declared):
    items = [("TOTAL_RECORD_COUNT", total), ("HEADER_RECORD_COUNT", header),
             ("DETAIL_RECORD_COUNT", detail), ("ERROR_RECORD_COUNT", errors)]
    if pdef.record_type and pdef.record_type.get("trailer_count_expr"):
        items.append(("TRAILER_DECLARED_COUNT", trailer_declared))
        items.append(("TRAILER_MATCH", 1 if trailer_declared == detail + errors else 0))
    return [{
        "RUN_DATE": ctx.run_ts, "PROCESS_NAME": pdef.mapping,
        "COUNTER_DESCRIPTION": name, "COUNTER_VALUE": val,
        "PP_END_YEAR": ctx.pp_end_year, "PP_NUM": ctx.pp_num, "CYCLE_ID": ctx.cycle_id,
    } for name, val in items]
