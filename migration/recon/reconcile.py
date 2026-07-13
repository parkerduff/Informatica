#!/usr/bin/env python3
"""
recon.reconcile -- field-level reconciliation of converted-job output against
the spec-derived golden baseline.

Checks, per job:
  * row counts (main / error / counter targets),
  * field-level diffs per column (precision/scale-aware for numerics),
  * null-handling parity,
  * dedup / latest-record parity (implicitly, via ordered main comparison),
  * error-routing parity (same source keys land in ERROR_TBL),
  * record-count / audit (COUNTER_TBL) parity.

Produces a JSON-serialisable verdict consumed by the HTML reports.
"""
from __future__ import annotations

from decimal import Decimal, InvalidOperation
from typing import Any, Dict, List, Tuple


def _canon(v: Any) -> str:
    if v is None:
        return "\x00NULL"
    if isinstance(v, Decimal):
        return format(v.normalize(), "f")
    if isinstance(v, float):
        return format(Decimal(str(v)).normalize(), "f")
    if isinstance(v, bool):
        return "TRUE" if v else "FALSE"
    return str(v)


def _num_equal(a: Any, b: Any, scale: int) -> bool:
    try:
        da, db = Decimal(str(a)), Decimal(str(b))
    except (InvalidOperation, ValueError, TypeError):
        return False
    if not (da.is_finite() and db.is_finite()):
        return da == db
    q = Decimal(10) ** -(scale or 0)
    try:
        return da.quantize(q) == db.quantize(q)
    except InvalidOperation:
        # value magnitude exceeds the default context precision for quantize;
        # compare normalised forms instead (scale-agnostic exact compare).
        return da.normalize() == db.normalize()


def compare_rows(conv: List[Dict[str, Any]], base: List[Dict[str, Any]],
                 columns: List[Dict[str, Any]]) -> Dict[str, Any]:
    n = min(len(conv), len(base))
    col_names = [c["name"] for c in columns] if columns else (
        list(base[0].keys()) if base else [])
    col_scale = {c["name"]: (c.get("scale") or 0) for c in columns}
    col_type = {c["name"]: (c.get("datatype") or "") for c in columns}

    field_total: Dict[str, int] = {c: 0 for c in col_names}
    field_mismatch: Dict[str, int] = {c: 0 for c in col_names}
    null_mismatch = 0
    examples: List[Dict[str, Any]] = []

    for i in range(n):
        rc, rb = conv[i], base[i]
        for c in col_names:
            field_total[c] += 1
            vc, vb = rc.get(c), rb.get(c)
            if (vc is None) != (vb is None):
                null_mismatch += 1
                field_mismatch[c] += 1
                if len(examples) < 25:
                    examples.append({"row": i, "col": c, "conv": _canon(vc), "base": _canon(vb)})
                continue
            if vc is None and vb is None:
                continue
            equal = False
            if "number" in col_type.get(c, "").lower() or "dec" in col_type.get(c, "").lower():
                equal = _num_equal(vc, vb, col_scale.get(c, 0))
            if not equal:
                equal = _canon(vc) == _canon(vb)
            if not equal:
                field_mismatch[c] += 1
                if len(examples) < 25:
                    examples.append({"row": i, "col": c, "conv": _canon(vc), "base": _canon(vb)})

    total_cells = sum(field_total.values())
    total_mismatch = sum(field_mismatch.values())
    return {
        "rows_compared": n,
        "total_cells": total_cells,
        "cell_mismatches": total_mismatch,
        "null_mismatches": null_mismatch,
        "field_match_rate": (1.0 if total_cells == 0 else 1 - total_mismatch / total_cells),
        "per_field_mismatch": {c: field_mismatch[c] for c in col_names if field_mismatch[c]},
        "examples": examples,
    }


def _error_keys(rows: List[Dict[str, Any]]) -> List[str]:
    return sorted(_canon(r.get("SOURCE_KEY")) for r in rows)


def _counter_map(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    return {r.get("COUNTER_DESCRIPTION"): r.get("COUNTER_VALUE") for r in rows}


def reconcile(folder: str, conv, base, columns: List[Dict[str, Any]]) -> Dict[str, Any]:
    row_counts = {
        "main": {"conv": len(conv.main), "base": len(base.main),
                  "match": len(conv.main) == len(base.main)},
        "error": {"conv": len(conv.error), "base": len(base.error),
                   "match": len(conv.error) == len(base.error)},
        "counter": {"conv": len(conv.counters), "base": len(base.counters),
                     "match": len(conv.counters) == len(base.counters)},
    }
    field = compare_rows(conv.main, base.main, columns)

    ck, bk = _error_keys(conv.error), _error_keys(base.error)
    error_parity = {
        "conv_count": len(ck), "base_count": len(bk),
        "keys_match": ck == bk,
        "only_conv": sorted(set(ck) - set(bk))[:20],
        "only_base": sorted(set(bk) - set(ck))[:20],
    }

    cc, bc = _counter_map(conv.counters), _counter_map(base.counters)
    counter_parity = {
        "match": cc == bc,
        "conv": cc, "base": bc,
        "diffs": {k: {"conv": cc.get(k), "base": bc.get(k)}
                  for k in set(cc) | set(bc) if cc.get(k) != bc.get(k)},
    }

    passed = (
        row_counts["main"]["match"] and row_counts["error"]["match"]
        and field["cell_mismatches"] == 0 and error_parity["keys_match"]
        and counter_parity["match"]
    )
    return {
        "folder": folder,
        "row_counts": row_counts,
        "field": field,
        "error_parity": error_parity,
        "counter_parity": counter_parity,
        "verdict": "PASS" if passed else "FAIL",
        "conv_metrics": conv.metrics,
        "base_metrics": base.metrics,
    }
