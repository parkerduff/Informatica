#!/usr/bin/env python3
"""
records -- shared source readers and target formatters (pure Python).

Reading (codepage decoding, fixed-width slicing, delimited splitting, relational
CSV) and value formatting (precision/scale, trailing-blank handling) are
mechanical and identical for every engine, so they live here.  The actual
*transformation semantics* live in infa_compat / infa_expr, and the *execution*
(distributed vs single-node, error routing, dedup) is what each engine
implements independently.
"""
from __future__ import annotations

import csv
import os
from decimal import Decimal
from typing import Any, Dict, Iterator, List, Optional

from . import infa_compat as C
from .pipeline import Column, SourceDef

SEQ_COL = "__seq"


def read_source(src: SourceDef, path: str) -> List[Dict[str, Any]]:
    """Read a source file/table into a list of raw string-valued row dicts.

    A monotonically increasing ``__seq`` column is added so every engine breaks
    dedup / ordering ties identically.
    """
    if src.kind == "fixedwidth":
        return _read_fixedwidth(src, path)
    if src.kind == "delimited":
        return _read_delimited(src, path)
    if src.kind == "relational":
        return _read_relational(src, path)
    raise ValueError(f"unknown source kind {src.kind}")


def _read_fixedwidth(src: SourceDef, path: str) -> List[Dict[str, Any]]:
    cp = C.resolve_codepage(src.codepage)
    rows: List[Dict[str, Any]] = []
    seq = 0
    with open(path, "r", encoding=cp, errors="replace", newline="") as fh:
        for line in fh:
            line = line.rstrip("\r\n")
            if line == "":
                continue
            row: Dict[str, Any] = {SEQ_COL: seq}
            for c in src.columns:
                if c.offset is None or c.length is None:
                    continue
                row[c.name] = C.slice_fixed(line, c.offset, c.length)
            rows.append(row)
            seq += 1
    return rows


def _read_delimited(src: SourceDef, path: str) -> List[Dict[str, Any]]:
    cp = C.resolve_codepage(src.codepage)
    rows: List[Dict[str, Any]] = []
    seq = 0
    with open(path, "r", encoding=cp, errors="replace", newline="") as fh:
        reader = csv.reader(fh, delimiter=src.delimiter or ",")
        names = [c.name for c in src.columns]
        for parts in reader:
            if not parts or (len(parts) == 1 and parts[0] == ""):
                continue
            row: Dict[str, Any] = {SEQ_COL: seq}
            for i, name in enumerate(names):
                row[name] = parts[i] if i < len(parts) else ""
            rows.append(row)
            seq += 1
    return rows


def _read_relational(src: SourceDef, path: str) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    seq = 0
    with open(path, "r", encoding="utf-8", newline="") as fh:
        reader = csv.DictReader(fh)
        for d in reader:
            row: Dict[str, Any] = {SEQ_COL: seq}
            for c in src.columns:
                row[c.name] = d.get(c.name, "")
            rows.append(row)
            seq += 1
    return rows


# ---------------------------------------------------------------------------
# Value formatting for target contracts (precision / scale / trailing blanks).
# ---------------------------------------------------------------------------


def format_value(value: Any, col: Column) -> Any:
    if value is None:
        return None
    dt = (col.datatype or "").lower()
    if any(k in dt for k in ("number", "decimal", "numeric", "integer", "double")):
        scale = col.scale or 0
        d = C.to_decimal(value, scale)
        if d is None:
            return None
        return format(d, "f")
    if "date" in dt:
        return C.to_char(value, "YYYY-MM-DD HH24:MI:SS") if not isinstance(value, str) else value
    s = C._to_str(value)
    return s.rstrip(" ")


def canonical(value: Any) -> str:
    """Canonical string form for field-level reconciliation comparisons."""
    if value is None:
        return "\x00NULL\x00"
    if isinstance(value, Decimal):
        return format(value, "f")
    return str(value)
