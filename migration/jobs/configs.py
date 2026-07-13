#!/usr/bin/env python3
"""
configs -- build a faithful ``PipelineDef`` for each of the eight folders.

The field maps and derived transformation ports are reconstructed automatically
from the parsed spec (via ``mapping_resolver``), so the conversion stays grounded
in the actual PowerCenter CONNECTOR lineage and TRANSFORMFIELD expressions rather
than hand-written guesses.  Per-folder metadata (primary mapping/source/target,
Sorter dedup keys, record-type flagging, lookups, error rules) is declared in
``FOLDER_META`` and mirrors the XML.
"""
from __future__ import annotations

import os
import re
import sys
from typing import Any, Dict, List, Optional

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from migration.lib import mapping_resolver as MR
from migration.lib.pipeline import (
    Column,
    Dedup,
    DerivedPort,
    Lookup,
    PipelineDef,
    SourceDef,
    TargetDef,
    is_fixed_width,
    load_spec,
    source_columns,
    source_flatfile,
    target_columns,
)

# ---------------------------------------------------------------------------
# Per-folder metadata.  Everything here is read straight from the XML exports.
# ---------------------------------------------------------------------------

FOLDER_META: Dict[str, Dict[str, Any]] = {
    "Pseudossn": {
        "engine": "pyspark",
        "mapping": "m_Pseudossn_Load_Pseudossn_Tbl",
        "primary_source": "PSEUDOSSN_FILE",
        "target": "PSEUDOSSN_TBL",
        "record_type": {"field": "SSN", "header": "HEADER", "trailer": "TRAILER",
                         "key_field": "PSEUDO_SSN",
                         "trailer_count_expr": "TO_INTEGER(LTRIM(SUBSTR(CAN_CD,1,8)))"},
        "dedup": {"keys": ["PSEUDO_SSN"], "order": [("EFFECTIVE_DATE", "desc"),
                                                     ("EFFECTIVE_SEQ", "desc")]},
        "lookups": [
            {"name": "lkp_Current_Pay_Period", "source": "PAY_PERIOD",
             "on": [("__const_curr", "CURR_PP_FLAG")],
             "select": [("PP_NUM", "PP_NUM"), ("PP_END_YEAR", "PP_END_YEAR")]},
        ],
        "error_rules": [
            {"name": "bad_effective_date", "code": "BADDATE",
             "expr": "LENGTH(LTRIM(RTRIM(EFFECTIVE_DATE))) > 0 AND NOT IS_DATE("
                     "SUBSTR(EFFECTIVE_DATE,7,2)||'/'||SUBSTR(EFFECTIVE_DATE,5,2)||'/'"
                     "||SUBSTR(EFFECTIVE_DATE,1,4),'MM/DD/YYYY')",
             "message": "Invalid EFFECTIVE_DATE"},
            {"name": "non_numeric_ssn", "code": "BADSSN",
             "expr": "NOT IS_NUMBER(PSEUDO_SSN)",
             "message": "PSEUDO_SSN is not numeric"},
        ],
        "partition_keys": ["PSEUDO_SSN"],
    },
    "COMPTIME": {
        "engine": "python_shell",
        "mapping": "m_COMPTIME_Load_COMP_TIME_DAILY_TBL",
        "primary_source": "U0287D01",
        "target": "COMP_TIME_DAILY_TBL",
        "lookups": [
            {"name": "lkp_PAY_PERIOD", "source": "PAY_PERIOD",
             "on": [("__const_curr", "CURR_PP_FLAG")],
             "select": [("PP_NUM", "PP_NUM"), ("PP_END_YEAR", "PP_END_YEAR")]},
        ],
        "error_rules": [
            {"name": "bad_pp_end_date", "code": "BADDATE",
             "expr": "LENGTH(LTRIM(RTRIM(PP_END_DATE)))>0 AND NOT IS_DATE(PP_END_DATE,'YYYYMMDD')",
             "message": "Invalid PP_END_DATE"},
        ],
    },
    "Pay_Calendar": {
        "engine": "python_shell",
        "mapping": "m_Pay_Calendar_Set_Pay_Calendar",
        "primary_source": "PAY_PERIOD",
        "target": "PAY_PERIOD",
        "error_rules": [],
    },
    "CPM_NIH": {
        "engine": "pyspark",
        "mapping": "m_CPM_NIH_Load_CPM_NIH_Data_File",
        "primary_source": "CPM_NEWPAY_TBL",
        "target": "nihtest_NIH_PAYROLL_MASTER",
        "partition_keys": ["EMPLID"],
        "error_rules": [],
    },
    "CPM_CDC": {
        "engine": "pyspark",
        "mapping": "m_CPM_CDC_Load_CPM_CDC_Data_File",
        "primary_source": "CPM_NEWPAY_TBL",
        "target": "cdcskel_WS_PAY_OUT_REC",
        "partition_keys": ["EMPLID"],
        "error_rules": [],
    },
    "CPM_OIG": {
        "engine": "pyspark",
        "mapping": "m_CPM_OIG_Load_CPM_OIG_File",
        "primary_source": "CPM_NEWPAY_TBL",
        "target": "oigsgndec_SKPAYROLL_MASTER",
        "partition_keys": ["EMPLID"],
        "error_rules": [],
    },
    "EHRP2BIIS_UPDATE": {
        "engine": "pyspark",
        "mapping": "m_EHRP2BIIS_UPDATE",
        "primary_source": "NWK_NEW_EHRP_ACTIONS_TBL",
        "target": "NWK_ACTION_PRIMARY_TBL",
        "lookups": [
            {"name": "lkp_PS_GVT_JOB", "source": "PS_GVT_JOB",
             "on": [("EMPLID", "EMPLID")],
             "select": []},  # select filled dynamically (all overlapping cols)
        ],
        "partition_keys": ["EMPLID"],
        "error_rules": [],
    },
    "FDA_Leave": {
        "engine": "pyspark",
        "mapping": "m_0100_PM_FDA_Load_TATRAN_To_DB",
        "primary_source": "HI_PM_FDA_TATRAN_FLAT",
        "target": "HI_PM_FDA_TATRAN_TBL",
        "partition_keys": [],
        "error_rules": [],
    },
}


def _mk_source(spec: Dict[str, Any], name: str) -> SourceDef:
    cols = source_columns(spec, name)
    ff = source_flatfile(spec, name)
    if ff is None:
        kind = "relational"
        codepage = "utf-8"
        delim = ","
    elif ff.get("delimited"):
        kind = "delimited"
        codepage = ff.get("codepage") or "Latin1"
        delim = ff.get("delimiters") or ","
    elif is_fixed_width(cols):
        kind = "fixedwidth"
        codepage = ff.get("codepage") or "Latin1"
        delim = ","
    else:
        kind = "delimited"
        codepage = ff.get("codepage") or "Latin1"
        delim = ff.get("delimiters") or ","
    return SourceDef(name=name, kind=kind, codepage=codepage, delimiter=delim, columns=cols)


def _mk_target(spec: Dict[str, Any], name: str, mapping: Dict[str, Any]) -> TargetDef:
    cols = target_columns(spec, name)
    fmap = MR.field_map(mapping, name)
    dbtype = next((t["database_type"] for t in spec["targets"] if t["name"] == name), "")
    kind = "flatfile" if "flat" in (dbtype or "").lower() or "SEQ" in (dbtype or "") else "relational"
    return TargetDef(name=name, kind=kind, columns=cols, field_map=fmap)


def _derived_program(mapping: Dict[str, Any], target_name: str) -> List[DerivedPort]:
    """Collect ordered derived ports (input aliases + variable/output ports).

    We include every Expression transform that produces a target field, in
    topological order, prefixing each transform's input-port aliases so renamed
    ports resolve to their upstream source/derived value.
    """
    lineage = MR.resolve_target(mapping, target_name)
    expr_transforms: List[str] = []
    for lin in lineage.values():
        if lin["kind"] == "expr" and lin["transform"] not in expr_transforms:
            expr_transforms.append(lin["transform"])
    order = MR.topo_order(mapping)
    expr_transforms.sort(key=lambda t: order.get(t, 9999))

    ports: List[DerivedPort] = []
    seen: set = set()
    for t in expr_transforms:
        for alias, lin in MR.input_aliases(mapping, t).items():
            if lin.get("kind") == "source":
                src = lin["source_field"]
                if alias != src and alias not in seen:
                    ports.append(DerivedPort(name=alias, expr=src, note=f"alias<-{src}"))
                    seen.add(alias)
        for name, expr in MR.derived_ports(mapping, t):
            if name in seen or not expr:
                continue
            ports.append(DerivedPort(name=name, expr=expr, note=t))
            seen.add(name)
    return ports


def build(folder: str) -> PipelineDef:
    meta = FOLDER_META[folder]
    spec = load_spec(folder)
    mapping = next(m for m in spec["mappings"] if m["name"] == meta["mapping"])

    primary = _mk_source(spec, meta["primary_source"])
    target = _mk_target(spec, meta["target"], mapping)
    derived = _derived_program(mapping, meta["target"])

    extra_sources: List[SourceDef] = []
    lookups: List[Lookup] = []
    for lk in meta.get("lookups", []):
        extra_sources.append(_mk_source(spec, lk["source"]))
        select = lk["select"]
        if not select:  # e.g. join enrichment -- take overlapping cols
            src_cols = {c.name for c in extra_sources[-1].columns}
            tgt_cols = [c.name for c in target.columns]
            select = [(c, c) for c in tgt_cols if c in src_cols]
        lookups.append(Lookup(name=lk["name"], source=lk["source"],
                              on=[tuple(p) for p in lk["on"]],
                              select=[tuple(s) for s in select]))

    dedup = None
    if meta.get("dedup"):
        dedup = Dedup(keys=meta["dedup"]["keys"],
                      order=[tuple(o) for o in meta["dedup"]["order"]])

    pdef = PipelineDef(
        folder=folder,
        mapping=meta["mapping"],
        engine=meta["engine"],
        primary_source=primary,
        extra_sources=extra_sources,
        derived_ports=derived,
        record_type=meta.get("record_type"),
        lookups=lookups,
        dedup=dedup,
        error_rules=meta.get("error_rules", []),
        target=target,
        partition_keys=meta.get("partition_keys", []),
        description=f"Converted from PowerCenter mapping {meta['mapping']}",
    )
    return pdef


ALL_FOLDERS = list(FOLDER_META.keys())
