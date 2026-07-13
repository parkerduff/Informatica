#!/usr/bin/env python3
"""
pipeline -- declarative model of a converted Informatica mapping.

A ``PipelineDef`` is a faithful, traceable re-expression of one Informatica
mapping: it records the source layout (fixed-width offsets / delimited /
relational), the ordered derived ports (verbatim Informatica expressions from
the XML), record-type flagging, dedup (Sorter) semantics, broadcast lookups,
error-routing rules and the target field contracts.

The definition is engine-agnostic.  It is executed by:
  * ``engine_pandas.run``  -> Glue Python Shell jobs (light feeds)
  * ``engine_spark.run``   -> Glue PySpark jobs (wide feeds)
  * ``baseline.reference.run`` -> the independent, spec-derived golden baseline

so the same contract is checked from three directions during reconciliation.
"""
from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

SPEC_DIR = os.path.join(os.path.dirname(__file__), "..", "spec")


# ---------------------------------------------------------------------------
# Field / source / target contracts
# ---------------------------------------------------------------------------


@dataclass
class Column:
    name: str
    datatype: str = "string"
    precision: Optional[int] = None
    scale: Optional[int] = 0
    nullable: bool = True
    # fixed-width layout (inbound flat files)
    offset: Optional[int] = None
    length: Optional[int] = None


@dataclass
class SourceDef:
    name: str
    kind: str  # 'fixedwidth' | 'delimited' | 'relational'
    codepage: str = "Latin1"
    delimiter: str = ","
    columns: List[Column] = field(default_factory=list)
    # relative path under the data root (set by orchestration / local runtime)
    path: Optional[str] = None


@dataclass
class DerivedPort:
    """A variable (v_) or output (o_) port: name = <Informatica expression>."""
    name: str
    expr: str
    note: str = ""


@dataclass
class Lookup:
    """Cached lookup -> broadcast join in Spark."""
    name: str
    source: str            # SourceDef name providing the lookup rows
    on: List[Tuple[str, str]]   # [(row_field, lookup_field)]
    select: List[Tuple[str, str]]  # [(output_name, lookup_field)]


@dataclass
class Dedup:
    """Informatica Sorter 'latest record wins' -> Window + row_number()."""
    keys: List[str]
    order: List[Tuple[str, str]]  # [(field, 'asc'|'desc')]


@dataclass
class TargetDef:
    name: str
    kind: str  # 'relational' | 'flatfile'
    columns: List[Column] = field(default_factory=list)
    # maps target column name -> source/derived field name feeding it
    field_map: Dict[str, str] = field(default_factory=dict)


@dataclass
class PipelineDef:
    folder: str
    mapping: str
    engine: str                    # 'pyspark' | 'python_shell'
    primary_source: SourceDef
    extra_sources: List[SourceDef] = field(default_factory=list)
    derived_ports: List[DerivedPort] = field(default_factory=list)
    record_type: Optional[Dict[str, Any]] = None   # {'field','header','trailer'}
    lookups: List[Lookup] = field(default_factory=list)
    dedup: Optional[Dedup] = None
    # error routing: list of {'name','expr','message'}; expr TRUE -> route to error
    error_rules: List[Dict[str, str]] = field(default_factory=list)
    # only rows whose record_type flag is in this set become detail output
    detail_flags: Tuple[str, ...] = ("D",)
    target: Optional[TargetDef] = None
    error_target: str = "ERROR_TBL"
    counter_target: str = "COUNTER_TBL"
    partition_keys: List[str] = field(default_factory=list)
    description: str = ""


# ---------------------------------------------------------------------------
# Spec loading helpers -- build Column lists straight from parsed XML metadata.
# ---------------------------------------------------------------------------


def load_spec(folder: str) -> Dict[str, Any]:
    with open(os.path.join(SPEC_DIR, f"{folder}.json"), encoding="utf-8") as fh:
        return json.load(fh)


def _col_from_field(f: Dict[str, Any]) -> Column:
    return Column(
        name=f["name"],
        datatype=f.get("datatype") or "string",
        precision=f.get("precision"),
        scale=f.get("scale") or 0,
        nullable=(f.get("nullable") != "NOTNULL"),
        offset=f.get("physicaloffset"),
        length=f.get("physicallength"),
    )


def source_columns(spec: Dict[str, Any], source_name: str) -> List[Column]:
    for s in spec["sources"]:
        if s["name"] == source_name:
            return [_col_from_field(f) for f in s["fields"]]
    raise KeyError(f"source {source_name} not found in {spec['folder']}")


def source_flatfile(spec: Dict[str, Any], source_name: str) -> Optional[Dict[str, Any]]:
    for s in spec["sources"]:
        if s["name"] == source_name:
            return s.get("flatfile")
    return None


def target_columns(spec: Dict[str, Any], target_name: str) -> List[Column]:
    for t in spec["targets"]:
        if t["name"] == target_name:
            return [_col_from_field(f) for f in t["fields"]]
    raise KeyError(f"target {target_name} not found in {spec['folder']}")


def is_fixed_width(columns: List[Column]) -> bool:
    return any(c.offset is not None and c.length is not None for c in columns)
