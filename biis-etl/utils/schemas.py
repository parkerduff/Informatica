"""Schema registry derived from the Informatica PowerCenter XML exports.

The JSON files in ``schemas/`` were extracted verbatim from the SOURCE/TARGET
definitions in ``XML/*`` so that table DDL, Spark schemas and golden data all
share a single source of truth.
"""
import json
import os
from typing import Dict, List, Optional

SCHEMA_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "schemas")

_ORACLE_TO_SQLSERVER = {
    "number(p,s)": "DECIMAL",
    "number": "DECIMAL",
    "varchar2": "NVARCHAR",
    "varchar": "NVARCHAR",
    "char": "NCHAR",
    "date": "DATETIME2",
    "timestamp": "DATETIME2",
    "string": "NVARCHAR",
    "nstring": "NVARCHAR",
}


def load_module_schema(module_name: str) -> dict:
    path = os.path.join(SCHEMA_DIR, module_name + ".json")
    with open(path) as f:
        return json.load(f)


def get_table_fields(module_name: str, table_name: str, kind: str = "sources") -> List[dict]:
    schema = load_module_schema(module_name)
    return schema[kind][table_name]["fields"]


def oracle_type_to_sqlserver(datatype: str, precision: Optional[str], scale: Optional[str]) -> str:
    dt = (datatype or "").lower()
    base = _ORACLE_TO_SQLSERVER.get(dt)
    if base is None:
        return "NVARCHAR(255)"
    if base == "DECIMAL":
        p = int(precision or 18)
        s = int(scale or 0)
        p = max(1, min(p, 38))
        s = max(0, min(s, p))
        return f"DECIMAL({p},{s})"
    if base in ("NVARCHAR", "NCHAR"):
        p = int(precision or 255)
        p = max(1, min(p, 4000))
        return f"{base}({p})"
    return base


def sqlserver_column_def(field: dict) -> str:
    coltype = oracle_type_to_sqlserver(field.get("datatype"), field.get("precision"), field.get("scale"))
    nullable = "NOT NULL" if field.get("nullable") == "NOTNULL" else "NULL"
    return f"[{field['name']}] {coltype} {nullable}"


def spark_type_for(field: dict) -> str:
    """Return the pyspark.sql.types simple string for a field."""
    dt = (field.get("datatype") or "").lower()
    if dt in ("number(p,s)", "number"):
        s = int(field.get("scale") or 0)
        p = int(field.get("precision") or 18)
        p = max(1, min(p, 38))
        s = max(0, min(s, p))
        return f"decimal({p},{s})"
    if dt in ("date", "timestamp"):
        return "timestamp"
    return "string"


def fixed_width_layout(module_name: str, table_name: str, kind: str = "sources") -> List[dict]:
    """Return [(name, offset, length)] for fixed-width flat file parsing."""
    fields = get_table_fields(module_name, table_name, kind)
    layout = []
    cursor = 0
    for f in fields:
        length = int(f.get("physicallength") or f.get("precision") or 0)
        offset = f.get("physicaloffset")
        offset = int(offset) if offset not in (None, "") else cursor
        layout.append({"name": f["name"], "offset": offset, "length": length})
        cursor = offset + length
    return layout


def column_names(module_name: str, table_name: str, kind: str = "sources") -> List[str]:
    return [f["name"] for f in get_table_fields(module_name, table_name, kind)]


def all_modules() -> Dict[str, dict]:
    out = {}
    for fn in sorted(os.listdir(SCHEMA_DIR)):
        if fn.endswith(".json"):
            out[fn[:-5]] = load_module_schema(fn[:-5])
    return out
