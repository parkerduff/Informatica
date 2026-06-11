#!/usr/bin/env python3
"""Generate jobs/schemas.py column specs from the Informatica XML definitions.

Each generated entry is (column_name, oracle_datatype, precision, scale). The
module is imported by both the PySpark jobs and the synthetic golden generator
so the two never drift. Re-run only when the upstream XML changes.
"""
from __future__ import annotations

import re
import xml.etree.ElementTree as ET
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]  # repo root (contains XML/)
OUT = Path(__file__).resolve().parents[1] / "jobs" / "schemas.py"

SPECS = [
    ("PS_GVT_JOB", "XML/EHRP2BIIS_UPDATE", "SOURCE"),
    ("NWK_ACTION_PRIMARY_TBL", "XML/EHRP2BIIS_UPDATE", "TARGET"),
    ("NWK_ACTION_SECONDARY_TBL", "XML/EHRP2BIIS_UPDATE", "TARGET"),
    ("CPM_NEWPAY_TBL", "XML/CPM_CDC", "SOURCE"),
    ("PSEUDOSSN_FROM_SDA_TBL", "XML/Pseudossn", "SOURCE"),
]


def parse(path: Path) -> ET.Element:
    data = re.sub(r"<!DOCTYPE[^>]*>", "", path.read_text(encoding="latin-1"))
    return ET.fromstring(data)


def cols(path: Path, name: str, tag: str):
    root = parse(path)
    best = None
    for e in root.iter(tag):
        if e.get("NAME", "").upper() == name.upper() and e.get("DATABASETYPE", "") == "Oracle":
            fields = e.findall(f"{tag}FIELD")
            if best is None or len(fields) > len(best):
                best = fields
    return [(f.get("NAME"), (f.get("DATATYPE") or "").lower(), f.get("PRECISION"), f.get("SCALE"))
            for f in best]


def render(name: str, lst) -> str:
    out = [f"{name} = ["]
    for n, dt, p, sc in lst:
        out.append(f"    ({n!r}, {dt!r}, {p!r}, {sc!r}),")
    out.append("]")
    return "\n".join(out)


def main() -> int:
    parts = ['"""Auto-generated column specs from Informatica XML. Do not edit by hand.',
             "",
             "Each entry: (column_name, oracle_datatype, precision, scale).",
             "Imported by both the PySpark jobs and the synthetic golden generator so the",
             "two never drift. Regenerate with scripts/gen_schemas.py if the XML changes.",
             '"""',
             ""]
    for name, fn, tag in SPECS:
        parts.append(render(name + "_COLS", cols(ROOT / fn, name, tag)))
        parts.append("")
    OUT.write_text("\n".join(parts).rstrip("\n") + "\n")
    print(f"Wrote {OUT}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
