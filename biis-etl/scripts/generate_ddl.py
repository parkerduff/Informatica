#!/usr/bin/env python3
"""Generate sql/ddl/001_create_all_tables.sql from the Informatica PowerCenter
XML source/target definitions (Oracle) converted to SQL Server (T-SQL) syntax.

This is a build-time helper that documents the Oracle -> SQL Server type
mapping. The generated SQL file is the canonical, committed artifact consumed
by scripts/apply_ddl.py; this generator only needs to be re-run when the
upstream Informatica XML definitions change.

Type mapping (Oracle -> SQL Server):
    NUMBER(p,s) / number  -> DECIMAL(p,s)
    VARCHAR2(n)           -> NVARCHAR(n)
    DATE                  -> DATETIME2
"""
from __future__ import annotations

import argparse
import re
import xml.etree.ElementTree as ET
from pathlib import Path

# Oracle tables to extract from the XML (table_name -> source XML file). When a
# table appears in multiple XMLs the richest definition is used.
TABLES_FROM_XML = {
    "PAY_PERIOD": "Pay_Calendar",
    "COMP_TIME_DAILY_TBL": "COMPTIME",
    "COUNTER_TBL": "COMPTIME",
    "PSEUDOSSN_FROM_SDA_TBL": "Pseudossn",
    "PSEUDOSSN_TBL": "Pseudossn",
    "HI_ARCH_PSEUDOSSN_TBL": "Pseudossn",
    "ERROR_TBL": "Pseudossn",
    "HI_PM_FDA_TATRAN_TBL": "FDA_Leave",
    "CPM_CYCLE_TBL": "FDA_Leave",
    "CPM_YTD_DETAIL_STG_TBL": "CPM",
    "CPM_PAD_DETAIL_STG_TBL": "CPM",
    "CPM_MER_DETAIL_STG_TBL": "CPM",
    "NWK_NEW_EHRP_ACTIONS_TBL": "EHRP2BIIS_UPDATE",
    "PS_GVT_JOB": "EHRP2BIIS_UPDATE",
    "NWK_ACTION_PRIMARY_TBL": "EHRP2BIIS_UPDATE",
    "NWK_ACTION_SECONDARY_TBL": "EHRP2BIIS_UPDATE",
    "EHRP_RECS_TRACKING_TBL": "EHRP2BIIS_UPDATE",
    "CPM_NEWPAY_TBL": "CPM_CDC",
}


def parse_xml(path: Path) -> ET.Element:
    data = re.sub(r"<!DOCTYPE[^>]*>", "", path.read_text(encoding="latin-1"))
    return ET.fromstring(data)


def find_table(root: ET.Element, name: str):
    """Return (tag, element) for the richest SOURCE/TARGET Oracle def of name."""
    best = None
    for tag in ("SOURCE", "TARGET"):
        for e in root.iter(tag):
            if e.get("NAME", "").upper() != name.upper():
                continue
            if e.get("DATABASETYPE", "") != "Oracle":
                continue
            nf = len(e.findall(f"{tag}FIELD"))
            if best is None or nf > best[2]:
                best = (tag, e, nf)
    if best is None:
        return None
    return best[0], best[1]


def map_type(datatype: str, precision: str, scale: str) -> str:
    dt = (datatype or "").lower()
    p = int(precision) if precision and precision.isdigit() else 0
    s = int(scale) if scale and scale.isdigit() else 0
    if dt.startswith("date") or dt == "timestamp":
        return "DATETIME2"
    if dt.startswith("number") or dt in ("decimal", "numeric"):
        p = max(p, 1)
        p = min(p, 38)
        if s > p:
            p = s
        return f"DECIMAL({p},{s})"
    # varchar2 / string / char
    n = max(p, 1)
    return f"NVARCHAR({n})"


def column_defs(tag: str, elem: ET.Element):
    cols = []
    pk = []
    for f in elem.findall(f"{tag}FIELD"):
        name = f.get("NAME")
        sqltype = map_type(f.get("DATATYPE"), f.get("PRECISION"), f.get("SCALE"))
        nullable = "NULL"
        if (f.get("NULLABLE") or "").upper() in ("NOTNULL", "NOT NULL"):
            nullable = "NOT NULL"
        cols.append((name, sqltype, nullable))
        if (f.get("KEYTYPE") or "").upper() == "PRIMARY KEY":
            pk.append(name)
    return cols, pk


def render_table(name: str, cols, pk, extra_cols=None) -> str:
    extra_cols = extra_cols or []
    seen = {c[0].upper() for c in cols}
    for ec in extra_cols:
        if ec[0].upper() not in seen:
            cols.append(ec)
    lines = [f"IF OBJECT_ID('dbo.{name}', 'U') IS NOT NULL DROP TABLE dbo.{name};",
             f"CREATE TABLE dbo.{name} ("]
    body = []
    for cname, ctype, cnull in cols:
        body.append(f"    [{cname}] {ctype} {cnull}")
    if pk:
        pk_cols = ", ".join(f"[{c}]" for c in pk)
        body.append(f"    CONSTRAINT [PK_{name}] PRIMARY KEY ({pk_cols})")
    lines.append(",\n".join(body))
    lines.append(");")
    return "\n".join(lines)


# Audit/derived columns added by the PySpark jobs that are not in the source XML.
EXTRA_COLUMNS = {
    "COMP_TIME_DAILY_TBL": [("SSN_HASH", "NVARCHAR(64)", "NULL"),
                            ("LOAD_DATE", "DATETIME2", "NULL")],
    "PSEUDOSSN_FROM_SDA_TBL": [("LOAD_DATE", "DATETIME2", "NULL")],
    "PSEUDOSSN_TBL": [("LOAD_DATE", "DATETIME2", "NULL")],
    "HI_ARCH_PSEUDOSSN_TBL": [("LOAD_DATE", "DATETIME2", "NULL")],
    "NWK_ACTION_PRIMARY_TBL": [("LOAD_DATE", "DATETIME2", "NULL")],
    "NWK_ACTION_SECONDARY_TBL": [("LOAD_DATE", "DATETIME2", "NULL")],
    "EHRP_RECS_TRACKING_TBL": [("LOAD_DATE", "DATETIME2", "NULL")],
    "CPM_NEWPAY_TBL": [("LOAD_DATE", "DATETIME2", "NULL")],
}

# Hand-written tables that have no Informatica XML definition (they live only in
# the Oracle database, referenced by ehrp2biis_afterload.sql / shell scripts).
HANDWRITTEN = """
-- ---------------------------------------------------------------------------
-- Sequence number tracking (driven by update_sequence_number_tbl_p)
-- ---------------------------------------------------------------------------
IF OBJECT_ID('dbo.SEQUENCE_NUM_TBL', 'U') IS NOT NULL DROP TABLE dbo.SEQUENCE_NUM_TBL;
CREATE TABLE dbo.SEQUENCE_NUM_TBL (
    [SEQ_NAME] NVARCHAR(50) NOT NULL,
    [CURRENT_VALUE] DECIMAL(15,0) NOT NULL,
    [LAST_UPDATED] DATETIME2 NULL,
    CONSTRAINT [PK_SEQUENCE_NUM_TBL] PRIMARY KEY ([SEQ_NAME])
);

-- ---------------------------------------------------------------------------
-- Process control table (WIP status driver)
-- ---------------------------------------------------------------------------
IF OBJECT_ID('dbo.PROCESS_TABLE', 'U') IS NOT NULL DROP TABLE dbo.PROCESS_TABLE;
CREATE TABLE dbo.PROCESS_TABLE (
    [PROCESS_NAME] NVARCHAR(100) NULL,
    [P_STARTDT] DATETIME2 NULL,
    [P_ENDDT] DATETIME2 NULL
);

-- ---------------------------------------------------------------------------
-- Remarks staging (derived by remarks formatting procedures)
-- ---------------------------------------------------------------------------
IF OBJECT_ID('dbo.NWK_ACTION_REMARKS_TBL', 'U') IS NOT NULL DROP TABLE dbo.NWK_ACTION_REMARKS_TBL;
CREATE TABLE dbo.NWK_ACTION_REMARKS_TBL (
    [EVENT_ID] DECIMAL(15,0) NOT NULL,
    [REMARK_SEQ] DECIMAL(5,0) NOT NULL,
    [REMARK_CD] NVARCHAR(10) NULL,
    [REMARK_TEXT] NVARCHAR(500) NULL,
    [LOAD_DATE] DATETIME2 NULL,
    CONSTRAINT [PK_NWK_ACTION_REMARKS_TBL] PRIMARY KEY ([EVENT_ID], [REMARK_SEQ])
);

-- ---------------------------------------------------------------------------
-- CPM agency-specific validation staging tables (one row per output record)
-- ---------------------------------------------------------------------------
IF OBJECT_ID('dbo.CPM_NIH_STAGING_TBL', 'U') IS NOT NULL DROP TABLE dbo.CPM_NIH_STAGING_TBL;
CREATE TABLE dbo.CPM_NIH_STAGING_TBL (
    [PP_END_YEAR] DECIMAL(4,0) NULL,
    [PP_NUM] DECIMAL(2,0) NULL,
    [SSN] NVARCHAR(9) NULL,
    [AGENCY_CD] NVARCHAR(4) NULL,
    [GROSS_PAY] DECIMAL(12,2) NULL,
    [NET_PAY] DECIMAL(12,2) NULL,
    [RECORD_TEXT] NVARCHAR(512) NULL,
    [LOAD_DATE] DATETIME2 NULL
);

IF OBJECT_ID('dbo.CPM_OIG_STAGING_TBL', 'U') IS NOT NULL DROP TABLE dbo.CPM_OIG_STAGING_TBL;
CREATE TABLE dbo.CPM_OIG_STAGING_TBL (
    [PP_END_YEAR] DECIMAL(4,0) NULL,
    [PP_NUM] DECIMAL(2,0) NULL,
    [SSN] NVARCHAR(9) NULL,
    [AGENCY_CD] NVARCHAR(4) NULL,
    [GROSS_PAY] DECIMAL(12,2) NULL,
    [NET_PAY] DECIMAL(12,2) NULL,
    [RECORD_TEXT] NVARCHAR(512) NULL,
    [LOAD_DATE] DATETIME2 NULL
);

IF OBJECT_ID('dbo.CPM_CDC_STAGING_TBL', 'U') IS NOT NULL DROP TABLE dbo.CPM_CDC_STAGING_TBL;
CREATE TABLE dbo.CPM_CDC_STAGING_TBL (
    [PP_END_YEAR] DECIMAL(4,0) NULL,
    [PP_NUM] DECIMAL(2,0) NULL,
    [SSN] NVARCHAR(9) NULL,
    [AGENCY_CD] NVARCHAR(4) NULL,
    [GROSS_PAY] DECIMAL(12,2) NULL,
    [NET_PAY] DECIMAL(12,2) NULL,
    [RECORD_TEXT] NVARCHAR(512) NULL,
    [LOAD_DATE] DATETIME2 NULL
);
"""


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--xml-dir", default=str(Path(__file__).resolve().parents[2] / "XML"))
    ap.add_argument("--out", default=str(Path(__file__).resolve().parents[1] / "sql/ddl/001_create_all_tables.sql"))
    args = ap.parse_args()

    xml_dir = Path(args.xml_dir)
    roots: dict[str, ET.Element] = {}
    out = ["-- Auto-generated by scripts/generate_ddl.py",
           "-- Oracle (Informatica PowerCenter 9.6.1) -> SQL Server DDL",
           "-- Do not edit by hand; re-run the generator instead.",
           ""]

    # The ALL tables mirror their NWK_* counterparts; build them after parsing.
    all_table_clones = {
        "ACTION_PRIMARY_ALL": "NWK_ACTION_PRIMARY_TBL",
        "ACTION_SECONDARY_ALL": "NWK_ACTION_SECONDARY_TBL",
        "ACTION_REMARKS_ALL": "NWK_ACTION_REMARKS_TBL",
    }
    parsed_cols: dict[str, tuple] = {}

    for table, xmlname in TABLES_FROM_XML.items():
        if xmlname not in roots:
            roots[xmlname] = parse_xml(xml_dir / xmlname)
        found = find_table(roots[xmlname], table)
        if not found:
            raise SystemExit(f"Table {table} not found in XML/{xmlname}")
        tag, elem = found
        cols, pk = column_defs(tag, elem)
        cols = list(cols)
        out.append(f"-- {table} ({len(cols)} base cols, from XML/{xmlname})")
        rendered = render_table(table, cols, pk, EXTRA_COLUMNS.get(table))
        out.append(rendered)
        out.append("")
        parsed_cols[table] = (cols, pk)

    # ACTION_*_ALL clones (no PK; archive-style append targets).
    for clone, base in all_table_clones.items():
        if base == "NWK_ACTION_REMARKS_TBL":
            cols = [("EVENT_ID", "DECIMAL(15,0)", "NOT NULL"),
                    ("REMARK_SEQ", "DECIMAL(5,0)", "NOT NULL"),
                    ("REMARK_CD", "NVARCHAR(10)", "NULL"),
                    ("REMARK_TEXT", "NVARCHAR(500)", "NULL"),
                    ("LOAD_DATE", "DATETIME2", "NULL")]
        else:
            base_cols, _ = parsed_cols[base]
            cols = [list(c) for c in base_cols]
            if "LOAD_DATE" not in {c[0] for c in cols}:
                cols.append(("LOAD_DATE", "DATETIME2", "NULL"))
        out.append(f"-- {clone} (BIIS production mirror of {base})")
        out.append(render_table(clone, [list(c) for c in cols], []))
        out.append("")

    out.append(HANDWRITTEN)

    Path(args.out).write_text("\n".join(out))
    print(f"Wrote {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
