"""Generate sql/ddl/001_create_all_tables.sql from the schemas/ JSON registry.

Run from biis-etl/:  python scripts/generate_ddl.py
The committed DDL file is the output of this script; regenerate after any
schema registry change.
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils import schemas  # noqa: E402

OUT = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                   "sql", "ddl", "001_create_all_tables.sql")

# (table, module, definition name, kind)
XML_TABLES = [
    ("PAY_PERIOD", "Pay_Calendar", "PAY_PERIOD", "sources"),
    ("COMP_TIME_DAILY_TBL", "COMPTIME", "COMP_TIME_DAILY_TBL", "targets"),
    ("COUNTER_TBL", "COMPTIME", "COUNTER_TBL", "targets"),
    ("PSEUDOSSN_FROM_SDA_TBL", "Pseudossn", "PSEUDOSSN_FROM_SDA_TBL", "targets"),
    ("PSEUDOSSN_TBL", "Pseudossn", "PSEUDOSSN_TBL", "targets"),
    ("HI_ARCH_PSEUDOSSN_TBL", "Pseudossn", "HI_ARCH_PSEUDOSSN_TBL", "targets"),
    ("ERROR_TBL", "Pseudossn", "ERROR_TBL", "targets"),
    ("HI_PM_FDA_TATRAN_TBL", "FDA_Leave", "HI_PM_FDA_TATRAN_TBL", "sources"),
    ("CPM_CYCLE_TBL", "FDA_Leave", "CPM_CYCLE_TBL", "sources"),
    ("NWK_NEW_EHRP_ACTIONS_TBL", "EHRP2BIIS_UPDATE", "NWK_NEW_EHRP_ACTIONS_TBL", "sources"),
    ("PS_GVT_JOB", "EHRP2BIIS_UPDATE", "PS_GVT_JOB", "sources"),
    ("NWK_ACTION_PRIMARY_TBL", "EHRP2BIIS_UPDATE", "NWK_ACTION_PRIMARY_TBL", "targets"),
    ("NWK_ACTION_SECONDARY_TBL", "EHRP2BIIS_UPDATE", "NWK_ACTION_SECONDARY_TBL", "targets"),
    ("EHRP_RECS_TRACKING_TBL", "EHRP2BIIS_UPDATE", "EHRP_RECS_TRACKING_TBL", "targets"),
    ("ACTION_PRIMARY_ALL", "EHRP2BIIS_UPDATE", "NWK_ACTION_PRIMARY_TBL", "targets"),
    ("ACTION_SECONDARY_ALL", "EHRP2BIIS_UPDATE", "NWK_ACTION_SECONDARY_TBL", "targets"),
    ("CPM_NEWPAY_TBL", "CPM_NIH", "CPM_NEWPAY_TBL", "sources"),
]

# Tables whose definitions are not present in the XML exports (live only in
# Oracle); minimal layouts inferred from usage in ehrp2biis_afterload.sql and
# the migration spec.
MANUAL_TABLES = {
    "COMP_TIME_DAILY_TBL_EXTRA": None,  # placeholder, unused
    "SEQUENCE_NUM_TBL": """
CREATE TABLE [dbo].[SEQUENCE_NUM_TBL] (
    [SEQ_NAME] NVARCHAR(50) NOT NULL,
    [OLD_SEQUENCE_NUMBER] DECIMAL(15,0) NULL,
    [NEW_SEQUENCE_NUMBER] DECIMAL(15,0) NULL,
    [LAST_UPDATE_DATE] DATETIME2 NULL
);""",
    "PROCESS_TABLE": """
CREATE TABLE [dbo].[PROCESS_TABLE] (
    [PROCESS_NAME] NVARCHAR(50) NULL,
    [P_STARTDT] DATETIME2 NULL,
    [P_ENDDT] DATETIME2 NULL
);""",
    "NWK_ACTION_REMARKS_TBL": """
CREATE TABLE [dbo].[NWK_ACTION_REMARKS_TBL] (
    [EVENT_ID] DECIMAL(15,0) NOT NULL,
    [REMARK_SEQ] DECIMAL(5,0) NOT NULL,
    [REMARK_CD] NVARCHAR(10) NULL,
    [REMARK_TEXT] NVARCHAR(500) NULL,
    [LOAD_DATE] DATETIME2 NULL
);""",
    "ACTION_REMARKS_ALL": """
CREATE TABLE [dbo].[ACTION_REMARKS_ALL] (
    [EVENT_ID] DECIMAL(15,0) NOT NULL,
    [REMARK_SEQ] DECIMAL(5,0) NOT NULL,
    [REMARK_CD] NVARCHAR(10) NULL,
    [REMARK_TEXT] NVARCHAR(500) NULL,
    [LOAD_DATE] DATETIME2 NULL
);""",
    "CPM_YTD_DETAIL_STG_TBL": """
CREATE TABLE [dbo].[CPM_YTD_DETAIL_STG_TBL] (
    [EMP_ID] NVARCHAR(16) NOT NULL,
    [PP_YEAR] DECIMAL(4,0) NOT NULL,
    [PP_NUM] DECIMAL(2,0) NOT NULL,
    [REC_TYPE] NVARCHAR(3) NULL,
    [LOAD_DATE] DATETIME2 NULL
);""",
    "CPM_PAD_DETAIL_STG_TBL": """
CREATE TABLE [dbo].[CPM_PAD_DETAIL_STG_TBL] (
    [EMP_ID] NVARCHAR(16) NOT NULL,
    [PP_YEAR] DECIMAL(4,0) NOT NULL,
    [PP_NUM] DECIMAL(2,0) NOT NULL,
    [REC_TYPE] NVARCHAR(3) NULL,
    [LOAD_DATE] DATETIME2 NULL
);""",
    "CPM_MER_DETAIL_STG_TBL": """
CREATE TABLE [dbo].[CPM_MER_DETAIL_STG_TBL] (
    [EMP_ID] NVARCHAR(16) NOT NULL,
    [PP_YEAR] DECIMAL(4,0) NOT NULL,
    [PP_NUM] DECIMAL(2,0) NOT NULL,
    [REC_TYPE] NVARCHAR(3) NULL,
    [LOAD_DATE] DATETIME2 NULL
);""",
    "CPM_NIH_STG_TBL": """
CREATE TABLE [dbo].[CPM_NIH_STG_TBL] (
    [RECORD_NUM] INT NOT NULL,
    [RECORD_TYPE] NVARCHAR(3) NOT NULL,
    [RECORD_DATA] NVARCHAR(MAX) NULL,
    [LOAD_DATE] DATETIME2 NULL
);""",
    "CPM_OIG_STG_TBL": """
CREATE TABLE [dbo].[CPM_OIG_STG_TBL] (
    [RECORD_NUM] INT NOT NULL,
    [RECORD_TYPE] NVARCHAR(3) NOT NULL,
    [RECORD_DATA] NVARCHAR(MAX) NULL,
    [LOAD_DATE] DATETIME2 NULL
);""",
    "CPM_CDC_STG_TBL": """
CREATE TABLE [dbo].[CPM_CDC_STG_TBL] (
    [RECORD_NUM] INT NOT NULL,
    [RECORD_TYPE] NVARCHAR(3) NOT NULL,
    [RECORD_DATA] NVARCHAR(MAX) NULL,
    [LOAD_DATE] DATETIME2 NULL
);""",
}

EXTRA_COLUMNS = {
    # COMP_TIME_DAILY_TBL target in the XML already contains SSN_HASH/LOAD_DATE
    # equivalents; ensure they exist per the migration spec.
    "COMP_TIME_DAILY_TBL": [
        "[SSN_HASH] NVARCHAR(64) NULL",
        "[LOAD_DATE] DATETIME2 NULL",
    ],
}


def render_table(table_name, module, def_name, kind):
    fields = schemas.get_table_fields(module, def_name, kind)
    cols = [schemas.sqlserver_column_def(f) for f in fields]
    existing = {f["name"].upper() for f in fields}
    for extra in EXTRA_COLUMNS.get(table_name, []):
        col = extra.split("]")[0].strip("[")
        if col.upper() not in existing:
            cols.append(extra)
    body = ",\n    ".join(cols)
    return f"CREATE TABLE [dbo].[{table_name}] (\n    {body}\n);"


def main():
    parts = [
        "-- Generated by scripts/generate_ddl.py from schemas/*.json",
        "-- (extracted from the Informatica PowerCenter XML exports in XML/).",
        "-- Oracle -> SQL Server type mapping: NUMBER(p,s)->DECIMAL(p,s),",
        "-- VARCHAR2(n)->NVARCHAR(n), DATE->DATETIME2.",
        "",
    ]
    all_names = [t[0] for t in XML_TABLES] + [
        n for n in MANUAL_TABLES if MANUAL_TABLES[n]
    ]
    for name in all_names:
        parts.append(f"IF OBJECT_ID('dbo.{name}', 'U') IS NOT NULL DROP TABLE [dbo].[{name}];")
    parts.append("")
    for table_name, module, def_name, kind in XML_TABLES:
        parts.append(render_table(table_name, module, def_name, kind))
        parts.append("")
    for name, ddl in MANUAL_TABLES.items():
        if ddl:
            parts.append(ddl.strip())
            parts.append("")
    with open(OUT, "w") as f:
        f.write("\n".join(parts) + "\n")
    print(f"Wrote {OUT} ({len(all_names)} tables)")


if __name__ == "__main__":
    main()
