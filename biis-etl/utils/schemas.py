"""Central table schema registry.

Each table is a list of ``(column, oracle_type, precision, scale)`` tuples,
derived from the source Informatica PowerCenter XML definitions.  Large tables
(CPM_NEWPAY_TBL, the 260/209-column action tables, PS_GVT_JOB) are loaded from
JSON resources extracted verbatim from the XML; smaller tables are inlined.

The registry drives DDL generation, seeding and reconciliation so there is a
single source of truth for the schema.
"""
from __future__ import annotations

import json
import os
from typing import Dict, List, Tuple

Column = Tuple[str, str, int, int]  # name, oracle_type, precision, scale

_HERE = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.dirname(_HERE)
_EHRP = os.path.join(_ROOT, "jobs", "ehrp2biis")
_CPM = os.path.join(_ROOT, "jobs", "cpm")


def _load(path: str) -> List[Column]:
    with open(path) as fh:
        return [tuple(c) for c in json.load(fh)]  # type: ignore[misc]


# --------------------------------------------------------------------------- #
# Inlined small tables
# --------------------------------------------------------------------------- #
PAY_PERIOD: List[Column] = [
    ("PP_NUM", "number(p,s)", 2, 0),
    ("PP_END_YEAR", "number(p,s)", 4, 0),
    ("PP_START_DTE", "date", 19, 0),
    ("PP_END_DTE", "date", 19, 0),
    ("LV_NUM", "number(p,s)", 2, 0),
    ("LV_YEAR", "number(p,s)", 4, 0),
    ("PAY_DTE", "date", 19, 0),
    ("CURR_PP_FLAG", "varchar2", 1, 0),
    ("HOLIDAY_1", "date", 19, 0),
    ("HOLIDAY_2", "date", 19, 0),
]

COMP_TIME_DAILY_TBL: List[Column] = [
    ("PP_END_YEAR", "number(p,s)", 4, 0),
    ("PP_NUM", "number(p,s)", 2, 0),
    ("PP_YEAR_NUM", "number(p,s)", 6, 0),
    ("SSN", "varchar2", 9, 0),
    ("NAME", "varchar2", 30, 0),
    ("CURRENT_ACCT", "varchar2", 6, 0),
    ("CURRENT_ORG", "varchar2", 7, 0),
    ("FLSA_STATUS", "varchar2", 1, 0),
    ("COMP_TIME_CUR_BAL", "number(p,s)", 8, 2),
    ("COMP_TIME_YEAR_EARNED", "number(p,s)", 4, 0),
    ("PP_END_DATE", "date", 19, 0),
    ("DAILY_DATE_EARNED", "date", 19, 0),
    ("COMP_TIME_RATE", "number(p,s)", 6, 2),
    ("COMP_TIME_HOURS", "number(p,s)", 8, 2),
    ("COMP_TIME_UNDEF", "number(p,s)", 6, 0),
]

COUNTER_TBL: List[Column] = [
    ("PROCESS_NAME", "varchar2", 30, 0),
    ("PP_END_YEAR", "number(p,s)", 4, 0),
    ("PP_NUM", "number(p,s)", 2, 0),
    ("COUNTER_VALUE", "number(p,s)", 10, 0),
    ("RUN_DATE", "date", 19, 0),
]

# Fixed-width SDA detail layout (PseudoSSN)
PSEUDOSSN_FROM_SDA_TBL: List[Column] = [
    ("PSEUDO_SSN", "varchar2", 9, 0),
    ("REAL_SSN", "varchar2", 9, 0),
    ("LAST_NAME", "varchar2", 30, 0),
    ("FIRST_NAME", "varchar2", 20, 0),
    ("EFFECTIVE_DATE", "date", 19, 0),
    ("AMOUNT", "number(p,s)", 10, 2),
    ("RECORD_TYPE", "varchar2", 1, 0),
]

PSEUDOSSN_TBL: List[Column] = [
    ("PSEUDO_SSN", "varchar2", 9, 0),
    ("REAL_SSN", "varchar2", 9, 0),
    ("LAST_NAME", "varchar2", 30, 0),
    ("FIRST_NAME", "varchar2", 20, 0),
    ("EFFECTIVE_DATE", "date", 19, 0),
    ("AMOUNT", "number(p,s)", 10, 2),
]

HI_ARCH_PSEUDOSSN_TBL: List[Column] = list(PSEUDOSSN_TBL) + [
    ("ARCHIVE_DATE", "date", 19, 0),
]

# FDA Leave
HI_PM_FDA_TATRAN_TBL: List[Column] = [
    ("FDA_BATCH_ID", "varchar2", 10, 0),
    ("FDA_TK_NO", "varchar2", 10, 0),
    ("FDA_EMP_ID", "varchar2", 9, 0),
    ("PP_END_YEAR", "number(p,s)", 4, 0),
    ("PP_NUM", "number(p,s)", 2, 0),
    ("LEAVE_HOURS", "number(p,s)", 8, 2),
    ("LEAVE_TYPE", "varchar2", 4, 0),
]

ERROR_TBL: List[Column] = [
    ("FDA_BATCH_ID", "varchar2", 10, 0),
    ("FDA_TK_NO", "varchar2", 10, 0),
    ("FDA_EMP_ID", "varchar2", 9, 0),
    ("ERROR_TYPE", "varchar2", 30, 0),
    ("ERROR_MSG", "varchar2", 200, 0),
    ("RUN_DATE", "date", 19, 0),
]

_CPM_STG_DETAIL = [
    ("FDA_EMP_ID", "varchar2", 9, 0),
    ("PP_END_YEAR", "number(p,s)", 4, 0),
    ("PP_NUM", "number(p,s)", 2, 0),
    ("DETAIL_AMT", "number(p,s)", 10, 2),
]
CPM_YTD_DETAIL_STG_TBL: List[Column] = list(_CPM_STG_DETAIL)
CPM_PAD_DETAIL_STG_TBL: List[Column] = list(_CPM_STG_DETAIL)
CPM_MER_DETAIL_STG_TBL: List[Column] = list(_CPM_STG_DETAIL)

SEQUENCE_NUM_TBL: List[Column] = [
    ("SEQ_NAME", "varchar2", 30, 0),
    ("SEQ_VALUE", "number(p,s)", 12, 0),
]

PROCESS_TABLE: List[Column] = [
    ("P_NAME", "varchar2", 30, 0),
    ("P_STARTDT", "date", 19, 0),
]

NWK_ACTION_REMARKS_TBL: List[Column] = [
    ("EVENT_ID", "number(p,s)", 12, 0),
    ("REMARK_SEQ", "number(p,s)", 4, 0),
    ("REMARK_CD", "varchar2", 10, 0),
    ("REMARK_TEXT", "varchar2", 240, 0),
    ("LOAD_DATE", "date", 19, 0),
]

# CPM agency extract staging (one row per extracted employee per agency)
_CPM_AGENCY_STG = [
    ("PP_END_YEAR", "number(p,s)", 4, 0),
    ("PP_NUM", "number(p,s)", 2, 0),
    ("DFAS_PSEUDO_SSN", "varchar2", 9, 0),
    ("BUSINESS_UNIT", "varchar2", 5, 0),
    ("ORG_CDE", "varchar2", 10, 0),
    ("AGENCY", "varchar2", 5, 0),
    ("RECORD_COUNT", "number(p,s)", 6, 0),
]
CPM_NIH_STG_TBL: List[Column] = list(_CPM_AGENCY_STG)
CPM_OIG_STG_TBL: List[Column] = list(_CPM_AGENCY_STG)
CPM_CDC_STG_TBL: List[Column] = list(_CPM_AGENCY_STG)


# --------------------------------------------------------------------------- #
# Large tables loaded from JSON
# --------------------------------------------------------------------------- #
NWK_NEW_EHRP_ACTIONS_TBL: List[Column] = _load(os.path.join(_EHRP, "schema_new_ehrp_actions.json"))
PS_GVT_JOB: List[Column] = _load(os.path.join(_EHRP, "schema_ps_gvt_job.json"))
EHRP_RECS_TRACKING_TBL: List[Column] = _load(os.path.join(_EHRP, "schema_recs_tracking.json"))
NWK_ACTION_PRIMARY_TBL: List[Column] = _load(os.path.join(_EHRP, "schema_action_primary.json"))
NWK_ACTION_SECONDARY_TBL: List[Column] = _load(os.path.join(_EHRP, "schema_action_secondary.json"))
CPM_NEWPAY_TBL: List[Column] = _load(os.path.join(_CPM, "cpm_newpay_columns.json"))

# ACTION_*_ALL share the schema of the corresponding NWK_ACTION_*_TBL but add a
# LOAD_DATE bookkeeping column (used by the afterload window deletes/inserts).
ACTION_PRIMARY_ALL: List[Column] = list(NWK_ACTION_PRIMARY_TBL)
ACTION_SECONDARY_ALL: List[Column] = list(NWK_ACTION_SECONDARY_TBL)
ACTION_REMARKS_ALL: List[Column] = list(NWK_ACTION_REMARKS_TBL)


# NWK_ACTION_PRIMARY_TBL needs a LOAD_DATE column for the afterload's
# "where load_date = trunc(sysdate)" filtering (present in production DDL).
def _ensure_load_date(cols: List[Column]) -> List[Column]:
    if not any(c[0] == "LOAD_DATE" for c in cols):
        return list(cols) + [("LOAD_DATE", "date", 19, 0)]
    return list(cols)


# EHRP_RECS_TRACKING_TBL needs the WIP-status-change date used by the
# afterload's cancelled-action delete/reinsert window (present in prod DDL).
if not any(c[0] == "BIIS_WIP_STATUS_CHANGED_DT" for c in EHRP_RECS_TRACKING_TBL):
    EHRP_RECS_TRACKING_TBL = list(EHRP_RECS_TRACKING_TBL) + [
        ("BIIS_WIP_STATUS_CHANGED_DT", "date", 19, 0),
    ]

NWK_ACTION_PRIMARY_TBL = _ensure_load_date(NWK_ACTION_PRIMARY_TBL)
NWK_ACTION_SECONDARY_TBL = _ensure_load_date(NWK_ACTION_SECONDARY_TBL)
ACTION_PRIMARY_ALL = _ensure_load_date(ACTION_PRIMARY_ALL)
ACTION_SECONDARY_ALL = _ensure_load_date(ACTION_SECONDARY_ALL)


TABLES: Dict[str, List[Column]] = {
    "PAY_PERIOD": PAY_PERIOD,
    "COMP_TIME_DAILY_TBL": COMP_TIME_DAILY_TBL,
    "COUNTER_TBL": COUNTER_TBL,
    "PSEUDOSSN_FROM_SDA_TBL": PSEUDOSSN_FROM_SDA_TBL,
    "PSEUDOSSN_TBL": PSEUDOSSN_TBL,
    "HI_ARCH_PSEUDOSSN_TBL": HI_ARCH_PSEUDOSSN_TBL,
    "HI_PM_FDA_TATRAN_TBL": HI_PM_FDA_TATRAN_TBL,
    "ERROR_TBL": ERROR_TBL,
    "CPM_YTD_DETAIL_STG_TBL": CPM_YTD_DETAIL_STG_TBL,
    "CPM_PAD_DETAIL_STG_TBL": CPM_PAD_DETAIL_STG_TBL,
    "CPM_MER_DETAIL_STG_TBL": CPM_MER_DETAIL_STG_TBL,
    "SEQUENCE_NUM_TBL": SEQUENCE_NUM_TBL,
    "PROCESS_TABLE": PROCESS_TABLE,
    "NWK_NEW_EHRP_ACTIONS_TBL": NWK_NEW_EHRP_ACTIONS_TBL,
    "PS_GVT_JOB": PS_GVT_JOB,
    "EHRP_RECS_TRACKING_TBL": EHRP_RECS_TRACKING_TBL,
    "NWK_ACTION_PRIMARY_TBL": NWK_ACTION_PRIMARY_TBL,
    "NWK_ACTION_SECONDARY_TBL": NWK_ACTION_SECONDARY_TBL,
    "NWK_ACTION_REMARKS_TBL": NWK_ACTION_REMARKS_TBL,
    "ACTION_PRIMARY_ALL": ACTION_PRIMARY_ALL,
    "ACTION_SECONDARY_ALL": ACTION_SECONDARY_ALL,
    "ACTION_REMARKS_ALL": ACTION_REMARKS_ALL,
    "CPM_NEWPAY_TBL": CPM_NEWPAY_TBL,
    "CPM_NIH_STG_TBL": CPM_NIH_STG_TBL,
    "CPM_OIG_STG_TBL": CPM_OIG_STG_TBL,
    "CPM_CDC_STG_TBL": CPM_CDC_STG_TBL,
}


# --------------------------------------------------------------------------- #
# Type mapping helpers
# --------------------------------------------------------------------------- #
def column_names(table: str) -> List[str]:
    return [c[0] for c in TABLES[table]]


def sqlite_type(col: Column) -> str:
    _, otype, _, scale = col
    otype = otype.lower()
    if otype.startswith("number"):
        return "INTEGER" if scale == 0 else "REAL"
    if otype == "date":
        return "TEXT"
    return "TEXT"


def sqlserver_type(col: Column) -> str:
    name, otype, precision, scale = col
    otype = otype.lower()
    if otype.startswith("number"):
        if scale == 0:
            return "BIGINT" if precision > 9 else "INT"
        return f"DECIMAL({max(precision, 1)},{scale})"
    if otype == "date":
        return "DATETIME2"
    return f"VARCHAR({max(precision, 1)})"


def spark_field(col: Column):
    from pyspark.sql.types import (
        DataType,
        DoubleType,
        LongType,
        StringType,
        StructField,
    )

    name, otype, precision, scale = col
    otype = otype.lower()
    t: DataType
    if otype.startswith("number"):
        t = LongType() if scale == 0 else DoubleType()
    else:
        t = StringType()
    return StructField(name, t, True)


def spark_schema(table: str):
    from pyspark.sql.types import StructType

    return StructType([spark_field(c) for c in TABLES[table]])
