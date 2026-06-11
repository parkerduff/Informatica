"""Shared field-mapping rules for the EHRP2BIIS ETL.

The Informatica mapping ``m_EHRP2BIIS_UPDATE`` copies the matching PS_GVT_JOB
fields into the NWK_ACTION_* targets, derives the BIIS EVENT_ID from a sequence,
and leaves the remaining target fields (populated downstream by the afterload
stored procedures) null. Encoding the column rules here once keeps the PySpark
job and the synthetic golden generator perfectly aligned.
"""
from __future__ import annotations

from typing import Dict, List, Tuple

from jobs.schemas import (NWK_ACTION_PRIMARY_TBL_COLS,
                          NWK_ACTION_SECONDARY_TBL_COLS, PS_GVT_JOB_COLS)

# Columns set explicitly by the ETL rather than copied from the source.
PRIMARY_OVERRIDES = {"EVENT_ID", "LOAD_ID", "LOAD_DATE"}
SECONDARY_OVERRIDES = {"EVENT_ID", "LOAD_DATE"}

SEQ_NAME = "EHRP_EVENT_ID"

# Tracking table (EHRP_RECS_TRACKING_TBL) explicit column order.
TRACKING_COLS = [
    "EMPLID", "EMPL_RCD", "EFFDT", "EFFSEQ", "EVENT_SUBMITTED_DT",
    "GVT_WIP_STATUS", "NOA_CD", "NOA_SUFFIX_CD", "BIIS_EVENT_ID", "LOAD_DATE",
]

# Remarks table explicit column order.
REMARKS_COLS = ["EVENT_ID", "REMARK_SEQ", "REMARK_CD", "REMARK_TEXT", "LOAD_DATE"]


def _source_names() -> set:
    return {c[0] for c in PS_GVT_JOB_COLS}


def _target_names(cols) -> List[str]:
    return [c[0] for c in cols]


def col_type(cols, name: str) -> Tuple[str, str, str]:
    """Return (datatype, precision, scale) for a column in a spec list."""
    for n, dt, p, s in cols:
        if n == name:
            return dt, p, s
    return "varchar2", "255", "0"


def classify_target(target_cols, overrides) -> Dict[str, List[str]]:
    """Split target columns into copied-from-source vs null-filled."""
    src = _source_names()
    copy_cols, null_cols = [], []
    for name in _target_names(target_cols):
        if name in overrides:
            continue
        if name in src:
            copy_cols.append(name)
        else:
            null_cols.append(name)
    return {"copy": copy_cols, "null": null_cols}


def primary_plan() -> Dict[str, List[str]]:
    return classify_target(NWK_ACTION_PRIMARY_TBL_COLS, PRIMARY_OVERRIDES)


def secondary_plan() -> Dict[str, List[str]]:
    return classify_target(NWK_ACTION_SECONDARY_TBL_COLS, SECONDARY_OVERRIDES)
