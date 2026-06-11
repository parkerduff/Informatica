"""Reconciliation targets: which (module, table, key_columns) tuples are
compared against golden expected data.  Shared by the regression test suite,
``scripts/reconcile.py`` and ``scripts/check_reconciliation.py``.
"""
from __future__ import annotations

from typing import Dict, List, Tuple

# module -> list of (table, key_columns)
RECON_TARGETS: Dict[str, List[Tuple[str, List[str]]]] = {
    "pay_calendar": [("PAY_PERIOD", ["PP_NUM", "PP_END_YEAR"])],
    "comptime": [
        ("COMP_TIME_DAILY_TBL", ["SSN", "PP_END_DATE", "DAILY_DATE_EARNED"]),
        ("COUNTER_TBL", ["PROCESS_NAME", "PP_END_YEAR", "PP_NUM"]),
    ],
    "pseudossn": [
        ("PSEUDOSSN_FROM_SDA_TBL", ["PSEUDO_SSN", "EFFECTIVE_DATE"]),
        ("PSEUDOSSN_TBL", ["PSEUDO_SSN"]),
    ],
    "fda_leave": [("ERROR_TBL", ["FDA_BATCH_ID", "FDA_TK_NO", "FDA_EMP_ID", "ERROR_TYPE"])],
    "ehrp2biis": [
        ("ACTION_PRIMARY_ALL", ["EVENT_ID"]),
        ("ACTION_SECONDARY_ALL", ["EVENT_ID"]),
        ("ACTION_REMARKS_ALL", ["EVENT_ID", "REMARK_SEQ"]),
    ],
    "cpm_nih": [("CPM_NIH_STG_TBL", ["DFAS_PSEUDO_SSN", "PP_END_YEAR", "PP_NUM"])],
    "cpm_oig": [("CPM_OIG_STG_TBL", ["DFAS_PSEUDO_SSN", "PP_END_YEAR", "PP_NUM"])],
    "cpm_cdc": [("CPM_CDC_STG_TBL", ["DFAS_PSEUDO_SSN", "PP_END_YEAR", "PP_NUM"])],
}


def flat_targets():
    for module, tables in RECON_TARGETS.items():
        for table, keys in tables:
            yield module, table, keys
