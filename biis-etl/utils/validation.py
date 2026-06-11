"""Lightweight data-quality validation used by all jobs.

These checks replicate the implicit row-count / not-null guards that the
Informatica sessions and the shell-script ``ERR_FLAG`` logic enforced.
"""
from __future__ import annotations

import datetime as dt
import logging
from typing import Any, List, Optional

logger = logging.getLogger(__name__)


class ValidationError(Exception):
    """Raised when a data-quality check fails."""


def validate_row_count(
    df: Any, expected_min: int = 1, expected_max: Optional[int] = None, context: str = ""
) -> int:
    count = df.count()
    if count < expected_min:
        raise ValidationError(
            f"{context}: row count {count} below minimum {expected_min}"
        )
    if expected_max is not None and count > expected_max:
        raise ValidationError(
            f"{context}: row count {count} above maximum {expected_max}"
        )
    logger.info("%s: row count %d OK", context or "validate_row_count", count)
    return count


def validate_schema(df: Any, expected_columns: List[str], context: str = "") -> None:
    actual = set(df.columns)
    missing = [c for c in expected_columns if c not in actual]
    if missing:
        raise ValidationError(
            f"{context}: missing expected columns {missing} (have {sorted(actual)})"
        )
    logger.info("%s: schema OK (%d expected columns present)", context, len(expected_columns))


def validate_no_nulls(df: Any, columns: List[str], context: str = "") -> None:
    from pyspark.sql import functions as F

    conds = None
    for c in columns:
        cond = F.col(c).isNull()
        conds = cond if conds is None else (conds | cond)
    if conds is None:
        return
    bad = df.filter(conds).count()
    if bad:
        raise ValidationError(
            f"{context}: {bad} rows have NULLs in required columns {columns}"
        )
    logger.info("%s: no nulls in %s", context, columns)


def log_row_count(
    conn: Any,
    table: str,
    process_name: str,
    count: int,
    pay_period: dict,
    description: Optional[str] = None,
    cycle_id: int = 1,
) -> None:
    """Insert a counter row into COUNTER_TBL (the Informatica counter targets)."""
    from utils.db import execute_sql

    sql = (
        "INSERT INTO dbo.COUNTER_TBL "
        "(RUN_DATE, PROCESS_NAME, COUNTER_DESCRIPTION, COUNTER_VALUE, "
        " PP_END_YEAR, PP_NUM, CYCLE_ID) VALUES (?, ?, ?, ?, ?, ?, ?)"
    )
    execute_sql(
        conn,
        sql,
        [
            dt.datetime.now(),
            process_name,
            description or f"{process_name} record count",
            count,
            pay_period.get("pp_end_year"),
            pay_period.get("pp_num"),
            cycle_id,
        ],
    )
    logger.info("Logged counter %s=%d into COUNTER_TBL", process_name, count)
