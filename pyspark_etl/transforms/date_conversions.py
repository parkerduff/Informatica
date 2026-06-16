"""Date parsing helpers mirroring the Informatica ``IS_DATE``/``TO_DATE`` logic.

The flat-file dates arrive as 8-character strings in one of three layouts. The
original mappings rebuild each value into an ``MM/DD/YYYY`` string, validate it
with ``IS_DATE`` and only then call ``TO_DATE`` -- returning NULL when the value
is not a valid date. These helpers reproduce that behaviour.

Layouts (see Pseudossn exp_Conversions):
    MMDDYYYY  -> HIRE_DATE, CAREER_START_DATE, CAREER_CONV_DATE, PROBATION_DATE,
                 LAST_PAY_CHANGE
    YYYYMMDD  -> SEPARATION_DATE, UNIF_ALLOW_DATE, PCA_* dates, CHARITY_EFF_DATE,
                 EFFECTIVE_DATE (changed from YYYYDDMM on 2012-12-13)
    YYYYDDMM  -> the original EFFECTIVE_DATE convention (kept for completeness)
"""
from __future__ import annotations

import datetime
from typing import Optional


def _clean(value: Optional[str]) -> str:
    return (value or "").strip()


def _build(value: Optional[str], y: slice, m: slice, d: slice) -> Optional[datetime.date]:
    """Slice ``value`` into year/month/day pieces and validate as a real date.

    Returns ``None`` for missing, wrong-length or invalid input, matching the
    Informatica ``IIF(IS_DATE(...), TO_DATE(...))`` idiom.
    """
    v = _clean(value)
    if len(v) != 8 or not v.isdigit():
        return None
    try:
        return datetime.date(int(v[y]), int(v[m]), int(v[d]))
    except ValueError:
        return None


def parse_mmddyyyy(value: Optional[str]) -> Optional[datetime.date]:
    """Parse an ``MMDDYYYY`` string (e.g. ``01312020`` -> 2020-01-31)."""
    return _build(value, y=slice(4, 8), m=slice(0, 2), d=slice(2, 4))


def parse_yyyymmdd(value: Optional[str]) -> Optional[datetime.date]:
    """Parse a ``YYYYMMDD`` string (e.g. ``20200131`` -> 2020-01-31)."""
    return _build(value, y=slice(0, 4), m=slice(4, 6), d=slice(6, 8))


def parse_yyyyddmm(value: Optional[str]) -> Optional[datetime.date]:
    """Parse a ``YYYYDDMM`` string (e.g. ``20203101`` -> 2020-01-31)."""
    return _build(value, y=slice(0, 4), m=slice(6, 8), d=slice(4, 6))


_PARSERS = {
    "MMDDYYYY": parse_mmddyyyy,
    "YYYYMMDD": parse_yyyymmdd,
    "YYYYDDMM": parse_yyyyddmm,
}


def parse_date(value: Optional[str], layout: str) -> Optional[datetime.date]:
    """Dispatch to the parser for ``layout`` (one of the keys in ``_PARSERS``)."""
    try:
        return _PARSERS[layout](value)
    except KeyError as exc:  # pragma: no cover - defensive
        raise ValueError(f"Unknown date layout {layout!r}; expected {list(_PARSERS)}") from exc


# --- PySpark helpers ----------------------------------------------------------
def date_col(column, layout: str):
    """Return a ``DateType`` Spark Column parsing ``column`` using ``layout``."""
    from pyspark.sql.functions import udf
    from pyspark.sql.types import DateType

    parser = _PARSERS[layout]
    return udf(parser, DateType())(column)
