"""Shared, framework-agnostic transformation helpers.

These pure functions encode the business rules that must remain identical
between the PySpark jobs and the synthetic golden generator (which uses pandas).
Keeping them in one place is what guarantees zero reconciliation drift.
"""
from __future__ import annotations

import hashlib
from decimal import Decimal
from typing import Optional, Tuple

# Fixed-width CPM (PWX) output field widths -- shared by the job and generator.
CPM_SSN_W = 9
CPM_AGENCY_W = 3
CPM_AMT_W = 11  # scaled integer cents, zero-padded
CPM_AMT_SCALE = 2


def scaled_cents(value) -> int:
    """Convert a monetary value to a scaled integer (cents). Shared CPM rule."""
    if value is None or value == "":
        value = 0
    return int((Decimal(str(value)) * (10 ** CPM_AMT_SCALE)).to_integral_value())


def build_record_text(ssn: str, agency_code: str, gross, net) -> str:
    """Build the fixed-width PWX output record. Shared by job and generator."""
    return (
        str(ssn).rjust(CPM_SSN_W, "0")[:CPM_SSN_W]
        + str(agency_code).ljust(CPM_AGENCY_W)[:CPM_AGENCY_W]
        + f"{scaled_cents(gross):0{CPM_AMT_W}d}"
        + f"{scaled_cents(net):0{CPM_AMT_W}d}"
    )


def oracle_kind(datatype: str, precision: Optional[str], scale: Optional[str]) -> Tuple[str, int, int]:
    """Classify an Oracle column as ('date'|'decimal'|'string', precision, scale)."""
    dt = (datatype or "").lower()
    p = int(precision) if precision and str(precision).isdigit() else 0
    s = int(scale) if scale and str(scale).isdigit() else 0
    if dt.startswith("date") or dt == "timestamp":
        return ("date", 19, 0)
    if dt.startswith("number") or dt in ("decimal", "numeric"):
        p = min(max(p, 1), 38)
        if s > p:
            p = s
        return ("decimal", p, s)
    return ("string", max(p, 1), 0)


def spark_type(datatype: str, precision: Optional[str], scale: Optional[str]):
    """Map an Oracle column to a Spark SQL type."""
    from pyspark.sql import types as T

    kind, p, s = oracle_kind(datatype, precision, scale)
    if kind == "date":
        return T.TimestampType()
    if kind == "decimal":
        return T.DecimalType(p, s)
    return T.StringType()


def hash_ssn(ssn: Optional[str]) -> Optional[str]:
    """SHA-256 hex digest of an SSN. Matches Spark ``sha2(col, 256)``."""
    if ssn is None:
        return None
    return hashlib.sha256(str(ssn).encode("utf-8")).hexdigest()


def pp_year_num(pp_end_year: int, pp_num: int) -> int:
    """Derive PP_YEAR_NUM: year concatenated with zero-padded pay-period number."""
    return int(f"{int(pp_end_year)}{int(pp_num):02d}")


def yyyymmdd_to_iso(value: Optional[str]) -> Optional[str]:
    """Convert YYYYMMDD -> YYYY-MM-DD (returns None for blank/invalid)."""
    if not value:
        return None
    v = str(value).strip()
    if len(v) != 8 or not v.isdigit():
        return None
    return f"{v[0:4]}-{v[4:6]}-{v[6:8]}"


def mmddyyyy_to_iso(value: Optional[str]) -> Optional[str]:
    """Convert MMDDYYYY -> YYYY-MM-DD (returns None for blank/invalid)."""
    if not value:
        return None
    v = str(value).strip()
    if len(v) != 8 or not v.isdigit():
        return None
    return f"{v[4:8]}-{v[0:2]}-{v[2:4]}"


def parse_signed_decimal(raw: Optional[str], scale: int = 2) -> Optional[float]:
    """Parse a zoned/overpunch-free signed decimal where a trailing '-' (or a
    leading '-') denotes a negative value and digits are implied-decimal."""
    if raw is None:
        return None
    v = str(raw).strip()
    if not v:
        return None
    sign = 1
    if v.endswith("-") or v.startswith("-"):
        sign = -1
        v = v.replace("-", "")
    if not v.isdigit():
        return None
    return sign * int(v) / (10 ** scale)
