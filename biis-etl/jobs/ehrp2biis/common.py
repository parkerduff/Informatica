"""Shared helpers for the EHRP2BIIS preload/etl/afterload stages."""
from __future__ import annotations

from typing import Dict, Optional

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from utils import db
from utils.schemas import TABLES, spark_field

JOIN_KEYS = ["EMPLID", "EMPL_RCD", "EFFDT", "EFFSEQ"]
EVENT_ID_SEQ = "EVENT_ID"
NINE_HUNDRED_SERIES = 9000000000


def build_full_df(base: DataFrame, table: str, overrides: Optional[Dict[str, object]] = None) -> DataFrame:
    """Return a DataFrame with the full schema of ``table``.

    Columns are populated, in priority order, from ``overrides``, then from a
    same-named column in ``base``; everything else is a typed NULL.
    """
    overrides = overrides or {}
    base_cols = set(base.columns)
    select_exprs = []
    for col in TABLES[table]:
        name = col[0]
        field = spark_field(col)
        expr: Column
        if name in overrides:
            override = overrides[name]
            expr = F.lit(None).cast(field.dataType) if override is None else override  # type: ignore[assignment]
        elif name in base_cols:
            expr = F.col(name).cast(field.dataType)
        else:
            expr = F.lit(None).cast(field.dataType)
        select_exprs.append(expr.alias(name))
    return base.select(*select_exprs)


def get_sequence(cfg, name: str = EVENT_ID_SEQ) -> int:
    rows = db.fetch_all(cfg, "SELECT SEQ_VALUE FROM SEQUENCE_NUM_TBL WHERE SEQ_NAME = ?", [name])
    return int(rows[0][0]) if rows else 0


def set_sequence(cfg, value: int, name: str = EVENT_ID_SEQ) -> None:
    existing = db.fetch_all(cfg, "SELECT COUNT(*) FROM SEQUENCE_NUM_TBL WHERE SEQ_NAME = ?", [name])
    if existing and int(existing[0][0]) > 0:
        db.execute(cfg, "UPDATE SEQUENCE_NUM_TBL SET SEQ_VALUE = ? WHERE SEQ_NAME = ?", [value, name])
    else:
        db.execute(cfg, "INSERT INTO SEQUENCE_NUM_TBL (SEQ_NAME, SEQ_VALUE) VALUES (?, ?)", [name, value])
