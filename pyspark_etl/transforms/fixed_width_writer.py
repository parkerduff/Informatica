"""Fixed-width flat-file writer (the inverse of the fixed-width reader).

Informatica flat-file *targets* emit each field left-justified/space-padded (or
right-justified for numerics) at a fixed width. :func:`write_fixed_width` builds
each output line by concatenating padded columns and writes a single text file.

When ``layout`` (a list of :class:`~pyspark_etl.config.schemas.FixedField`) is
supplied the exact agency record format is produced; otherwise every column is
emitted at its current string width separated by nothing (useful for debugging).
"""
from __future__ import annotations

from typing import List, Optional

from pyspark_etl.config.schemas import FixedField


def _pad(value, length: int, numeric: bool) -> str:
    s = "" if value is None else str(value)
    s = s[:length]
    return s.rjust(length, "0") if numeric else s.ljust(length)


def to_fixed_width_line(row: dict, layout: List[FixedField]) -> str:
    """Render one record dict into a fixed-width line using ``layout``."""
    parts = []
    for f in layout:
        numeric = f.datatype.lower().startswith(("number", "decimal", "int"))
        parts.append(_pad(row.get(f.name), f.length, numeric))
    return "".join(parts)


def write_fixed_width(df, output_path: str, layout: Optional[List[FixedField]] = None,
                      coalesce: bool = True) -> None:
    """Write a Spark DataFrame to a fixed-width text file at ``output_path``."""
    from pyspark.sql import functions as F

    if layout:
        cols = []
        for f in layout:
            numeric = f.datatype.lower().startswith(("number", "decimal", "int"))
            col = F.coalesce(F.col(f.name).cast("string"), F.lit(""))
            col = F.substring(col, 1, f.length)
            if numeric:
                col = F.lpad(col, f.length, "0")
            else:
                col = F.rpad(col, f.length, " ")
            cols.append(col)
        line = F.concat(*cols)
    else:
        line = F.concat_ws("", *[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in df.columns])

    out = df.select(line.alias("value"))
    if coalesce:
        out = out.coalesce(1)
    out.write.mode("overwrite").text(output_path)
