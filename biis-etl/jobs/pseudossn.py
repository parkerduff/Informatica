"""PseudoSSN workflow (migrated from XML/Pseudossn).

Parses the fixed-width SDA file (Header / Detail / Trailer records), keeps only
the ``D`` detail rows, converts EBCDIC signed-overpunch amounts and MMDDYYYY
dates, dedups to the latest effective date per pseudo-SSN, and loads the
``PSEUDOSSN_FROM_SDA_TBL``, ``PSEUDOSSN_TBL`` and ``HI_ARCH_PSEUDOSSN_TBL``
tables.
"""
from __future__ import annotations

import datetime as dt
from typing import List, Optional, Tuple

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType

from utils import db, notifications
from utils.config import get_config
from utils.schemas import column_names
from utils.spark import get_spark, parse_args
from utils.validation import ValidationError

# (name, physical_offset, physical_length) -- fixed-width SDA layout.
LAYOUT: List[Tuple[str, int, int]] = [
    ("RECORD_TYPE", 0, 1),
    ("PSEUDO_SSN", 1, 9),
    ("REAL_SSN", 10, 9),
    ("LAST_NAME", 19, 30),
    ("FIRST_NAME", 49, 20),
    ("EFFECTIVE_DATE_RAW", 69, 8),
    ("AMOUNT_RAW", 77, 11),
]

_OVERPUNCH_POS = {"{": "0", "A": "1", "B": "2", "C": "3", "D": "4",
                  "E": "5", "F": "6", "G": "7", "H": "8", "I": "9"}
_OVERPUNCH_NEG = {"}": "0", "J": "1", "K": "2", "L": "3", "M": "4",
                  "N": "5", "O": "6", "P": "7", "Q": "8", "R": "9"}


def parse_overpunch(field: str) -> int:
    """Convert an EBCDIC signed-overpunch numeric string to int.

    The trailing character encodes both the final digit and the sign, e.g.
    ``"1234}"`` -> ``-12340`` and ``"1234{"`` -> ``12340``.
    """
    field = (field or "").strip()
    if not field:
        return 0
    prefix, last = field[:-1], field[-1]
    if last.isdigit():
        return int(field)
    if last in _OVERPUNCH_POS:
        return int(prefix + _OVERPUNCH_POS[last])
    if last in _OVERPUNCH_NEG:
        return -int(prefix + _OVERPUNCH_NEG[last])
    raise ValueError(f"Invalid overpunch character: {last!r}")


def parse_date_mmddyyyy(field: str) -> Optional[str]:
    """``"06112026" -> "2026-06-11"`` (ISO date string); blank -> None."""
    field = (field or "").strip()
    if not field or field == "0" * len(field):
        return None
    mm, dd, yyyy = field[0:2], field[2:4], field[4:8]
    return dt.date(int(yyyy), int(mm), int(dd)).isoformat()


_overpunch_udf = F.udf(lambda s: parse_overpunch(s) / 100.0 if s else None, DoubleType())
_date_udf = F.udf(parse_date_mmddyyyy, StringType())


def parse_fixed_width(spark, path: str) -> DataFrame:
    """Read the .dat file and slice every fixed-width field."""
    df = spark.read.text(path)
    for name, offset, length in LAYOUT:
        df = df.withColumn(name, F.trim(F.substring(F.col("value"), offset + 1, length)))
    return df.drop("value")


def filter_details(df: DataFrame) -> DataFrame:
    """Keep only ``D`` detail records (drop header/trailer)."""
    return df.filter(F.col("RECORD_TYPE") == "D")


def convert(df: DataFrame) -> DataFrame:
    return (
        df.withColumn("EFFECTIVE_DATE", _date_udf(F.col("EFFECTIVE_DATE_RAW")))
        .withColumn("AMOUNT", _overpunch_udf(F.col("AMOUNT_RAW")))
        .drop("EFFECTIVE_DATE_RAW", "AMOUNT_RAW")
    )


def to_sda_rows(df: DataFrame) -> DataFrame:
    return df.select(*column_names("PSEUDOSSN_FROM_SDA_TBL"))


def dedup_latest(df: DataFrame) -> DataFrame:
    """Keep the most recent effective date per pseudo-SSN."""
    w = Window.partitionBy("PSEUDO_SSN").orderBy(F.col("EFFECTIVE_DATE").desc_nulls_last())
    return (
        df.withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
        .select(*column_names("PSEUDOSSN_TBL"))
    )


def run(env: str = "test", file_path: Optional[str] = None,
        run_date=None, spark=None) -> int:
    cfg = get_config(env)
    spark = spark or get_spark("pseudossn")
    if not file_path:
        raise ValueError("--file-path is required for PseudoSSN")

    raw = parse_fixed_width(spark, file_path)
    details = convert(filter_details(raw))
    if details.rdd.isEmpty():
        raise ValidationError("No detail (D) records in PseudoSSN input")

    sda = to_sda_rows(details)
    db.write_table(spark, sda, "PSEUDOSSN_FROM_SDA_TBL", cfg, mode="overwrite")

    deduped = dedup_latest(details)
    db.write_table(spark, deduped, "PSEUDOSSN_TBL", cfg, mode="overwrite")

    archive = deduped.withColumn("ARCHIVE_DATE", F.lit(str(run_date) if run_date else None))
    archive = archive.select(*column_names("HI_ARCH_PSEUDOSSN_TBL"))
    db.write_table(spark, archive, "HI_ARCH_PSEUDOSSN_TBL", cfg, mode="append")

    n = sda.count()
    notifications.send_notification(
        f"PseudoSSN processed {n} detail records",
        f"PSEUDOSSN_FROM_SDA_TBL loaded {n} rows; "
        f"PSEUDOSSN_TBL deduped to {deduped.count()} rows.",
        cfg,
    )
    return n


def main(argv=None) -> None:
    args = parse_args("PseudoSSN workflow", argv)
    n = run(env=args.env, file_path=args.file_path, run_date=args.run_date)
    print(f"PseudoSSN OK: {n} detail records")


if __name__ == "__main__":
    main()
