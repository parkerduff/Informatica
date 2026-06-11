#!/usr/bin/env python3
"""PseudoSSN job -- migration of the Informatica ``Pseudossn`` SDA load.

Reads a fixed-width SDA file (Header / Detail / Trailer records), parses the
detail records using the physical layout derived from PSEUDOSSN_FROM_SDA_TBL,
converts dates and signed decimals, deduplicates to the latest effective record
per pseudo SSN, and writes the SDA, deduplicated and archive tables.

The fixed-width layout is exposed via :func:`build_layout` and imported by the
synthetic golden generator so the writer and reader never drift.
"""
from __future__ import annotations

import argparse
import logging
from typing import Any, List, Tuple

from jobs.common import oracle_kind
from jobs.schemas import PSEUDOSSN_FROM_SDA_TBL_COLS
from utils.db import pyodbc_connection, write_table
from utils.notifications import send_notification
from utils.secrets import Config, get_db_secret, load_config
from utils.spark import get_spark
from utils.validation import log_row_count, validate_row_count

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
logger = logging.getLogger("pseudossn")

PROCESS_NAME = "PSEUDOSSN"

# Record-type indicator occupies column 1; detail fields follow from column 2.
RECTYPE_WIDTH = 1
DATE_WIDTH = 8  # YYYYMMDD in the flat file

# Layout entry: (name, start_1based, width, kind, scale)
Layout = List[Tuple[str, int, int, str, int]]


def build_layout() -> Layout:
    """Build the detail-record fixed-width layout from the table column specs."""
    layout: Layout = []
    pos = 1 + RECTYPE_WIDTH  # 1-based position after the record-type indicator
    for name, dt, prec, scale in PSEUDOSSN_FROM_SDA_TBL_COLS:
        kind, p, s = oracle_kind(dt, prec, scale)
        if kind == "date":
            width = DATE_WIDTH
        elif kind == "decimal":
            width = p
        else:
            width = p
        layout.append((name, pos, width, kind, s))
        pos += width
    return layout


def record_width(layout: Layout) -> int:
    return RECTYPE_WIDTH + sum(w for _, _, w, _, _ in layout)


def parse_details(spark: Any, file_path: str, layout: Layout) -> Any:
    from pyspark.sql import functions as F
    from pyspark.sql import types as T

    lines = spark.read.text(file_path)
    rectype = F.substring(F.col("value"), 1, RECTYPE_WIDTH)
    details = lines.filter(rectype == F.lit("D"))

    cols = []
    for name, start, width, kind, scale in layout:
        raw = F.trim(F.substring(F.col("value"), start, width))
        if kind == "date":
            col = F.to_timestamp(raw, "yyyyMMdd").alias(name)
        elif kind == "decimal":
            # zero-padded scaled integer -> decimal value
            sign = F.when(raw.endswith("-"), F.lit(-1)).otherwise(F.lit(1))
            digits = F.regexp_replace(raw, "[^0-9]", "")
            val = (sign * digits.cast(T.DecimalType(38, 0)) / F.pow(F.lit(10), F.lit(scale)))
            col = val.cast(T.DecimalType(max(int(width), 1), scale)).alias(name)
        else:
            col = raw.alias(name)
        cols.append(col)
    return details.select(*cols)


def deduplicate(df: Any) -> Any:
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    w = Window.partitionBy("PSEUDOSSN").orderBy(
        F.col("EFFECTIVE_DATE").desc(), F.col("EFFECTIVE_SEQ").desc()
    )
    return (
        df.withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )


def run(config: Config, file_path: str) -> int:  # pragma: no cover
    from pyspark.sql import functions as F

    spark = get_spark("pseudossn")
    secret = get_db_secret(config)
    layout = build_layout()
    logger.info("SDA record width = %d bytes", record_width(layout))

    details = parse_details(spark, file_path, layout).withColumn(
        "LOAD_DATE", F.current_timestamp()
    )
    detail_count = validate_row_count(details, expected_min=1, context="pseudossn detail records")

    target_cols = [name for name, *_ in layout]
    sda = details.select(*target_cols, "LOAD_DATE")
    write_table(sda, "PSEUDOSSN_FROM_SDA_TBL", config, secret, mode="append")
    write_table(sda, "HI_ARCH_PSEUDOSSN_TBL", config, secret, mode="append")

    deduped = deduplicate(details).select(*target_cols, "LOAD_DATE")
    dedup_count = deduped.count()
    write_table(deduped, "PSEUDOSSN_TBL", config, secret, mode="append")

    pay_period = {"pp_end_year": None, "pp_num": None}
    with pyodbc_connection(secret, config) as conn:
        log_row_count(conn, "PSEUDOSSN_FROM_SDA_TBL", PROCESS_NAME, detail_count, pay_period)

    send_notification(
        f"{PROCESS_NAME}: SDA load complete",
        f"Parsed {detail_count} detail records, {dedup_count} unique pseudo SSNs.",
        config,
    )
    logger.info("pseudossn complete: %d detail, %d unique", detail_count, dedup_count)
    return detail_count


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--env", default=None)
    ap.add_argument("--file-path", required=True)
    args = ap.parse_args()
    config = load_config(args.env)
    run(config, args.file_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
