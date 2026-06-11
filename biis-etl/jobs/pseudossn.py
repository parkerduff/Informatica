"""PseudoSSN job — migration of the Informatica Pseudossn workflow.

Parses the fixed-width SDA file (layout taken from the PSEUDOSSN_FILE source
definition in XML/Pseudossn), keeps Detail records, converts dates and signed
decimals, dedupes by PSEUDOSSN keeping the latest EFFECTIVE_DATE, and loads
PSEUDOSSN_FROM_SDA_TBL, PSEUDOSSN_TBL and HI_ARCH_PSEUDOSSN_TBL.
"""
import argparse
import logging
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from jobs.spark_common import get_spark  # noqa: E402
from utils import schemas  # noqa: E402
from utils.db import get_current_pay_period, pyodbc_connection, write_table  # noqa: E402
from utils.notifications import send_notification  # noqa: E402
from utils.secrets import get_secret, load_config  # noqa: E402
from utils.validation import log_row_count, validate_row_count  # noqa: E402

logger = logging.getLogger("pseudossn")

PROCESS_NAME = "PSEUDOSSN"

DATE_FIELDS = {
    "HIRE_DATE", "UNIF_ALLOW_DATE", "CAREER_START_DATE", "CAREER_CONV_DATE",
    "PROBATION_DATE", "LAST_PAY_CHANGE", "CHARITY_EFF_DATE", "SEPARATION_DATE",
    "PCA_CONTR_EFF_START_DATE", "PCA_CONTR_EFF_END_DATE", "APP_LIMIT_DATE",
    "EFFECTIVE_DATE",
}
SIGNED_DECIMAL_FIELDS = {
    "UNIF_ALLOW_AMT": 2, "APPT_LIMIT_HRS": 2, "APPT_LIMIT_PAY": 2,
    "CHARITY_DED_AMT": 2, "QUARTERS_DEDUCTION": 2, "SUBSIST_DEDUCTION": 2,
    "MAX_ANNUAL_PAY": 2, "PCA_BIWEEKLY_AMOUNT": 2,
}
INTEGER_FIELDS = {"EFFECTIVE_SEQ", "PCA_CONTR_YEAR"}


def file_layout():
    return [f for f in schemas.fixed_width_layout("Pseudossn", "PSEUDOSSN_FILE")
            if not f["name"].startswith("FILLER")]


def table_columns():
    return schemas.column_names("Pseudossn", "PSEUDOSSN_FROM_SDA_TBL", "targets")


def parse_fixed_width(spark, path: str):
    """Parse the SDA file into one column per (non-filler) layout field."""
    from pyspark.sql import functions as F

    raw = spark.read.text(path)
    cols = []
    for f in file_layout():
        trimmed = F.trim(F.substring(F.col("value"), f["offset"] + 1, f["length"]))
        cols.append(
            F.when(trimmed == "", F.lit(None).cast("string"))
            .otherwise(trimmed).alias(f["name"])
        )
    rec_type = (
        F.when(F.substring(F.col("value"), 1, 1) == "H", F.lit("H"))
        .when(F.substring(F.col("value"), 1, 1) == "T", F.lit("T"))
        .otherwise(F.lit("D"))
        .alias("REC_TYPE")
    )
    return raw.select(rec_type, *cols)


def convert_date(col):
    """MMDDYYYY or YYYYMMDD -> timestamp."""
    from pyspark.sql import functions as F

    yyyymmdd = F.to_timestamp(col, "yyyyMMdd")
    mmddyyyy = F.to_timestamp(col, "MMddyyyy")
    return (
        F.when(col.isNull() | (col == "") | (col.rlike("^0+$")), F.lit(None).cast("timestamp"))
        .when(col.rlike("^(19|20)[0-9]{6}$"), yyyymmdd)
        .otherwise(mmddyyyy)
    )


def parse_signed_decimal(col, scale: int):
    """Numeric text whose last byte may carry the sign ('+'/'-')."""
    from pyspark.sql import functions as F

    last = F.substring(col, -1, 1)
    has_sign = last.isin("+", "-")
    digits = F.when(has_sign, col.substr(F.lit(1), F.length(col) - 1)).otherwise(col)
    sign = F.when(last == "-", F.lit(-1)).otherwise(F.lit(1))
    value = (digits.cast("decimal(18,0)") * sign) / (10 ** scale)
    return F.when(col.isNull() | (col == ""), F.lit(None)).otherwise(
        value.cast(f"decimal(10,{scale})")
    )


def transform_details(df, pay_period: dict):
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window

    details = df.filter(F.col("REC_TYPE") == "D").drop("REC_TYPE")
    for name in DATE_FIELDS:
        if name in details.columns:
            details = details.withColumn(name, convert_date(F.col(name)))
    for name, scale in SIGNED_DECIMAL_FIELDS.items():
        if name in details.columns:
            details = details.withColumn(name, parse_signed_decimal(F.col(name), scale))
    for name in INTEGER_FIELDS:
        if name in details.columns:
            details = details.withColumn(name, F.col(name).cast("decimal(5,0)"))

    # Positional rename: file fields (sans fillers) line up with the first N
    # table columns; PP_NUM / PP_END_YEAR are appended from the pay period.
    src_names = [f["name"] for f in file_layout()]
    tgt_names = table_columns()
    mapping = dict(zip(src_names, tgt_names))
    for src, tgt in mapping.items():
        if src != tgt:
            details = details.withColumnRenamed(src, tgt)
    details = (
        details.withColumn("PP_NUM", F.lit(pay_period["pp_num"]).cast("decimal(2,0)"))
        .withColumn("PP_END_YEAR", F.lit(pay_period["pp_end_year"]).cast("decimal(4,0)"))
    )
    missing = [c for c in tgt_names if c not in details.columns]
    for c in missing:
        details = details.withColumn(c, F.lit(None).cast("string"))
    details = details.select(*tgt_names)

    ordered = details.orderBy(F.col("PSEUDOSSN").asc(), F.col("EFFECTIVE_DATE").desc())
    w = Window.partitionBy("PSEUDOSSN").orderBy(
        F.col("EFFECTIVE_DATE").desc(), F.col("EFFECTIVE_SEQ").desc()
    )
    deduped = (
        details.withColumn("__rn", F.row_number().over(w))
        .filter(F.col("__rn") == 1)
        .drop("__rn")
    )
    return ordered, deduped


def run(env: str = None, file_path: str = None) -> dict:
    config = load_config(env)
    secret = get_secret("biis", config)
    spark = get_spark("biis-pseudossn")
    try:
        pay_period = get_current_pay_period(spark, config, secret)
        parsed = parse_fixed_width(spark, file_path)
        all_details, deduped = transform_details(parsed, pay_period)
        n_all = validate_row_count(all_details, expected_min=1, context="PseudoSSN detail records")
        n_dedup = deduped.count()
        write_table(all_details, "PSEUDOSSN_FROM_SDA_TBL", config, secret, mode="overwrite")
        write_table(deduped, "PSEUDOSSN_TBL", config, secret, mode="overwrite")
        write_table(all_details, "HI_ARCH_PSEUDOSSN_TBL", config, secret, mode="append")
        with pyodbc_connection(secret, config) as conn:
            log_row_count(conn, "PSEUDOSSN_FROM_SDA_TBL", PROCESS_NAME, n_all, pay_period)
            conn.commit()
        send_notification(
            "PseudoSSN load completed",
            f"Loaded {n_all} detail rows ({n_dedup} unique pseudo SSNs)",
            config,
        )
        return {"all": n_all, "deduped": n_dedup}
    except Exception as exc:
        send_notification("PseudoSSN load FAILED", str(exc), config)
        raise
    finally:
        spark.stop()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--env", default=None)
    parser.add_argument("--file-path", required=True)
    args = parser.parse_args()
    run(args.env, args.file_path)
    return 0


if __name__ == "__main__":
    sys.exit(main())
