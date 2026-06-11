"""EHRP2BIIS ETL — migration of mapping m_EHRP2BIIS_UPDATE.

Joins the staged actions (NWK_NEW_EHRP_ACTIONS_TBL) to PS_GVT_JOB on
EMPLID / EMPL_RCD / EFFDT / EFFSEQ, assigns EVENT_IDs from
SEQUENCE_NUM_TBL (lkp_OLD_SEQUENCE_NUMBER + sequence generator) and writes
NWK_ACTION_PRIMARY_TBL, NWK_ACTION_SECONDARY_TBL and EHRP_RECS_TRACKING_TBL.

The 260/209-column Informatica expression set is ported as a name-driven
mapping from PS_GVT_JOB; columns with no PS_GVT_JOB counterpart default to
NULL exactly as the original mapping's unconnected ports did.
"""
import argparse
import logging
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from jobs.spark_common import get_spark  # noqa: E402
from utils import schemas  # noqa: E402
from utils.db import pyodbc_connection, read_table, write_table  # noqa: E402
from utils.notifications import send_notification  # noqa: E402
from utils.secrets import get_secret, load_config  # noqa: E402
from utils.validation import validate_row_count  # noqa: E402

logger = logging.getLogger("ehrp2biis.etl")

JOIN_KEYS = ["EMPLID", "EMPL_RCD", "EFFDT", "EFFSEQ"]

# Explicit port wiring for identity columns whose names differ between
# PS_GVT_JOB and the action tables.
PRIMARY_EXPLICIT = {
    "SSN": "EMPLID",
    "EVENT_EFF_DTE": "EFFDT",
    "EFFSEQ": "EFFSEQ",
    "EMPL_REC_NO": "EMPL_RCD",
}


def lookup_old_sequence_number(conn) -> int:
    cur = conn.cursor()
    cur.execute(
        "SELECT MAX(OLD_SEQUENCE_NUMBER) FROM SEQUENCE_NUM_TBL"
    )
    row = cur.fetchone()
    return int(row[0]) if row and row[0] is not None else 0


def cast_for(field: dict, col):
    from pyspark.sql import functions as F  # noqa: F401

    return col.cast(schemas.spark_type_for(field))


def build_target(joined, target_fields, base_event_id, explicit=None):
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window

    explicit = explicit or {}
    source_cols = set(joined.columns)
    w = Window.orderBy("EMPLID", "EMPL_RCD", "EFFDT", "EFFSEQ")
    out = joined.withColumn(
        "EVENT_ID",
        (F.lit(base_event_id) + F.row_number().over(w)).cast("decimal(15,0)"),
    )
    select_cols = []
    for f in target_fields:
        name = f["name"]
        if name == "EVENT_ID":
            select_cols.append(F.col("EVENT_ID"))
        elif name == "LOAD_DATE":
            select_cols.append(F.current_timestamp().alias("LOAD_DATE"))
        elif name in explicit and explicit[name] in source_cols:
            select_cols.append(cast_for(f, F.col(explicit[name])).alias(name))
        elif name in source_cols and name not in JOIN_KEYS:
            select_cols.append(cast_for(f, F.col(name)).alias(name))
        elif name in JOIN_KEYS:
            select_cols.append(cast_for(f, F.col(name)).alias(name))
        else:
            select_cols.append(F.lit(None).cast(schemas.spark_type_for(f)).alias(name))
    return out.select(*select_cols)


def run(env: str = None, run_date: str = None) -> dict:
    config = load_config(env)
    secret = get_secret("biis", config)
    spark = get_spark("biis-ehrp2biis-etl")
    try:
        from pyspark.sql import functions as F

        actions = read_table(
            spark, "NWK_NEW_EHRP_ACTIONS_TBL", config, secret, columns=JOIN_KEYS
        )
        job_cols = schemas.column_names("EHRP2BIIS_UPDATE", "PS_GVT_JOB")
        ps_gvt_job = read_table(spark, "PS_GVT_JOB", config, secret, columns=job_cols)

        joined = actions.alias("a").join(ps_gvt_job.alias("j"), on=JOIN_KEYS, how="inner")
        n = validate_row_count(joined, expected_min=1, context="EHRP2BIIS join")

        with pyodbc_connection(secret, config) as conn:
            base_event_id = lookup_old_sequence_number(conn)

        prim_fields = schemas.get_table_fields("EHRP2BIIS_UPDATE", "NWK_ACTION_PRIMARY_TBL", "targets")
        sec_fields = schemas.get_table_fields("EHRP2BIIS_UPDATE", "NWK_ACTION_SECONDARY_TBL", "targets")

        primary = build_target(joined, prim_fields, base_event_id, PRIMARY_EXPLICIT)
        secondary = build_target(joined, sec_fields, base_event_id)

        from pyspark.sql.window import Window

        w = Window.orderBy("EMPLID", "EMPL_RCD", "EFFDT", "EFFSEQ")
        tracking = joined.withColumn(
            "BIIS_EVENT_ID",
            (F.lit(base_event_id) + F.row_number().over(w)).cast("decimal(10,0)"),
        ).select(
            F.col("EMPLID").cast("string").alias("EMPLID"),
            F.col("EMPL_RCD").cast("decimal(2,0)").alias("EMPL_RCD"),
            F.col("EFFDT").cast("timestamp").alias("EFFDT"),
            F.col("EFFSEQ").cast("decimal(3,0)").alias("EFFSEQ"),
            F.current_timestamp().alias("EVENT_SUBMITTED_DT"),
            F.col("GVT_WIP_STATUS").cast("string").alias("GVT_WIP_STATUS"),
            F.lit(None).cast("string").alias("NOA_CD"),
            F.lit(None).cast("string").alias("NOA_SUFFIX_CD"),
            F.col("BIIS_EVENT_ID"),
            F.current_timestamp().alias("LOAD_DATE"),
        )

        write_table(primary, "NWK_ACTION_PRIMARY_TBL", config, secret, mode="overwrite")
        write_table(secondary, "NWK_ACTION_SECONDARY_TBL", config, secret, mode="overwrite")
        write_table(tracking, "EHRP_RECS_TRACKING_TBL", config, secret, mode="overwrite")

        send_notification(
            "EHRP2BIIS ETL completed",
            f"Loaded {n} action events into primary/secondary/tracking tables",
            config,
        )
        return {"events": n}
    except Exception as exc:
        send_notification("EHRP2BIIS ETL FAILED", str(exc), config)
        raise
    finally:
        spark.stop()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--env", default=None)
    parser.add_argument("--run-date", default=None)
    args = parser.parse_args()
    run(args.env, args.run_date)
    return 0


if __name__ == "__main__":
    sys.exit(main())
