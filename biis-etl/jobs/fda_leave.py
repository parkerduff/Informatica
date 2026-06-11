"""FDA Leave workflow (migrated from XML/FDA_Leave).

Validates that every employee with an FDA leave transaction
(``HI_PM_FDA_TATRAN_TBL``) has matching YTD / PAD / MER staging records.
Employees missing any required staging record are written to ``ERROR_TBL``.
"""
from __future__ import annotations

from typing import Dict, Optional

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from utils import db, notifications
from utils.config import get_config
from utils.schemas import column_names
from utils.spark import get_spark, parse_args

REQUIRED_STAGING = [
    ("CPM_YTD_DETAIL_STG_TBL", "MISSING_YTD", "No YTD detail record for employee"),
    ("CPM_PAD_DETAIL_STG_TBL", "MISSING_PAD", "No PAD detail record for employee"),
    ("CPM_MER_DETAIL_STG_TBL", "MISSING_MER", "No MER detail record for employee"),
]


def find_errors(fda: DataFrame, staging: Dict[str, DataFrame], run_date=None) -> DataFrame:
    """Return an ERROR_TBL-shaped DataFrame of employees missing staging rows."""
    spark = fda.sparkSession
    errors: Optional[DataFrame] = None
    for table, err_type, msg in REQUIRED_STAGING:
        stg = staging[table].select("FDA_EMP_ID").distinct()
        missing = (
            fda.join(stg, on="FDA_EMP_ID", how="left_anti")
            .select("FDA_BATCH_ID", "FDA_TK_NO", "FDA_EMP_ID")
            .withColumn("ERROR_TYPE", F.lit(err_type))
            .withColumn("ERROR_MSG", F.lit(msg))
            .withColumn("RUN_DATE", F.lit(str(run_date) if run_date else None))
        )
        errors = missing if errors is None else errors.unionByName(missing)
    if errors is None:  # pragma: no cover
        errors = spark.createDataFrame([], fda.select().schema)
    return errors.select(*column_names("ERROR_TBL"))


def count_errors_by_type(error_df: DataFrame) -> Dict[str, int]:
    rows = error_df.groupBy("ERROR_TYPE").count().collect()
    return {r["ERROR_TYPE"]: int(r["count"]) for r in rows}


def run(env: str = "test", run_date=None, spark=None) -> int:
    cfg = get_config(env)
    spark = spark or get_spark("fda_leave")

    fda = db.read_table(spark, "HI_PM_FDA_TATRAN_TBL", cfg)
    staging = {t: db.read_table(spark, t, cfg) for t, _, _ in REQUIRED_STAGING}

    errors = find_errors(fda, staging, run_date)
    db.write_table(spark, errors, "ERROR_TBL", cfg, mode="overwrite")

    n = errors.count()
    notifications.send_notification(
        f"FDA Leave validation produced {n} error(s)",
        f"Error breakdown by type: {count_errors_by_type(errors)}",
        cfg,
    )
    return n


def main(argv=None) -> None:
    args = parse_args("FDA Leave workflow", argv)
    n = run(env=args.env, run_date=args.run_date)
    print(f"FDA Leave OK: {n} error(s) written")


if __name__ == "__main__":
    main()
