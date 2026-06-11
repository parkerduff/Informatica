"""Reconciliation engine proving zero deprecation versus golden datasets."""
import dataclasses
import html
import json
import logging
import os
from typing import List, Optional

logger = logging.getLogger(__name__)

DECIMAL_TOLERANCE = 1e-10


@dataclasses.dataclass
class ReconciliationResult:
    table: str
    expected_count: int = 0
    actual_count: int = 0
    row_count_match: bool = False
    schema_match: bool = False
    schema_diff: list = dataclasses.field(default_factory=list)
    diff_count: int = 0
    diff_sample: str = ""
    column_diffs: dict = dataclasses.field(default_factory=dict)
    passed: bool = False

    def to_dict(self) -> dict:
        return dataclasses.asdict(self)


def _normalized(df, key_columns: List[str]):
    """Cast every column to a comparable canonical string form."""
    from pyspark.sql import functions as F
    from pyspark.sql import types as T

    out = df
    for field in df.schema.fields:
        col = F.col(field.name)
        if isinstance(field.dataType, T.DecimalType):
            out = out.withColumn(field.name, col.cast("string"))
        elif isinstance(field.dataType, (T.DoubleType, T.FloatType)):
            out = out.withColumn(field.name, F.round(col.cast("double"), 8).cast("string"))
        elif isinstance(field.dataType, (T.TimestampType, T.DateType)):
            out = out.withColumn(field.name, F.date_format(col, "yyyy-MM-dd HH:mm:ss"))
        else:
            trimmed = F.trim(col.cast("string"))
            out = out.withColumn(
                field.name,
                F.when(trimmed == "", F.lit(None).cast("string")).otherwise(trimmed),
            )
    return out


def reconcile_table(spark, config: dict, table_name: str, key_columns: List[str],
                    golden_csv_path: str, secret: Optional[dict] = None,
                    ignore_columns: Optional[List[str]] = None) -> ReconciliationResult:
    from pyspark.sql import functions as F

    from utils import db as dbutil
    from utils.secrets import get_secret

    secret = secret or get_secret("biis", config)
    ignore_columns = [c.upper() for c in (ignore_columns or [])]

    result = ReconciliationResult(table=table_name)

    actual = dbutil.read_table(spark, table_name, config, secret)
    expected = spark.read.csv(golden_csv_path, header=True, inferSchema=False)

    compare_cols = [c for c in expected.columns if c.upper() not in ignore_columns]
    actual = actual.select([c for c in actual.columns if c in compare_cols or c in expected.columns])

    result.expected_count = expected.count()
    result.actual_count = actual.count()
    result.row_count_match = result.expected_count == result.actual_count

    exp_cols = [c.upper() for c in expected.columns]
    act_cols = [c.upper() for c in actual.columns]
    missing = [c for c in exp_cols if c not in act_cols]
    result.schema_diff = [(c, "expected", "missing") for c in missing]
    result.schema_match = not missing

    if not result.schema_match:
        result.passed = False
        return result

    a = _normalized(actual.select(*expected.columns), key_columns).alias("a")
    e = _normalized(expected, key_columns).alias("e")

    value_cols = [c for c in expected.columns if c not in key_columns and c.upper() not in ignore_columns]
    join_cond = [a[k].eqNullSafe(e[k]) for k in key_columns]
    joined = a.join(e, join_cond, "full_outer")

    mismatch_exprs = []
    for c in value_cols:
        neq = ~a[c].eqNullSafe(e[c])
        mismatch_exprs.append(F.when(neq, F.lit(1)).otherwise(F.lit(0)).alias(f"__diff_{c}"))
    missing_row = F.when(
        a[key_columns[0]].isNull() | e[key_columns[0]].isNull(), F.lit(1)
    ).otherwise(F.lit(0)).alias("__missing_row")

    diff_df = joined.select(
        *[F.coalesce(a[k], e[k]).alias(k) for k in key_columns],
        *[a[c].alias(f"actual_{c}") for c in value_cols],
        *[e[c].alias(f"expected_{c}") for c in value_cols],
        *mismatch_exprs,
        missing_row,
    )
    diff_flags = [F.col(f"__diff_{c}") for c in value_cols] + [F.col("__missing_row")]
    total_flag = sum(diff_flags[1:], diff_flags[0]) if diff_flags else F.lit(0)
    diff_rows = diff_df.filter(total_flag > 0)
    result.diff_count = diff_rows.count()

    for c in value_cols:
        n = diff_df.agg(F.sum(F.col(f"__diff_{c}"))).collect()[0][0] or 0
        if n:
            result.column_diffs[c] = int(n)

    if result.diff_count:
        sample = diff_rows.limit(10).collect()
        result.diff_sample = "\n".join(str(r.asDict()) for r in sample)

    result.passed = result.row_count_match and result.schema_match and result.diff_count == 0
    return result


def reconcile_module(spark, config: dict, module_name: str, golden_dir: str,
                     table_specs: List[dict]) -> List[ReconciliationResult]:
    """table_specs: [{"table": ..., "key_columns": [...], "golden_file": ..., "ignore_columns": [...]}]"""
    results = []
    for spec in table_specs:
        path = os.path.join(golden_dir, spec["golden_file"])
        logger.info("Reconciling %s.%s against %s", module_name, spec["table"], path)
        results.append(
            reconcile_table(
                spark, config, spec["table"], spec["key_columns"], path,
                ignore_columns=spec.get("ignore_columns"),
            )
        )
    return results


def generate_report(results: List[ReconciliationResult], output_dir: str) -> None:
    os.makedirs(output_dir, exist_ok=True)
    payload = [r.to_dict() for r in results]
    with open(os.path.join(output_dir, "reconciliation.json"), "w") as f:
        json.dump(payload, f, indent=2, default=str)

    rows = []
    for r in results:
        status = "PASS" if r.passed else "FAIL"
        rows.append(
            f"<tr><td>{html.escape(r.table)}</td><td>{r.expected_count}</td>"
            f"<td>{r.actual_count}</td><td>{r.diff_count}</td><td>{status}</td></tr>"
        )
    doc = (
        "<html><body><h1>BIIS Reconciliation Report</h1>"
        "<table border='1'><tr><th>Table</th><th>Expected</th><th>Actual</th>"
        "<th>Diffs</th><th>Status</th></tr>" + "".join(rows) + "</table></body></html>"
    )
    with open(os.path.join(output_dir, "reconciliation.html"), "w") as f:
        f.write(doc)
