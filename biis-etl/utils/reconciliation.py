"""Reconciliation engine -- proves zero deprecation of business logic.

For every migrated target table we compare the data produced by the PySpark
jobs (read back from SQL Server) against the golden expected dataset. A module
is considered fully migrated only when every table reconciles with zero diffs,
matching row counts and matching schema.
"""
from __future__ import annotations

import json
import logging
import os
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from utils.db import get_jdbc_properties, get_jdbc_url
from utils.secrets import Config, get_db_secret

logger = logging.getLogger(__name__)

# Numeric / datetime comparison tolerances.
DECIMAL_TOLERANCE = 1e-10

# Registry: module -> list of (table, key_columns, golden_filename).
# golden filename is relative to the golden fixtures directory.
ModuleSpec = Tuple[str, List[str], str]
MODULE_TABLES: Dict[str, List[ModuleSpec]] = {
    "pay_calendar": [
        ("PAY_PERIOD", ["PP_NUM", "PP_END_YEAR"],
         "pay_calendar_expected_pay_period.csv"),
    ],
    "comptime": [
        ("COMP_TIME_DAILY_TBL", ["SSN", "DAILY_DATE_EARNED"],
         "comptime_expected_comp_time_daily_tbl.csv"),
    ],
    "pseudossn": [
        ("PSEUDOSSN_FROM_SDA_TBL", ["PSEUDOSSN", "EFFECTIVE_DATE"],
         "pseudossn_expected_pseudossn_from_sda_tbl.csv"),
        ("PSEUDOSSN_TBL", ["PSEUDOSSN"],
         "pseudossn_expected_pseudossn_tbl.csv"),
    ],
    "fda_leave": [
        ("ERROR_TBL", ["SOURCE_KEY"],
         "fda_leave_expected_error_tbl.csv"),
    ],
    "ehrp2biis": [
        ("ACTION_PRIMARY_ALL", ["EVENT_ID"],
         "ehrp2biis_expected_action_primary_all.csv"),
        ("ACTION_SECONDARY_ALL", ["EVENT_ID"],
         "ehrp2biis_expected_action_secondary_all.csv"),
        ("ACTION_REMARKS_ALL", ["EVENT_ID", "REMARK_SEQ"],
         "ehrp2biis_expected_action_remarks_all.csv"),
    ],
    "cpm": [
        ("CPM_NIH_STAGING_TBL", ["SSN"], "cpm_nih_expected_staging.csv"),
        ("CPM_OIG_STAGING_TBL", ["SSN"], "cpm_oig_expected_staging.csv"),
        ("CPM_CDC_STAGING_TBL", ["SSN"], "cpm_cdc_expected_staging.csv"),
    ],
}


@dataclass
class ReconciliationResult:
    table: str
    module: str = ""
    expected_count: int = 0
    actual_count: int = 0
    row_count_match: bool = False
    schema_match: bool = False
    schema_diff: List[Tuple[str, str, str]] = field(default_factory=list)
    diff_count: int = 0
    diff_sample: str = ""
    column_diffs: Dict[str, int] = field(default_factory=dict)
    passed: bool = False
    error: Optional[str] = None


def _read_actual(spark: Any, config: Config, table: str) -> Any:  # pragma: no cover
    secret = get_db_secret(config)
    return (
        spark.read.format("jdbc")
        .option("url", get_jdbc_url(config))
        .option("dbtable", f"{config.database.schema}.{table}")
        .options(**get_jdbc_properties(config, secret))
        .load()
    )


def _read_golden(spark: Any, golden_csv_path: str) -> Any:
    # Read every column as text; the comparison casts to the actual DB types so
    # that int/decimal/string inference differences cannot create false diffs.
    return (
        spark.read.option("header", True)
        .option("inferSchema", False)
        .csv(golden_csv_path)
    )


_NUMERIC_PREFIXES = ("decimal", "double", "float", "int", "bigint", "smallint",
                     "tinyint", "long", "short")


def _normalize(df: Any, columns: List[str]) -> Any:
    """Cast comparable columns to canonical representations.

    All numeric types collapse to DECIMAL(38,10); DATETIME2 truncated to
    seconds; strings trimmed. Driving this off the DataFrame's own dtypes (which
    callers align to the DB schema first) keeps both sides byte-comparable.
    """
    from pyspark.sql import functions as F
    from pyspark.sql import types as T

    out = df
    for c in columns:
        dtype = dict(out.dtypes)[c]
        if dtype.startswith("timestamp") or dtype.startswith("date"):
            out = out.withColumn(c, F.date_trunc("second", F.col(c).cast(T.TimestampType())))
        elif dtype.startswith(_NUMERIC_PREFIXES):
            out = out.withColumn(c, F.round(F.col(c).cast(T.DecimalType(38, 10)), 10))
        elif dtype == "string":
            out = out.withColumn(c, F.trim(F.col(c)))
    return out


def reconcile_table(
    spark: Any,
    config: Config,
    table_name: str,
    key_columns: List[str],
    golden_csv_path: str,
    module: str = "",
) -> ReconciliationResult:  # pragma: no cover
    """Compare a SQL Server table against its golden expected CSV."""
    from pyspark.sql import functions as F

    result = ReconciliationResult(table=table_name, module=module)

    if not os.path.exists(golden_csv_path):
        result.error = f"golden CSV not found: {golden_csv_path}"
        return result

    actual = _read_actual(spark, config, table_name)
    expected = _read_golden(spark, golden_csv_path)

    # Compare on the set of columns present in the golden file (case-sensitive
    # match against actual columns, which JDBC returns in DB casing).
    actual_cols = {c.upper(): c for c in actual.columns}
    common: List[str] = []
    schema_diff: List[Tuple[str, str, str]] = []
    for ec in expected.columns:
        au = ec.upper()
        if au in actual_cols:
            common.append(ec)
        else:
            schema_diff.append((ec, "present-in-golden", "missing-in-actual"))

    # Align actual column names to golden naming for the comparison.
    for ec in common:
        if actual_cols[ec.upper()] != ec:
            actual = actual.withColumnRenamed(actual_cols[ec.upper()], ec)

    result.schema_diff = schema_diff
    result.schema_match = len(schema_diff) == 0

    result.actual_count = actual.count()
    result.expected_count = expected.count()
    result.row_count_match = result.actual_count == result.expected_count

    # Cast golden (all-string) columns to the actual DB column types so that the
    # two frames are type-aligned before canonical normalization.
    actual_dtypes = dict(actual.dtypes)
    expected_cast = expected.select(
        [F.col(c).cast(actual_dtypes[c]).alias(c) for c in common]
    )

    actual_n = _normalize(actual.select(common), common)
    expected_n = _normalize(expected_cast, common)

    non_key = [c for c in common if c.upper() not in {k.upper() for k in key_columns}]

    # Hash each row's full content for a fast equality check.
    def with_hash(df: Any) -> Any:
        return df.withColumn(
            "_row_hash", F.sha2(F.concat_ws("|", *[F.coalesce(F.col(c).cast("string"),
                                                              F.lit("\u0000")) for c in common]), 256)
        )

    a_h = with_hash(actual_n)
    e_h = with_hash(expected_n)

    # Rows present in golden but missing/different in actual and vice versa.
    only_expected = e_h.join(a_h, on="_row_hash", how="left_anti")
    only_actual = a_h.join(e_h, on="_row_hash", how="left_anti")
    diff_count = only_expected.count() + only_actual.count()
    result.diff_count = diff_count

    # Per-column diff attribution via key join (best effort).
    column_diffs: Dict[str, int] = {}
    if key_columns and non_key:
        ak = actual_n.select([F.col(c).alias(f"a_{c}") for c in common] +
                             [F.col(k).alias(k) for k in key_columns])
        ek = expected_n.select([F.col(c).alias(f"e_{c}") for c in common] +
                               [F.col(k).alias(k) for k in key_columns])
        joined = ek.join(ak, on=key_columns, how="inner")
        for c in non_key:
            mism = joined.filter(
                ~F.col(f"a_{c}").eqNullSafe(F.col(f"e_{c}"))
            ).count()
            if mism:
                column_diffs[c] = mism
    result.column_diffs = column_diffs

    if diff_count:
        sample_rows = only_expected.drop("_row_hash").limit(10).collect()
        result.diff_sample = "\n".join(str(r.asDict()) for r in sample_rows)

    result.passed = (
        result.row_count_match and result.schema_match and result.diff_count == 0
    )
    logger.info(
        "Reconcile %s: expected=%d actual=%d diffs=%d schema_match=%s -> %s",
        table_name, result.expected_count, result.actual_count,
        result.diff_count, result.schema_match, "PASS" if result.passed else "FAIL",
    )
    return result


def reconcile_module(
    spark: Any, config: Config, module_name: str, golden_dir: str
) -> List[ReconciliationResult]:  # pragma: no cover
    specs = MODULE_TABLES.get(module_name)
    if specs is None:
        raise KeyError(f"Unknown module {module_name!r}")
    results = []
    for table, keys, golden_file in specs:
        results.append(
            reconcile_table(
                spark, config, table, keys,
                str(Path(golden_dir) / golden_file), module=module_name,
            )
        )
    return results


def generate_report(results: List[ReconciliationResult], output_dir: str) -> str:
    """Write JSON + HTML reconciliation reports. Returns the JSON path."""
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)

    payload = {
        "total_tables": len(results),
        "passed": sum(1 for r in results if r.passed),
        "failed": sum(1 for r in results if not r.passed),
        "results": [asdict(r) for r in results],
    }
    json_path = out / "reconciliation.json"
    json_path.write_text(json.dumps(payload, indent=2, default=str))

    rows = []
    for r in results:
        status = "PASS" if r.passed else "FAIL"
        color = "#d4edda" if r.passed else "#f8d7da"
        rows.append(
            f"<tr style='background:{color}'><td>{r.module}</td><td>{r.table}</td>"
            f"<td>{r.expected_count}</td><td>{r.actual_count}</td>"
            f"<td>{r.diff_count}</td><td>{r.schema_match}</td><td>{status}</td></tr>"
        )
    html = (
        "<html><head><title>BIIS Reconciliation</title></head><body>"
        f"<h1>BIIS ETL Reconciliation Report</h1>"
        f"<p>{payload['passed']}/{payload['total_tables']} tables passed.</p>"
        "<table border='1' cellpadding='6' cellspacing='0'>"
        "<tr><th>Module</th><th>Table</th><th>Expected</th><th>Actual</th>"
        "<th>Diffs</th><th>Schema OK</th><th>Status</th></tr>"
        + "".join(rows) + "</table></body></html>"
    )
    (out / "reconciliation.html").write_text(html)
    return str(json_path)
