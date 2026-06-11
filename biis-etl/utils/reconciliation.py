"""Row-level, column-level reconciliation between job output and golden data.

Used by:
* ``tests/regression/`` (automated in CI)
* ``scripts/reconcile.py`` (manual validation / CLI report generation)
* parallel-run comparisons (Informatica vs PySpark)

The engine is pandas-based so it has no Spark dependency and can run against
any DB-API connection (sqlite or pyodbc).
"""
from __future__ import annotations

import math
import os
from dataclasses import dataclass, field
from typing import Any, Dict, List, Sequence

import pandas as pd

# Tolerances for fuzzy comparison (Oracle NUMBER vs SQL Server DECIMAL, etc.)
NUMERIC_TOLERANCE = 1e-10


@dataclass
class ReconciliationResult:
    table: str
    module: str = ""
    expected_count: int = 0
    actual_count: int = 0
    row_count_match: bool = False
    schema_match: bool = False
    schema_diff: List[tuple] = field(default_factory=list)
    diff_count: int = 0
    diff_sample: str = ""
    column_diffs: Dict[str, int] = field(default_factory=dict)

    @property
    def passed(self) -> bool:
        return self.row_count_match and self.schema_match and self.diff_count == 0

    @property
    def verdict(self) -> str:
        return "PASS" if self.passed else "FAIL"


def golden_expected_path(golden_dir: str, module: str, table: str) -> str:
    return os.path.join(golden_dir, f"{module}_expected_{table.lower()}.csv")


def _read_actual(db_conn, table: str) -> pd.DataFrame:
    return pd.read_sql_query(f"SELECT * FROM {table}", db_conn)


def _normalize(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [c.lower() for c in df.columns]
    return df


def _norm_key(value: Any) -> str:
    """Normalise a key value to a canonical string for joining.

    Handles the CSV-vs-DB type drift (e.g. ``900000000`` int from a CSV vs the
    ``"900000000"`` string from a VARCHAR column, or ``26.0`` vs ``26``).
    """
    if value is None or pd.isna(value):
        return ""
    if isinstance(value, float) and value.is_integer():
        return str(int(value))
    try:
        f = float(value)
        if f.is_integer():
            return str(int(f))
    except (TypeError, ValueError):
        pass
    return str(value).strip()


def _values_equal(a: Any, b: Any) -> bool:
    a_null = a is None or (isinstance(a, float) and math.isnan(a)) or pd.isna(a)
    b_null = b is None or (isinstance(b, float) and math.isnan(b)) or pd.isna(b)
    if a_null and b_null:
        return True
    if a_null or b_null:
        return False
    # numeric comparison with tolerance
    try:
        fa, fb = float(a), float(b)
        return abs(fa - fb) <= NUMERIC_TOLERANCE
    except (TypeError, ValueError):
        pass
    # date / datetime normalization to second precision
    sa, sb = str(a).strip(), str(b).strip()
    if sa == sb:
        return True
    # trim trailing ".0" / fractional seconds noise
    return sa.split(".")[0] == sb.split(".")[0]


def reconcile_dataframes(
    actual: pd.DataFrame,
    expected: pd.DataFrame,
    key_columns: Sequence[str],
    table: str = "",
    module: str = "",
) -> ReconciliationResult:
    actual = _normalize(actual)
    expected = _normalize(expected)
    keys = [k.lower() for k in key_columns]

    result = ReconciliationResult(table=table, module=module)
    result.expected_count = len(expected)
    result.actual_count = len(actual)
    result.row_count_match = result.expected_count == result.actual_count

    # schema comparison (column name sets)
    exp_cols, act_cols = set(expected.columns), set(actual.columns)
    if exp_cols != act_cols:
        result.schema_match = False
        for c in sorted(exp_cols - act_cols):
            result.schema_diff.append((c, "present", "missing"))
        for c in sorted(act_cols - exp_cols):
            result.schema_diff.append((c, "missing", "present"))
        # cannot do row-level diff with mismatched schema
        return result
    result.schema_match = True

    compare_cols = [c for c in expected.columns if c not in keys]

    def keyed(df):
        out = {}
        for rec in df.to_dict("records"):
            out[tuple(_norm_key(rec[k]) for k in keys)] = rec
        return out

    exp_map = keyed(expected)
    act_map = keyed(actual)

    diff_rows: List[str] = []
    for key, exp_row in exp_map.items():
        if key not in act_map:
            result.diff_count += 1
            if len(diff_rows) < 10:
                diff_rows.append(f"key={key}: MISSING in actual")
            continue
        act_row = act_map[key]
        row_diffs = []
        for c in compare_cols:
            if not _values_equal(exp_row[c], act_row[c]):
                result.column_diffs[c] = result.column_diffs.get(c, 0) + 1
                row_diffs.append(f"{c}: expected={exp_row[c]!r} actual={act_row[c]!r}")
        if row_diffs:
            result.diff_count += 1
            if len(diff_rows) < 10:
                diff_rows.append(f"key={key}: " + "; ".join(row_diffs))

    result.diff_sample = "\n".join(diff_rows)
    return result


def reconcile_table(
    spark,
    db_conn,
    module: str,
    table: str,
    key_columns: Sequence[str],
    golden_dir: str,
) -> ReconciliationResult:
    """Compare DB table ``table`` against its golden expected CSV.

    ``spark`` is accepted for API compatibility but the comparison is performed
    with pandas.  ``db_conn`` is any DB-API connection.
    """
    actual = _read_actual(db_conn, table)
    expected = pd.read_csv(golden_expected_path(golden_dir, module, table))
    return reconcile_dataframes(actual, expected, key_columns, table=table, module=module)


def render_html(result: ReconciliationResult) -> str:
    color = "#1a7f37" if result.passed else "#cf222e"
    rows = ""
    if result.schema_diff:
        rows += "<h3>Schema diff</h3><ul>"
        for col, exp, act in result.schema_diff:
            rows += f"<li>{col}: expected={exp}, actual={act}</li>"
        rows += "</ul>"
    if result.column_diffs:
        rows += "<h3>Column-level diffs</h3><ul>"
        for col, n in sorted(result.column_diffs.items(), key=lambda x: -x[1]):
            rows += f"<li>{col}: {n} row(s) differ</li>"
        rows += "</ul>"
    if result.diff_sample:
        rows += f"<h3>First mismatched rows</h3><pre>{result.diff_sample}</pre>"
    return f"""<!doctype html>
<html><head><meta charset="utf-8"><title>{result.module}/{result.table}</title></head>
<body style="font-family: monospace">
<h1 style="color:{color}">{result.module}/{result.table}: {result.verdict}</h1>
<table border="1" cellpadding="6" cellspacing="0">
<tr><th>Metric</th><th>Value</th></tr>
<tr><td>Expected rows</td><td>{result.expected_count}</td></tr>
<tr><td>Actual rows</td><td>{result.actual_count}</td></tr>
<tr><td>Row count match</td><td>{result.row_count_match}</td></tr>
<tr><td>Schema match</td><td>{result.schema_match}</td></tr>
<tr><td>Rows differing</td><td>{result.diff_count}</td></tr>
</table>
{rows}
</body></html>
"""


def write_report(result: ReconciliationResult, report_dir: str) -> str:
    os.makedirs(report_dir, exist_ok=True)
    path = os.path.join(report_dir, f"{result.module or result.table}_{result.table}_reconciliation.html")
    with open(path, "w") as fh:
        fh.write(render_html(result))
    return path
