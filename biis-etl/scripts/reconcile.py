"""CLI entry point for reconciliation: compares SQL Server output to golden CSVs."""
import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.reconciliation import generate_report, reconcile_module  # noqa: E402
from utils.secrets import load_config  # noqa: E402

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
GOLDEN_DIR = os.path.join(BASE, "tests", "fixtures", "golden")

MODULE_SPECS = {
    "pay_calendar": [
        {
            "table": "PAY_PERIOD",
            "key_columns": ["PP_NUM", "PP_END_YEAR"],
            "golden_file": "pay_calendar_expected_pay_period.csv",
        },
    ],
    "comptime": [
        {
            "table": "COMP_TIME_DAILY_TBL",
            "key_columns": ["SSN", "DAILY_DATE_EARNED"],
            "golden_file": "comptime_expected_comp_time_daily_tbl.csv",
            "ignore_columns": ["LOAD_DATE"],
        },
        {
            "table": "COUNTER_TBL",
            "key_columns": ["PROCESS_NAME", "COUNTER_DESCRIPTION"],
            "golden_file": "comptime_expected_counter_tbl.csv",
            "ignore_columns": ["RUN_DATE"],
        },
    ],
    "pseudossn": [
        {
            "table": "PSEUDOSSN_FROM_SDA_TBL",
            "key_columns": ["PSEUDOSSN", "EFFECTIVE_DATE", "EFFECTIVE_SEQ"],
            "golden_file": "pseudossn_expected_pseudossn_from_sda_tbl.csv",
        },
        {
            "table": "PSEUDOSSN_TBL",
            "key_columns": ["PSEUDOSSN"],
            "golden_file": "pseudossn_expected_pseudossn_tbl.csv",
        },
    ],
    "fda_leave": [
        {
            "table": "ERROR_TBL",
            "key_columns": ["SOURCE_KEY", "ERROR_MESSAGE"],
            "golden_file": "fda_leave_expected_error_tbl.csv",
            "ignore_columns": ["ERROR_DATE"],
        },
    ],
    "ehrp2biis": [
        {
            "table": "ACTION_PRIMARY_ALL",
            "key_columns": ["EVENT_ID"],
            "golden_file": "ehrp2biis_expected_action_primary_all.csv",
            "ignore_columns": ["LOAD_DATE"],
        },
        {
            "table": "ACTION_SECONDARY_ALL",
            "key_columns": ["EVENT_ID"],
            "golden_file": "ehrp2biis_expected_action_secondary_all.csv",
            "ignore_columns": ["LOAD_DATE"],
        },
        {
            "table": "ACTION_REMARKS_ALL",
            "key_columns": ["EVENT_ID", "REMARK_SEQ"],
            "golden_file": "ehrp2biis_expected_action_remarks_all.csv",
            "ignore_columns": ["LOAD_DATE"],
        },
    ],
    "cpm": [
        {
            "table": "CPM_NIH_STG_TBL",
            "key_columns": ["RECORD_NUM"],
            "golden_file": "cpm_nih_expected_staging.csv",
            "ignore_columns": ["LOAD_DATE"],
        },
        {
            "table": "CPM_OIG_STG_TBL",
            "key_columns": ["RECORD_NUM"],
            "golden_file": "cpm_oig_expected_staging.csv",
            "ignore_columns": ["LOAD_DATE"],
        },
        {
            "table": "CPM_CDC_STG_TBL",
            "key_columns": ["RECORD_NUM"],
            "golden_file": "cpm_cdc_expected_staging.csv",
            "ignore_columns": ["LOAD_DATE"],
        },
    ],
}


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--env", default=None)
    parser.add_argument("--modules", default=None,
                        help="Comma-separated module list; default: all")
    parser.add_argument("--report-dir", default="reports/reconciliation/")
    args = parser.parse_args()

    from pyspark.sql import SparkSession

    config = load_config(args.env)
    modules = args.modules.split(",") if args.modules else list(MODULE_SPECS)

    spark = (
        SparkSession.builder.appName("biis-reconcile")
        .config("spark.jars.packages", "com.microsoft.sqlserver:mssql-jdbc:12.4.2.jre11")
        .master("local[*]")
        .getOrCreate()
    )
    results = []
    for module in modules:
        results.extend(
            reconcile_module(spark, config, module, GOLDEN_DIR, MODULE_SPECS[module])
        )
    generate_report(results, args.report_dir)
    failed = [r for r in results if not r.passed]
    for r in results:
        print(f"{r.table}: diffs={r.diff_count} rows={r.actual_count}/{r.expected_count} "
              f"{'PASS' if r.passed else 'FAIL'}")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
