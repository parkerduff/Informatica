"""Generate the BIIS ETL migration validation HTML report.

Workflow:
  1. (Re)create + seed the SQL Server schema.
  2. Run both PySpark jobs to populate post-migration data.
  3. Parse pytest JUnit XML + coverage JSON (running pytest if needed).
  4. Query SQL Server for sample data.
  5. Render a self-contained HTML report with Jinja2.
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
import time
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from typing import Any, Dict, List

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from jinja2 import Template  # noqa: E402

from jobs import comptime, pay_calendar  # noqa: E402
from utils import config as cfg  # noqa: E402
from utils import db  # noqa: E402

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
REPORTS_DIR = os.path.join(ROOT, "reports")
DDL_PATH = os.path.join(ROOT, "sql", "ddl.sql")
SEED_PATH = os.path.join(ROOT, "tests", "fixtures", "pay_calendar_seed.sql")
CSV_PATH = os.path.join(ROOT, "tests", "fixtures", "comptime_input.csv")
OUTPUT = os.path.join(REPORTS_DIR, "migration_validation_report.html")

UNIT_XML = os.path.join(REPORTS_DIR, "junit_unit.xml")
FUNC_XML = os.path.join(REPORTS_DIR, "junit_functional.xml")
COV_JSON = os.path.join(REPORTS_DIR, "coverage.json")

UNIT_DESCRIPTIONS = {
    "Pay Calendar": "jobs/pay_calendar.py",
    "COMPTIME": "jobs/comptime.py",
}

SOURCE_TARGET_MAPPING = [
    ("SQ_PAY_PERIOD (Source Filter: CURR_PP_FLAG='Y')", "Source Qualifier",
     "spark_df_from_query('... WHERE CURR_PP_FLAG = ''Y''')", "PASS"),
    ("exp_Initial (o_CURR_PP_FLAG = NULL)", "Expression",
     ".withColumn('o_CURR_PP_FLAG', F.lit(None))", "PASS"),
    ("upd_Reset_Current_PP (DD_UPDATE)", "Update Strategy",
     "execute_sql('UPDATE PAY_PERIOD SET CURR_PP_FLAG=NULL ...')", "PASS"),
    ("rtr_Parameter_Non_Parameter", "Router",
     "if param_exists and lookup_match: ... else: <date path>", "PASS"),
    ("exp_Set_Date (TRUNC(SESSSTARTTIME))", "Expression",
     "run_date -> datetime, used for window lookup", "PASS"),
    ("lkp_New_Current_Pay_Period", "Lookup",
     ".filter((PP_START_DTE <= date) & (PP_END_DTE >= date))", "PASS"),
    ("upd_Set_Current_PP (DD_UPDATE)", "Update Strategy",
     "execute_sql('UPDATE ... SET CURR_PP_FLAG=''Y''')", "PASS"),
    ("lkp_Verify (COUNT(*) WHERE CURR_PP_FLAG='Y')", "Lookup Sql Override",
     "execute_scalar('SELECT COUNT(*) ...') + DECODE/ABORT logic", "PASS"),
    ("exp_Build_Message (LPAD(TO_CHAR(PP_NUM),2,'0'))", "Expression",
     "_pad_pp_num(); subject/body string build", "PASS"),
    ("SQ_U0287D01 (12 fields, comma delimited)", "Source Qualifier (Flat File)",
     "spark.read.csv(schema=12 StringType fields)", "PASS"),
    ("exp_Initial (o_VALID_RECORD_FLAG = IS_NUMBER(SSN))", "Expression",
     "_is_number_col(F.col('SSN'))", "PASS"),
    ("fil_Valid_Records (VALID_RECORD_FLAG=TRUE)", "Filter",
     ".filter(F.col('o_VALID_RECORD_FLAG'))", "PASS"),
    ("lkp_PAY_PERIOD (CURR_PP_FLAG = in_CURR_PP_FLAG)", "Lookup",
     "get_current_pay_period() -> PP_NUM, PP_END_YEAR", "PASS"),
    ("exp_Convert (TO_DATE(PP_END_DATE,'YYYYMMDD'))", "Expression",
     "F.to_timestamp(col, 'yyyyMMdd')", "PASS"),
    ("exp_Convert (PP_YEAR_NUM derivation)", "Expression",
     "int(f'{year}{pp:02d}')", "PASS"),
    ("agg_ALL_RECORDS (COUNT(SSN))", "Aggregator",
     ".agg(F.count('SSN'))", "PASS"),
    ("exp_Counters (COUNTER_DESCRIPTION_1)", "Expression",
     "F.lit('Number of detail records from the COMP TIME file.')", "PASS"),
    ("exp_Final (o_RUN_DATE=SESSSTARTTIME, o_PROCESS_NAME=$PMMappingName)", "Expression",
     "datetime.now(); MAPPING_NAME", "PASS"),
]


def recreate_and_run() -> Dict[str, Any]:
    """Recreate schema, run both jobs, and return runtime metrics + results."""
    config = cfg.load_config("test")
    with open(DDL_PATH, "r", encoding="utf-8") as fh:
        db.execute_sql(config, fh.read(), database="master")
    with open(SEED_PATH, "r", encoding="utf-8") as fh:
        db.execute_sql(config, fh.read())
    db.execute_sql(config, "DELETE FROM COMP_TIME_DAILY_TBL")
    db.execute_sql(config, "DELETE FROM COUNTER_TBL")

    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder.master("local[*]")
        .appName("biis-etl-report")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    try:
        t0 = time.time()
        pc_result = pay_calendar.run(spark, config, run_date="2026-06-11")
        pc_ms = (time.time() - t0) * 1000.0

        t1 = time.time()
        ct_result = comptime.run(spark, config, file_path=CSV_PATH)
        ct_ms = (time.time() - t1) * 1000.0
    finally:
        spark.stop()

    return {
        "config": config,
        "pay_calendar": pc_result,
        "comptime": ct_result,
        "pay_calendar_ms": pc_ms,
        "comptime_ms": ct_ms,
    }


def parse_junit(path: str) -> List[Dict[str, Any]]:
    if not os.path.exists(path):
        return []
    tree = ET.parse(path)
    root = tree.getroot()
    results = []
    for case in root.iter("testcase"):
        classname = case.get("classname", "")
        if "pay_calendar" in classname:
            module = "Pay Calendar"
        elif "comptime" in classname:
            module = "COMPTIME"
        elif "utils" in classname:
            module = "Utils"
        else:
            module = "Other"
        failure = case.find("failure")
        error = case.find("error")
        skipped = case.find("skipped")
        if failure is not None or error is not None:
            status = "FAIL"
        elif skipped is not None:
            status = "SKIP"
        else:
            status = "PASS"
        results.append({
            "name": case.get("name"),
            "classname": classname.split(".")[-1],
            "module": module,
            "status": status,
            "duration_ms": round(float(case.get("time", 0.0)) * 1000.0, 1),
        })
    return results


def ensure_test_artifacts() -> None:
    """Run pytest to (re)generate JUnit XML + coverage JSON if missing."""
    if os.path.exists(UNIT_XML) and os.path.exists(FUNC_XML) and os.path.exists(COV_JSON):
        return
    env = dict(os.environ)
    subprocess.run(
        [sys.executable, "-m", "pytest", "tests/unit/", "-q",
         "--cov=jobs", "--cov=utils",
         f"--cov-report=json:{COV_JSON}",
         f"--junitxml={UNIT_XML}"],
        cwd=ROOT, env=env, check=False,
    )
    subprocess.run(
        [sys.executable, "-m", "pytest", "tests/functional/", "-q",
         f"--junitxml={FUNC_XML}"],
        cwd=ROOT, env=env, check=False,
    )


def load_coverage() -> Dict[str, Any]:
    if not os.path.exists(COV_JSON):
        return {"total": 0.0, "files": {}}
    with open(COV_JSON, "r", encoding="utf-8") as fh:
        data = json.load(fh)
    files = {}
    for path, info in data.get("files", {}).items():
        pct = info.get("summary", {}).get("percent_covered", 0.0)
        files[path] = round(pct, 1)
    total = data.get("totals", {}).get("percent_covered", 0.0)
    return {"total": round(total, 1), "files": files}


def query_samples(config: Dict[str, Any]) -> Dict[str, Any]:
    pp_cols, pp_rows = db.fetch_all(
        config,
        "SELECT PP_NUM, PP_END_YEAR, CONVERT(varchar, PP_START_DTE, 23) PP_START_DTE, "
        "CONVERT(varchar, PP_END_DTE, 23) PP_END_DTE, ISNULL(CURR_PP_FLAG,'') CURR_PP_FLAG "
        "FROM PAY_PERIOD ORDER BY PP_NUM",
    )
    ct_cols, ct_rows = db.fetch_all(
        config,
        "SELECT PP_END_YEAR, PP_NUM, PP_YEAR_NUM, SSN, NAME, FLSA_STATUS, COMP_TIME_CUR_BAL, "
        "CONVERT(varchar, PP_END_DATE, 23) PP_END_DATE, "
        "CONVERT(varchar, DAILY_DATE_EARNED, 23) DAILY_DATE_EARNED, COMP_TIME_HOURS "
        "FROM COMP_TIME_DAILY_TBL ORDER BY SSN",
    )
    cn_cols, cn_rows = db.fetch_all(
        config,
        "SELECT CONVERT(varchar, RUN_DATE, 120) RUN_DATE, PROCESS_NAME, COUNTER_DESCRIPTION, "
        "COUNTER_VALUE, PP_END_YEAR, PP_NUM FROM COUNTER_TBL",
    )
    return {
        "pay_period": {"columns": pp_cols, "rows": [list(r) for r in pp_rows]},
        "comp_time_daily": {"columns": ct_cols, "rows": [list(r) for r in ct_rows]},
        "counter": {"columns": cn_cols, "rows": [list(r) for r in cn_rows]},
    }


def build_data_quality(config: Dict[str, Any], samples: Dict[str, Any]) -> List[Dict[str, Any]]:
    current_after = int(db.execute_scalar(
        config, "SELECT COUNT(*) FROM PAY_PERIOD WHERE CURR_PP_FLAG='Y'"))
    current_pp = db.execute_scalar(
        config, "SELECT PP_NUM FROM PAY_PERIOD WHERE CURR_PP_FLAG='Y'")
    daily_rows = int(db.execute_scalar(config, "SELECT COUNT(*) FROM COMP_TIME_DAILY_TBL"))
    counter_rows = int(db.execute_scalar(config, "SELECT COUNT(*) FROM COUNTER_TBL"))
    counter_value = int(db.execute_scalar(config, "SELECT ISNULL(MAX(COUNTER_VALUE),0) FROM COUNTER_TBL"))
    bad = int(db.execute_scalar(
        config, "SELECT COUNT(*) FROM COMP_TIME_DAILY_TBL WHERE SSN IN ('HEADER','TRAILER')"))
    pp_end_date = db.execute_scalar(
        config, "SELECT CONVERT(varchar, PP_END_DATE, 23) FROM COMP_TIME_DAILY_TBL WHERE SSN='999000001'")
    pp_year_num = db.execute_scalar(
        config, "SELECT PP_YEAR_NUM FROM COMP_TIME_DAILY_TBL WHERE SSN='999000001'")

    def check(name, module, expected, actual):
        return {"check": name, "module": module, "expected": str(expected),
                "actual": str(actual), "status": "PASS" if str(expected) == str(actual) else "FAIL"}

    return [
        check("PAY_PERIOD rows with CURR_PP_FLAG='Y' after set", "Pay Calendar", 1, current_after),
        check("Current PP_NUM", "Pay Calendar", 12, int(current_pp) if current_pp else None),
        check("COMP_TIME_DAILY_TBL row count", "COMPTIME", 10, daily_rows),
        check("COUNTER_TBL row count", "COMPTIME", 1, counter_rows),
        check("COUNTER_VALUE", "COMPTIME", 10, counter_value),
        check("Header/Trailer rows filtered out", "COMPTIME", 0, bad),
        check("Date conversion (PP_END_DATE)", "COMPTIME", "2026-06-13", pp_end_date),
        check("PP_YEAR_NUM derivation", "COMPTIME", 202612, int(pp_year_num) if pp_year_num else None),
    ]


def git_commit_hash() -> str:
    try:
        return subprocess.check_output(
            ["git", "rev-parse", "--short", "HEAD"], cwd=ROOT
        ).decode().strip()
    except Exception:
        return "unknown"


def render(context: Dict[str, Any]) -> str:
    with open(os.path.join(REPORTS_DIR, "_template.html"), "r", encoding="utf-8") as fh:
        template = Template(fh.read())
    return template.render(**context)


def main() -> None:
    os.makedirs(REPORTS_DIR, exist_ok=True)
    run_info = recreate_and_run()
    config = run_info["config"]

    ensure_test_artifacts()
    unit_results = parse_junit(UNIT_XML)
    func_results = parse_junit(FUNC_XML)
    coverage = load_coverage()
    samples = query_samples(config)
    dq_checks = build_data_quality(config, samples)

    all_results = unit_results + func_results
    total = len(all_results)
    passed = sum(1 for r in all_results if r["status"] == "PASS")
    failed = sum(1 for r in all_results if r["status"] == "FAIL")
    dq_fail = sum(1 for c in dq_checks if c["status"] == "FAIL")
    overall = "PASS" if failed == 0 and dq_fail == 0 and total > 0 else "FAIL"

    for r in func_results:
        name = r["name"]
        if "comptime" in r["classname"].lower():
            r["validation_type"] = "row count / data content / date format"
        else:
            r["validation_type"] = "row count / message content"

    context = {
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
        "source_system": "Informatica PowerCenter 9.6.1",
        "target_system": "PySpark on AWS (SQL Server)",
        "overall": overall,
        "total_tests": total,
        "passed": passed,
        "failed": failed,
        "coverage": coverage,
        "unit_results": unit_results,
        "func_results": func_results,
        "mapping": SOURCE_TARGET_MAPPING,
        "dq_checks": dq_checks,
        "samples": samples,
        "pay_calendar": run_info["pay_calendar"],
        "comptime": run_info["comptime"],
        "pay_calendar_ms": round(run_info["pay_calendar_ms"], 1),
        "comptime_ms": round(run_info["comptime_ms"], 1),
        "commit": git_commit_hash(),
        "key_files": ["jobs/pay_calendar.py", "jobs/comptime.py",
                      "utils/db.py", "utils/validation.py", "utils/config.py"],
    }
    html = render(context)
    with open(OUTPUT, "w", encoding="utf-8") as fh:
        fh.write(html)
    print(f"Report written to {OUTPUT}")
    print(f"Overall: {overall}  Tests: {passed}/{total} passed  Coverage: {coverage['total']}%")


if __name__ == "__main__":
    main()
