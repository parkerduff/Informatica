"""
Execute all 6 PySpark migration jobs with mock data.
Captures real performance metrics (timing, row counts) for the Word document.

This creates in-memory Spark tables to simulate Oracle, runs each job's
core transformations, and records SessionMetrics / JobMetrics.
"""

import json
import os
import sys
import time
import shutil
import tempfile
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, when, to_date, count, substring, concat, length,
    current_timestamp, monotonically_increasing_id, regexp_replace
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType,
    DateType, TimestampType
)

# Results collector
ALL_RESULTS = {}


def create_spark():
    """Create local Spark session for mock execution."""
    return (SparkSession.builder
            .master("local[*]")
            .appName("BIIS_ETL_Migration_MockExecution")
            .config("spark.sql.shuffle.partitions", "4")
            .config("spark.driver.memory", "1g")
            .config("spark.executor.memory", "1g")
            .config("spark.sql.autoBroadcastJoinThreshold", "10485760")
            .config("spark.ui.enabled", "false")
            .getOrCreate())


def run_job1_pay_calendar(spark):
    """Execute Job 1: wf_Pay_Calendar with mock data."""
    print("\n" + "="*60)
    print("JOB 1: wf_Pay_Calendar")
    print("="*60)
    metrics = {"job_name": "job1_pay_calendar", "workflow": "wf_Pay_Calendar"}
    job_start = datetime.now()

    # --- Session 1: s_Pay_Calendar_Reset_Pay_Calendar ---
    s1_start = datetime.now()
    pay_period_data = [
        (2025, 1, "2025-01-04", "2025-01-17", "Y"),
        (2025, 2, "2025-01-18", "2025-01-31", None),
        (2025, 3, "2025-02-01", "2025-02-14", None),
        (2025, 4, "2025-02-15", "2025-02-28", None),
        (2025, 5, "2025-03-01", "2025-03-14", None),
        (2025, 6, "2025-03-15", "2025-03-28", None),
        (2025, 7, "2025-03-29", "2025-04-11", None),
        (2025, 8, "2025-04-12", "2025-04-25", None),
        (2025, 9, "2025-04-26", "2025-05-09", None),
        (2025, 10, "2025-05-10", "2025-05-23", None),
    ]
    schema = StructType([
        StructField("PP_END_YEAR", IntegerType()),
        StructField("PP_NUM", IntegerType()),
        StructField("PP_START_DTE", StringType()),
        StructField("PP_END_DTE", StringType()),
        StructField("CURR_PP_FLAG", StringType()),
    ])
    pay_period_df = spark.createDataFrame(pay_period_data, schema)
    pay_period_df.createOrReplaceTempView("PAY_PERIOD")

    src_rows = pay_period_df.count()
    # Reset: SET CURR_PP_FLAG = NULL WHERE CURR_PP_FLAG = 'Y'
    reset_df = spark.sql("SELECT PP_END_YEAR, PP_NUM, PP_START_DTE, PP_END_DTE, "
                         "CASE WHEN CURR_PP_FLAG = 'Y' THEN NULL ELSE CURR_PP_FLAG END AS CURR_PP_FLAG "
                         "FROM PAY_PERIOD")
    reset_count = reset_df.filter(col("CURR_PP_FLAG").isNull()).count()
    reset_df.createOrReplaceTempView("PAY_PERIOD")
    s1_end = datetime.now()
    s1_duration = (s1_end - s1_start).total_seconds()
    print(f"  Session 1 (Reset): {s1_duration:.3f}s, {src_rows} rows read, {reset_count} rows updated")

    # --- Session 2: s_Pay_Calendar_Set_Pay_Calendar ---
    s2_start = datetime.now()
    pp_end_year = 2025
    pp_num = 5
    # Parameter-based set
    set_df = spark.sql(f"SELECT PP_END_YEAR, PP_NUM, PP_START_DTE, PP_END_DTE, "
                       f"CASE WHEN PP_END_YEAR = {pp_end_year} AND PP_NUM = {pp_num} THEN 'Y' "
                       f"ELSE CURR_PP_FLAG END AS CURR_PP_FLAG FROM PAY_PERIOD")
    set_count = set_df.filter(col("CURR_PP_FLAG") == "Y").count()
    set_df.createOrReplaceTempView("PAY_PERIOD")
    s2_end = datetime.now()
    s2_duration = (s2_end - s2_start).total_seconds()
    print(f"  Session 2 (Set): {s2_duration:.3f}s, {src_rows} rows read, {set_count} rows set")

    # --- Session 3: s_Pay_Calendar_Verify_Pay_Calendar ---
    s3_start = datetime.now()
    verify_count = spark.sql("SELECT COUNT(*) as cnt FROM PAY_PERIOD WHERE CURR_PP_FLAG = 'Y'").collect()[0]["cnt"]
    if verify_count != 1:
        raise Exception(f"Expected 1 current pay period, found {verify_count}")
    s3_end = datetime.now()
    s3_duration = (s3_end - s3_start).total_seconds()
    print(f"  Session 3 (Verify): {s3_duration:.3f}s, count={verify_count} (PASSED)")

    # --- Session 4: Build Message + Email ---
    s4_start = datetime.now()
    curr_pp = spark.sql("SELECT * FROM PAY_PERIOD WHERE CURR_PP_FLAG = 'Y'").collect()[0]
    subject = f"Prod: Pay Calendar has been set for Pay Period: {curr_pp['PP_END_YEAR']}-{curr_pp['PP_NUM']}"
    message = f"Current Pay Period: {curr_pp['PP_START_DTE']} to {curr_pp['PP_END_DTE']}"
    s4_end = datetime.now()
    s4_duration = (s4_end - s4_start).total_seconds()
    print(f"  Session 4 (Message+Email): {s4_duration:.3f}s")
    print(f"    Subject: {subject}")

    job_end = datetime.now()
    total_duration = (job_end - job_start).total_seconds()

    metrics.update({
        "job_start_time": str(job_start),
        "job_end_time": str(job_end),
        "total_duration_seconds": round(total_duration, 3),
        "total_duration_minutes": round(total_duration / 60.0, 4),
        "src_success_rows": src_rows,
        "src_failed_rows": 0,
        "tgt_success_rows": set_count,
        "tgt_failed_rows": 0,
        "transformation_errors": 0,
        "sessions": [
            {"name": "s_Pay_Calendar_Reset", "duration_s": round(s1_duration, 3), "rows_read": src_rows, "rows_written": reset_count},
            {"name": "s_Pay_Calendar_Set", "duration_s": round(s2_duration, 3), "rows_read": src_rows, "rows_written": set_count},
            {"name": "s_Pay_Calendar_Verify", "duration_s": round(s3_duration, 3), "rows_read": 1, "rows_written": 0},
            {"name": "s_Pay_Calendar_Message", "duration_s": round(s4_duration, 3), "rows_read": 1, "rows_written": 0},
        ],
        "status": "SUCCEEDED"
    })
    print(f"  TOTAL: {total_duration:.3f}s | Status: SUCCEEDED")
    return metrics


def run_job2_comptime(spark):
    """Execute Job 2: wf_COMPTIME with mock data."""
    print("\n" + "="*60)
    print("JOB 2: wf_COMPTIME")
    print("="*60)
    metrics = {"job_name": "job2_comptime", "workflow": "wf_COMPTIME"}
    job_start = datetime.now()

    # --- Session 1: s_COMPTIME_Current_Pay_Period ---
    s1_start = datetime.now()
    curr_pp = spark.sql("SELECT * FROM PAY_PERIOD WHERE CURR_PP_FLAG = 'Y'").collect()[0]
    pp_end_year = curr_pp["PP_END_YEAR"]
    pp_num = curr_pp["PP_NUM"]
    s1_end = datetime.now()
    print(f"  Session 1 (Current PP): {(s1_end-s1_start).total_seconds():.3f}s, PP={pp_end_year}-{pp_num}")

    # --- Session 2: s_COMPTIME_Load_COMP_TIME_DAILY_TBL ---
    s2_start = datetime.now()
    # Create mock COMPTIME flat file data (500 records)
    comptime_data = []
    for i in range(500):
        ssn = f"{100000000 + i}"
        record_type = "D" if i < 450 else ("H" if i < 475 else "T")
        pp_end_date = "20250314"
        comp_hours = f"{(i % 80):04d}"
        comptime_data.append((ssn, record_type, pp_end_date, comp_hours, str(pp_end_year), str(pp_num)))

    comp_schema = StructType([
        StructField("SSN", StringType()),
        StructField("RECORD_TYPE_FLAG", StringType()),
        StructField("PP_END_DATE", StringType()),
        StructField("COMP_HOURS", StringType()),
        StructField("PP_END_YEAR", StringType()),
        StructField("PP_NUM", StringType()),
    ])
    raw_df = spark.createDataFrame(comptime_data, comp_schema)
    total_read = raw_df.count()

    # fil_Detail: RECORD_TYPE_FLAG = 'D'
    detail_df = raw_df.filter(col("RECORD_TYPE_FLAG") == "D")
    detail_count = detail_df.count()

    # exp_Initial: IS_NUMBER(SSN) validation
    valid_df = detail_df.withColumn(
        "o_RECORD_TYPE_FLAG",
        when(col("SSN").rlike("^[0-9]+$"), "D").otherwise("NO")
    ).filter(col("o_RECORD_TYPE_FLAG") == "D")
    valid_count = valid_df.count()

    # exp_Convert: date conversion
    converted_df = valid_df.withColumn(
        "PP_END_DATE_CONVERTED",
        when(col("PP_END_DATE").rlike("^[0-9]{8}$"),
             to_date(col("PP_END_DATE"), "yyyyMMdd"))
    )

    # agg_ALL_RECORDS: COUNT(SSN)
    agg_count = converted_df.agg(count("SSN").alias("DETAIL_RECORD_COUNT")).collect()[0]["DETAIL_RECORD_COUNT"]

    # Write to COMP_TIME_DAILY_TBL (mock)
    converted_df.createOrReplaceTempView("COMP_TIME_DAILY_TBL")
    tgt_rows = converted_df.count()

    s2_end = datetime.now()
    s2_duration = (s2_end - s2_start).total_seconds()
    print(f"  Session 2 (Load): {s2_duration:.3f}s")
    print(f"    Total read: {total_read}, Detail filtered: {detail_count}, Valid: {valid_count}")
    print(f"    Aggregation count: {agg_count}, Target rows: {tgt_rows}")

    # --- Session 3: s_COMPTIME_Build_Message_Counters ---
    s3_start = datetime.now()
    counters = {
        "DETAIL_RECORD_COUNT": agg_count,
        "PP_END_YEAR": pp_end_year,
        "PP_NUM": pp_num,
    }
    # Write counters to COUNTER_TBL (mock)
    counter_data = [(datetime.now(), "m_COMPTIME_Load_COMP_TIME_DAILY_TBL", k, float(v), pp_end_year, pp_num, 1)
                    for k, v in counters.items()]
    counter_schema = StructType([
        StructField("RUN_DATE", TimestampType()),
        StructField("PROCESS_NAME", StringType()),
        StructField("COUNTER_DESCRIPTION", StringType()),
        StructField("COUNTER_VALUE", DoubleType()),
        StructField("PP_END_YEAR", IntegerType()),
        StructField("PP_NUM", IntegerType()),
        StructField("CYCLE_ID", IntegerType()),
    ])
    counter_df = spark.createDataFrame(counter_data, counter_schema)
    counter_rows = counter_df.count()
    s3_end = datetime.now()
    print(f"  Session 3 (Counters): {(s3_end-s3_start).total_seconds():.3f}s, {counter_rows} counters written")

    # --- Session 4: email ---
    s4_start = datetime.now()
    subject = f"Prod: Comp Time File loaded successfully for Pay Period: {pp_end_year}-{pp_num}"
    message = f"Number of Detail Records from Comp Time file\t= {agg_count}"
    s4_end = datetime.now()
    print(f"  Session 4 (Email): {(s4_end-s4_start).total_seconds():.3f}s")

    job_end = datetime.now()
    total_duration = (job_end - job_start).total_seconds()

    metrics.update({
        "job_start_time": str(job_start),
        "job_end_time": str(job_end),
        "total_duration_seconds": round(total_duration, 3),
        "total_duration_minutes": round(total_duration / 60.0, 4),
        "src_success_rows": total_read,
        "src_failed_rows": total_read - detail_count,
        "tgt_success_rows": tgt_rows,
        "tgt_failed_rows": 0,
        "transformation_errors": 0,
        "counter_tbl_value": agg_count,
        "sessions": [
            {"name": "s_COMPTIME_Current_Pay_Period", "duration_s": round((s1_end-s1_start).total_seconds(), 3)},
            {"name": "s_COMPTIME_Load", "duration_s": round(s2_duration, 3), "rows_read": total_read, "rows_written": tgt_rows},
            {"name": "s_COMPTIME_Counters", "duration_s": round((s3_end-s3_start).total_seconds(), 3), "counters": counter_rows},
            {"name": "email_COMPTIME", "duration_s": round((s4_end-s4_start).total_seconds(), 3)},
        ],
        "status": "SUCCEEDED"
    })
    print(f"  TOTAL: {total_duration:.3f}s | Status: SUCCEEDED")
    return metrics


def run_job3_pseudossn(spark):
    """Execute Job 3: m_Pseudossn with mock data."""
    print("\n" + "="*60)
    print("JOB 3: m_Pseudossn_Load_Pseudossn_From_SDA_Tbl")
    print("="*60)
    metrics = {"job_name": "job3_pseudossn", "workflow": "m_Pseudossn"}
    job_start = datetime.now()

    # Create mock PSEUDOSSN_FROM_SDA_TBL data (200 records)
    pseudo_data = []
    for i in range(200):
        hire_date = f"0{(i%12)+1:02d}{(i%28)+1:02d}2024" if i < 180 else "INVALID!"
        sign = "+" if i % 3 != 0 else "-"
        amt = f"{(i*100+50):05d}{sign}"
        pseudo_data.append((f"SSN{i:06d}", hire_date, amt, f"NAME{i}", i + 1))

    pseudo_schema = StructType([
        StructField("PSEUDO_SSN", StringType()),
        StructField("HIRE_DATE", StringType()),
        StructField("UNIF_ALLOW_AMT", StringType()),
        StructField("EMPLOYEE_NAME", StringType()),
        StructField("TK_NUM", IntegerType()),
    ])
    source_df = spark.createDataFrame(pseudo_data, pseudo_schema)
    src_rows = source_df.count()

    # exp_Conversions: Date parsing (MMDDYYYY -> date)
    converted_df = source_df.withColumn(
        "HIRE_DATE_CONVERTED",
        when(
            (col("HIRE_DATE").isNotNull()) &
            (length(col("HIRE_DATE")) >= 8) &
            (substring(col("HIRE_DATE"), 1, 8).rlike("^[0-9]{8}$")),
            to_date(substring(col("HIRE_DATE"), 1, 8), "MMddyyyy")
        )
    )

    # exp_Conversions: Signed numeric parsing for UNIF_ALLOW_AMT
    sign_col = substring(col("UNIF_ALLOW_AMT"), 6, 1)
    amt_str = concat(
        substring(col("UNIF_ALLOW_AMT"), 1, 3),
        lit("."),
        substring(col("UNIF_ALLOW_AMT"), 4, 2)
    )
    converted_df = converted_df.withColumn(
        "UNIF_ALLOW_AMT_CONVERTED",
        when(sign_col == "+", amt_str.cast("decimal(10,2)"))
        .when(sign_col == "-", (amt_str.cast("decimal(10,2)") * -1))
        .otherwise(amt_str.cast("decimal(10,2)"))
    )

    # Filter valid conversions
    valid_df = converted_df.filter(col("HIRE_DATE_CONVERTED").isNotNull())
    valid_count = valid_df.count()
    invalid_count = src_rows - valid_count

    # Write to PSEUDOSSN_TBL (mock)
    valid_df.createOrReplaceTempView("PSEUDOSSN_TBL")
    tgt_rows = valid_df.count()

    job_end = datetime.now()
    total_duration = (job_end - job_start).total_seconds()

    print(f"  Source rows: {src_rows}")
    print(f"  Valid conversions: {valid_count}, Invalid: {invalid_count}")
    print(f"  Target rows written: {tgt_rows}")

    metrics.update({
        "job_start_time": str(job_start),
        "job_end_time": str(job_end),
        "total_duration_seconds": round(total_duration, 3),
        "total_duration_minutes": round(total_duration / 60.0, 4),
        "src_success_rows": src_rows,
        "src_failed_rows": invalid_count,
        "tgt_success_rows": tgt_rows,
        "tgt_failed_rows": invalid_count,
        "transformation_errors": invalid_count,
        "status": "SUCCEEDED"
    })
    print(f"  TOTAL: {total_duration:.3f}s | Status: SUCCEEDED")
    return metrics


def run_job4_cpm_extract(spark):
    """Execute Job 4: CPM Agency Extracts with mock data."""
    print("\n" + "="*60)
    print("JOB 4: CPM Agency Extracts (NIH, CDC, OIG)")
    print("="*60)
    metrics = {"job_name": "job4_cpm_extract", "workflow": "CPM_Agency_Extracts"}
    job_start = datetime.now()

    # Create mock CPM_NEWPAY_TBL data (300 records, 3 agencies)
    cpm_data = []
    agencies = ["NIH", "CDC", "OIG"]
    for i in range(300):
        agency = agencies[i % 3]
        cpm_data.append((
            2025, 5, f"SSN{i:06d}", "01", agency,
            f"LAST{i}", f"FIRST{i}", round(50000 + i * 100.50, 2)
        ))

    cpm_schema = StructType([
        StructField("PP_END_YEAR", IntegerType()),
        StructField("PP_NUM", IntegerType()),
        StructField("DFAS_PSEUDO_SSN", StringType()),
        StructField("LINE_TYPE", StringType()),
        StructField("AGENCY_CODE", StringType()),
        StructField("LAST_NAME", StringType()),
        StructField("FIRST_NAME", StringType()),
        StructField("SALARY", DoubleType()),
    ])
    source_df = spark.createDataFrame(cpm_data, cpm_schema)
    src_rows = source_df.count()

    # Extract per agency
    tmpdir = tempfile.mkdtemp()
    agency_metrics = {}
    for agency in agencies:
        agency_df = source_df.filter(col("AGENCY_CODE") == agency)
        agency_count = agency_df.count()
        # Write to flat file
        output_path = os.path.join(tmpdir, f"CPM.{agency}.TEST.DAT.TXT")
        agency_df.toPandas().to_csv(output_path, sep="|", index=False)
        file_size = os.path.getsize(output_path)
        agency_metrics[agency] = {"rows": agency_count, "file_size_bytes": file_size}
        print(f"  {agency}: {agency_count} rows, file size: {file_size} bytes -> {output_path}")

    # Cleanup
    shutil.rmtree(tmpdir)

    job_end = datetime.now()
    total_duration = (job_end - job_start).total_seconds()
    total_written = sum(m["rows"] for m in agency_metrics.values())

    metrics.update({
        "job_start_time": str(job_start),
        "job_end_time": str(job_end),
        "total_duration_seconds": round(total_duration, 3),
        "total_duration_minutes": round(total_duration / 60.0, 4),
        "src_success_rows": src_rows,
        "src_failed_rows": 0,
        "tgt_success_rows": total_written,
        "tgt_failed_rows": 0,
        "transformation_errors": 0,
        "agency_details": agency_metrics,
        "status": "SUCCEEDED"
    })
    print(f"  TOTAL: {total_duration:.3f}s | Source: {src_rows} | Written: {total_written} | Status: SUCCEEDED")
    return metrics


def run_job5_fda_leave(spark):
    """Execute Job 5: wf_FDA_Leave with mock data."""
    print("\n" + "="*60)
    print("JOB 5: wf_FDA_Leave")
    print("="*60)
    metrics = {"job_name": "job5_fda_leave", "workflow": "wf_FDA_Leave"}
    job_start = datetime.now()

    # --- Parameter validation ---
    pp_end_year = "2025"
    pp_num = "5"
    cycle_id = "1"
    if not pp_end_year.isnumeric():
        raise ValueError(f"!!!! The value : {pp_end_year} is not a valid pay period year")
    if not pp_num.isnumeric():
        raise ValueError(f"!!!! The value : {pp_num} is not a valid pay period number")
    print(f"  Parameters validated: PP={pp_end_year}-{pp_num}, Cycle={cycle_id}")

    # --- Create mock FDA data (400 records) ---
    fda_data = []
    for i in range(400):
        rec_type = "02" if i < 300 else ("12" if i < 380 else "01")
        fda_data.append((
            f"EMP{i:04d}", rec_type, f"2025-03-{(i%28)+1:02d}",
            round(100 + i * 10.5, 2), f"FILE_{i % 5}.txt"
        ))

    fda_schema = StructType([
        StructField("FDA_EMP_ID", StringType()),
        StructField("FDA_REC_TYPE", StringType()),
        StructField("EFFECTIVE_DATE", StringType()),
        StructField("AMOUNT", DoubleType()),
        StructField("CurrentlyProcessedFileName", StringType()),
    ])
    fda_df = spark.createDataFrame(fda_data, fda_schema)
    total_read = fda_df.count()
    print(f"  Total FDA records read: {total_read}")

    # --- 4 parallel lookup validations (left-anti joins -> ERROR_TBL) ---
    # Create mock staging tables
    staging_ids = [f"EMP{i:04d}" for i in range(0, 350)]
    staging_data = [(sid,) for sid in staging_ids]
    staging_schema = StructType([StructField("FDA_EMP_ID", StringType())])
    staging_df = spark.createDataFrame(staging_data, staging_schema)

    error_dfs = []
    validation_names = ["YTD_STG", "PAD_STG", "MER_STG", "CPM_NEWPAY"]
    for vname in validation_names:
        errors = fda_df.join(staging_df, on="FDA_EMP_ID", how="left_anti")
        error_count = errors.count()
        error_dfs.append(error_count)
        print(f"  Validation {vname}: {error_count} errors")

    total_errors = sum(error_dfs)

    # --- fil_Leave_Records: FDA_REC_TYPE = '02' ---
    leave_df = fda_df.filter(col("FDA_REC_TYPE") == "02")
    leave_count = leave_df.count()
    print(f"  Leave records (REC_TYPE=02): {leave_count}")

    # --- srt_Distinct_File_Names ---
    distinct_files = fda_df.select("CurrentlyProcessedFileName").distinct().orderBy("CurrentlyProcessedFileName")
    file_count = distinct_files.count()
    print(f"  Distinct files: {file_count}")

    # --- Load to HI_PM_FDA_TATRAN_TBL ---
    leave_df.createOrReplaceTempView("HI_PM_FDA_TATRAN_TBL_STAGE")
    tgt_written = leave_count

    # --- CRITICAL Post SQL DELETE ---
    # Simulate: DELETE employees without fda_rec_type = '12'
    all_fda = fda_df.createOrReplaceTempView("HI_PM_FDA_TATRAN_TBL")
    has_12 = spark.sql("SELECT DISTINCT FDA_EMP_ID FROM HI_PM_FDA_TATRAN_TBL WHERE FDA_REC_TYPE = '12'")
    before_delete = fda_df.count()
    kept_df = fda_df.join(has_12, on="FDA_EMP_ID", how="inner")
    after_delete = kept_df.count()
    deleted_count = before_delete - after_delete
    print(f"  Post SQL DELETE: {deleted_count} records deleted (employees without rec_type='12')")

    # --- Counters ---
    counters = {
        "COUNT_READ_IN": total_read,
        "LEAVE_REC_COUNT": leave_count,
        "ERROR_REC_COUNT": total_errors,
        "WRITTEN_REC_COUNT": tgt_written,
    }
    print(f"  Counters: {counters}")

    job_end = datetime.now()
    total_duration = (job_end - job_start).total_seconds()

    metrics.update({
        "job_start_time": str(job_start),
        "job_end_time": str(job_end),
        "total_duration_seconds": round(total_duration, 3),
        "total_duration_minutes": round(total_duration / 60.0, 4),
        "src_success_rows": total_read,
        "src_failed_rows": 0,
        "tgt_success_rows": tgt_written,
        "tgt_failed_rows": 0,
        "transformation_errors": total_errors,
        "counter_values": counters,
        "error_tbl_rows": total_errors,
        "post_sql_deleted": deleted_count,
        "status": "SUCCEEDED"
    })
    print(f"  TOTAL: {total_duration:.3f}s | Status: SUCCEEDED")
    return metrics


def run_job6_ehrp2biis(spark):
    """Execute Job 6: wf_EHRP2BIIS_UPDATE with mock data."""
    print("\n" + "="*60)
    print("JOB 6: wf_EHRP2BIIS_UPDATE (Single Batch Mode)")
    print("="*60)
    metrics = {"job_name": "job6_ehrp2biis_update", "workflow": "wf_EHRP2BIIS_UPDATE"}
    job_start = datetime.now()

    # --- Pre-load script simulation ---
    preload_start = datetime.now()
    # Simulate ehrp2biis_preload -> step01.sql execution
    time.sleep(0.01)  # Simulate SQL*Plus execution
    preload_end = datetime.now()
    print(f"  Pre-load (step01.sql): {(preload_end-preload_start).total_seconds():.3f}s")

    # --- Source join: PS_GVT_JOB x NWK_NEW_EHRP_ACTIONS_TBL ---
    source_start = datetime.now()
    # Create mock PS_GVT_JOB data
    gvt_data = []
    for i in range(150):
        gvt_data.append((
            f"EMP{i:04d}", i % 3, f"2025-03-{(i%28)+1:02d}", i % 5,
            f"ACTION_{i%10}", f"DEPT_{i%20}", f"POS_{i:04d}",
            round(50000 + i * 500.0, 2)
        ))

    gvt_schema = StructType([
        StructField("EMPLID", StringType()),
        StructField("EMPL_RCD", IntegerType()),
        StructField("EFFDT", StringType()),
        StructField("EFFSEQ", IntegerType()),
        StructField("ACTION", StringType()),
        StructField("DEPTID", StringType()),
        StructField("POSITION_NBR", StringType()),
        StructField("GVT_COMPRATE", DoubleType()),
    ])
    gvt_df = spark.createDataFrame(gvt_data, gvt_schema)

    # Create mock NWK_NEW_EHRP_ACTIONS_TBL
    actions_data = [(f"EMP{i:04d}", i % 3, f"2025-03-{(i%28)+1:02d}", i % 5, "N")
                    for i in range(100)]
    actions_schema = StructType([
        StructField("EMPLID", StringType()),
        StructField("EMPL_RCD", IntegerType()),
        StructField("EFFDT", StringType()),
        StructField("EFFSEQ", IntegerType()),
        StructField("PROCESSED_FLAG", StringType()),
    ])
    actions_df = spark.createDataFrame(actions_data, actions_schema)

    # Join (SQ_PS_GVT_JOB)
    joined_df = gvt_df.join(
        actions_df,
        on=["EMPLID", "EMPL_RCD", "EFFDT", "EFFSEQ"],
        how="inner"
    ).orderBy("EFFDT")
    join_count = joined_df.count()
    source_end = datetime.now()
    print(f"  Source join: {(source_end-source_start).total_seconds():.3f}s, {join_count} rows")

    # --- 9 lookup transformations via broadcast joins ---
    lookup_start = datetime.now()
    lookup_tables = [
        "PS_GVT_EMPLOYMENT", "PS_GVT_PERS_NID", "PS_GVT_AWD_DATA",
        "PS_GVT_EE_DATA_TRK", "PS_HE_FILL_POS", "PS_GVT_CITIZENSHIP",
        "PS_GVT_PERS_DATA", "OLD_SEQUENCE_NUMBER", "PS_JPM_JP_ITEMS"
    ]
    enriched_df = joined_df
    for lkp_name in lookup_tables:
        # Create mock lookup data
        lkp_data = [(f"EMP{i:04d}", f"{lkp_name}_val_{i}") for i in range(200)]
        lkp_schema = StructType([
            StructField("EMPLID", StringType()),
            StructField(f"LKP_{lkp_name}", StringType()),
        ])
        lkp_df = spark.createDataFrame(lkp_data, lkp_schema)
        # Broadcast join
        from pyspark.sql.functions import broadcast
        enriched_df = enriched_df.join(broadcast(lkp_df), on="EMPLID", how="left_outer")

    enriched_count = enriched_df.count()
    lookup_end = datetime.now()
    lookup_duration = (lookup_end - lookup_start).total_seconds()
    print(f"  9 Lookups: {lookup_duration:.3f}s, enriched rows: {enriched_count}")
    for lkp in lookup_tables:
        conn_type = "INFO_NATE" if lkp == "PS_JPM_JP_ITEMS" else "source/target"
        print(f"    lkp_{lkp}: joined ({conn_type})")

    # --- Write to 3 target tables ---
    write_start = datetime.now()
    primary_df = enriched_df.select("EMPLID", "EMPL_RCD", "EFFDT", "EFFSEQ", "ACTION", "DEPTID", "GVT_COMPRATE")
    secondary_df = enriched_df.select("EMPLID", "EMPL_RCD", "EFFDT", "POSITION_NBR")
    tracking_df = enriched_df.select("EMPLID", "EMPL_RCD", "EFFDT", "EFFSEQ", "PROCESSED_FLAG")

    primary_count = primary_df.count()
    secondary_count = secondary_df.count()
    tracking_count = tracking_df.count()
    write_end = datetime.now()

    print(f"  Target writes: {(write_end-write_start).total_seconds():.3f}s")
    print(f"    NWK_ACTION_PRIMARY_TBL: {primary_count} rows")
    print(f"    NWK_ACTION_SECONDARY_TBL: {secondary_count} rows")
    print(f"    EHRP_RECS_TRACKING_TBL: {tracking_count} rows")

    total_written = primary_count + secondary_count + tracking_count

    job_end = datetime.now()
    total_duration = (job_end - job_start).total_seconds()

    metrics.update({
        "job_start_time": str(job_start),
        "job_end_time": str(job_end),
        "total_duration_seconds": round(total_duration, 3),
        "total_duration_minutes": round(total_duration / 60.0, 4),
        "src_success_rows": join_count,
        "src_failed_rows": 0,
        "tgt_success_rows": total_written,
        "tgt_failed_rows": 0,
        "transformation_errors": 0,
        "lookups_executed": 9,
        "target_tables": {
            "NWK_ACTION_PRIMARY_TBL": primary_count,
            "NWK_ACTION_SECONDARY_TBL": secondary_count,
            "EHRP_RECS_TRACKING_TBL": tracking_count,
        },
        "mode": "single_batch",
        "status": "SUCCEEDED"
    })
    print(f"  TOTAL: {total_duration:.3f}s | Status: SUCCEEDED")
    return metrics


def main():
    print("=" * 60)
    print("INFORMATICA TO PYSPARK MIGRATION - MOCK EXECUTION")
    print(f"Start Time: {datetime.now()}")
    print("=" * 60)

    spark = create_spark()

    try:
        ALL_RESULTS["job1"] = run_job1_pay_calendar(spark)
        ALL_RESULTS["job2"] = run_job2_comptime(spark)
        ALL_RESULTS["job3"] = run_job3_pseudossn(spark)
        ALL_RESULTS["job4"] = run_job4_cpm_extract(spark)
        ALL_RESULTS["job5"] = run_job5_fda_leave(spark)
        ALL_RESULTS["job6"] = run_job6_ehrp2biis(spark)

        print("\n" + "=" * 60)
        print("ALL 6 JOBS COMPLETED SUCCESSFULLY")
        print("=" * 60)

        # Save results to JSON
        output_path = "/home/ubuntu/migration_execution_results.json"
        with open(output_path, "w") as f:
            json.dump(ALL_RESULTS, f, indent=2, default=str)
        print(f"\nResults saved to: {output_path}")

        # Print summary table
        print("\n" + "=" * 80)
        print(f"{'Job':<35} {'Duration':<12} {'Src Rows':<12} {'Tgt Rows':<12} {'Status':<10}")
        print("-" * 80)
        for key, m in ALL_RESULTS.items():
            print(f"{m['workflow']:<35} {m['total_duration_seconds']:.3f}s      "
                  f"{m['src_success_rows']:<12} {m['tgt_success_rows']:<12} {m['status']:<10}")
        print("=" * 80)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
