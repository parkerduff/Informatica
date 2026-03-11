# Informatica-to-PySpark Migration Guide

## HHS BIISINT ETL Workflows

**Prepared**: March 2026
**Source System**: Informatica PowerCenter 9.6.1
**Target Platform**: PySpark (standalone, local mode for testing)
**Repository**: https://github.com/parkerduff/Informatica

---

## 1. Executive Summary

This migration converts 6 Informatica PowerCenter ETL workflows used by the U.S. Department of Health and Human Services (HHS) into PySpark jobs. The migration preserves all business logic while adding performance monitoring, fault tolerance, deterministic lookup joins, and data quality validation across all 7 DAMA dimensions.

### Migration Order (Complexity-Ranked)

| Job # | Workflow Name | Complexity | Sessions | Lookups | Targets | Status |
|-------|--------------|------------|----------|---------|---------|--------|
| 1 | wf_Pay_Calendar | Low | 4 | 2 | 3 | Migrated |
| 2 | wf_COMPTIME | Low-Med | 3 | 1 | 4 | Migrated |
| 3 | wf_Pseudossn | Medium | 10 | 12 | 9 | Migrated |
| 4 | wf_FDA_Leave | Med-High | 10 | 7+ | 9 | Migrated |
| 5 | wf_CPM_NIH / wf_CPM_CDC | High | 6+6 | 5+5 | 6+6 | Migrated |
| 6 | wf_EHRP2BIIS_UPDATE | Very High | 1 | 9 | 3 | Migrated |

### Key Improvements Over Informatica

| Area | Informatica (Before) | PySpark (After) |
|------|---------------------|-----------------|
| Performance Monitoring | Disabled in ALL 40 sessions | SessionMetrics on every session method |
| Fault Tolerance | Rollback disabled everywhere | JDBC transaction rollback, configurable error thresholds |
| Lookup Determinism | "Use Any Value" (non-deterministic) | Deterministic joins with `row_number()` by EFFDT DESC |
| Error Logging | Partial (only some jobs) | ERROR_TBL writes for all jobs |
| Data Types | Float for financials | `DecimalType` for all financial fields |
| Credentials | Hardcoded in scripts | Environment variables / secrets manager |
| Recovery | No checkpointing | Spark checkpointing for RUNFOREVER jobs |

---

## 2. Pre-Migration Baselining Checklist

Before cutover to PySpark, capture the following from the running Informatica environment:

- [ ] **Informatica Performance Data**: Enable "Collect performance data" on all sessions and run one full cycle; capture session logs
- [ ] **Counter/Error Snapshots**: Query `COUNTER_TBL` and `ERROR_TBL` for the most recent 3 pay periods
- [ ] **Target Table Row Counts**: Record `COUNT(*)` for all target tables referenced by each workflow
- [ ] **Pay Period State**: Record current `PAY_PERIOD` where `CURR_PP_FLAG = 'Y'`
- [ ] **EHRP Tracking State**: Record `ehrp_recs_tracking_tbl` counts by `PROCESSED_FLAG`
- [ ] **Flat File Checksums**: MD5 checksums of source files (U0287D01, SDA files, CPM VSAM files)
- [ ] **Execution Schedule**: Document cron/scheduler entries for each workflow
- [ ] **Credential Inventory**: Document all connection objects (`INFO_TARGET`, `$Source`, `INFO_NATE`) and their mapped credentials

---

## 3. Project Structure

```
pyspark_migration/
+-- __init__.py
+-- requirements.txt          # pyspark, oracledb, python-dotenv, pytest, pytest-cov
+-- Dockerfile                # Python 3.11 + Java 11 + PySpark
+-- docker-compose.yml        # Oracle XE + PySpark migration service
+-- common/
|   +-- __init__.py
|   +-- config.py             # Dataclass configs (Oracle, Spark, Email, Path)
|   +-- db_manager.py         # JDBC read/write with transaction management
|   +-- spark_session.py      # SparkSession factory with JDBC driver
|   +-- email_service.py      # Email notifications with env prefix
|   +-- counter_error.py      # COUNTER_TBL / ERROR_TBL management
|   +-- logging_utils.py      # SessionMetrics + JobMetrics tracking
+-- jobs/
|   +-- __init__.py
|   +-- job1_pay_calendar.py  # wf_Pay_Calendar (4 sessions)
|   +-- job2_comptime.py      # wf_COMPTIME (3 sessions)
|   +-- job3_pseudossn.py     # wf_Pseudossn (10 sessions)
|   +-- job4_fda_leave.py     # wf_FDA_Leave (10 sessions)
|   +-- job5_cpm.py           # wf_CPM_NIH + wf_CPM_CDC (12 sessions)
|   +-- job6_ehrp2biis.py     # wf_EHRP2BIIS_UPDATE (RUNFOREVER)
+-- scripts/
|   +-- run_job.py            # CLI entry point for individual jobs
|   +-- run_all_jobs.py       # Execute all jobs end-to-end
|   +-- setup_oracle_schema.py # Creates tables + loads synthetic data
+-- tests/
|   +-- conftest.py           # Shared fixtures (SparkSession, mocks)
|   +-- test_common_utilities.py
|   +-- test_job{1-6}_*.py    # Per-job unit tests
|   +-- test_job{2,3,6}_*_coverage.py # Extended coverage tests
|   +-- test_enhanced_unit.py # Cross-job unit tests
|   +-- test_data_quality.py  # 7 DAMA dimension validation (35 tests)
|   +-- test_integration.py   # End-to-end against Oracle XE (18 tests)
|   +-- test_performance.py   # Throughput with 10K-100K rows (13 tests)
+-- docs/
    +-- MIGRATION_GUIDE.md    # This document
    +-- performance_results.json # Captured performance metrics
```

---

## 4. Common Utilities

### 4.1 config.py - Configuration Management

Replaces Informatica parameter files (`BIIS_parms.iparms`) and connection objects.

| Informatica Artifact | PySpark Equivalent |
|---------------------|--------------------|
| `INFO_TARGET` connection | `OracleConnectionConfig` dataclass |
| `INFO_NATE` connection | Second `OracleConnectionConfig` instance |
| `$$PP_END_YEAR`, `$$PP_NUM` | `MigrationConfig.pp_end_year`, `.pp_num` |
| `$PMRepositoryServiceName` | `MigrationConfig.environment` |
| `/home/sa-biisint/.use1`, `.pw1` | Environment variables (`ORACLE_USERNAME`, `ORACLE_PASSWORD`) |

### 4.2 db_manager.py - Database Manager

Replaces Informatica session-level JDBC operations.

- `read_jdbc(table_or_query)` - Reads via Spark JDBC with pushdown predicates
- `write_jdbc(df, table, mode)` - Writes with batch commit and error handling
- `execute_sql(statement)` - Single statement with rollback on error
- `execute_sql_file(path)` - Multi-statement file execution with per-statement error tracking

**Key improvement**: Does NOT call `df.count()` after reads (avoids double full-table scan).

### 4.3 logging_utils.py - Metrics Tracking

Replaces Informatica's (disabled) performance data collection.

- `SessionMetrics` - Per-session: start/end time, duration, src/tgt row counts, errors, throughput
- `JobMetrics` - Per-workflow: aggregated metrics, session list, overall status

### 4.4 counter_error.py - Counter & Error Tables

Replaces Informatica's `COUNTER_TBL` / `ERROR_TBL` write patterns.

- `write_counter(process_name, description, value, pp_end_year, pp_num, cycle_id)`
- `write_error(process_name, message, source_key, pp_end_year, pp_num, cycle_id)`

### 4.5 email_service.py - Notifications

Replaces Informatica email tasks with environment-aware prefix.

- Auto-detects Dev/Test/Prod from `MigrationConfig.environment`
- Subject format: `[DEV] wf_Pay_Calendar - SUCCEEDED`

---

## 5. Job-by-Job Migration Details

### 5.1 Job 1: Pay Calendar (`wf_Pay_Calendar`)

**Informatica Workflow**: 4 sessions in sequence
1. `s_Pay_Calendar_Reset_Pay_Calendar` - UPDATE PAY_PERIOD SET CURR_PP_FLAG='N'
2. `s_Pay_Calendar_Set_Pay_Calendar` - UPDATE PAY_PERIOD SET CURR_PP_FLAG='Y' WHERE matching params
3. `s_Pay_Calendar_Verify_Pay_Calendar` - SELECT COUNT(*) WHERE CURR_PP_FLAG='Y' (must = 1)
4. `s_Pay_Calendar_Build_Message` - Build email with pay period details

**PySpark Implementation**: `PayCalendarJob` class
- `run(pp_end_year, pp_num)` orchestrates all 4 sessions
- Parameter validation: IS_NUMBER check on pp_end_year/pp_num, ABORT on invalid
- Session metrics captured for each step
- Email sent on success and failure

**Transformation Mapping**:

| Informatica | PySpark |
|------------|---------|
| Pre-Session SQL (RESET) | `db_manager.execute_sql("UPDATE PAY_PERIOD SET CURR_PP_FLAG='N'")` |
| Pre-Session SQL (SET) | `db_manager.execute_sql("UPDATE PAY_PERIOD SET CURR_PP_FLAG='Y' WHERE ...")` |
| SQ verification query | `db_manager.read_jdbc("(SELECT COUNT(*) ...)")` |
| Expression: IIF(count!=1, ABORT) | `if count != 1: raise RuntimeError(...)` |
| Email task | `email_service.send(subject, body, recipients)` |

### 5.2 Job 2: Compensatory Time (`wf_COMPTIME`)

**Informatica Workflow**: 3 sessions
1. `s_COMPTIME_Current_Pay_Period` - Get current PP from PAY_PERIOD
2. `s_COMPTIME_Load_Comp_Time_Daily` - Read U0287D01 flat file, load to COMP_TIME_DAILY_TBL
3. `s_COMPTIME_Build_Message_Counters` - Write counters and build email

**PySpark Implementation**: `CompTimeJob` class
- Reads headerless flat file with explicit StructType schema
- Pay period lookup with CURR_PP_FLAG='Y' validation
- ABORT if multiple pay periods found
- Counter writes for rows read/written

**Transformation Mapping**:

| Informatica | PySpark |
|------------|---------|
| Flat File source (headerless) | `spark.read.csv(schema=comptime_schema, header=False)` |
| SQ with PAY_PERIOD join | `db_manager.read_jdbc()` with pushdown query |
| Expression: LPAD(PP_NUM,2,'0') | `F.lpad(F.col("PP_NUM"), 2, "0")` |
| Filter: RECORD_TYPE_FLAG='D' | `df.filter(F.col("RECORD_TYPE_FLAG") == "D")` |
| Target: COMP_TIME_DAILY_TBL | `db_manager.write_jdbc(df, "COMP_TIME_DAILY_TBL")` |
| COUNTER_TBL writes | `counter_manager.write_counter(...)` |

### 5.3 Job 3: Pseudo-SSN (`wf_Pseudossn`)

**Informatica Workflow**: 10 sessions (most complex sequencing)
1. Current Pay Period lookup
2. Verify header date
3. Verify record count
4. Load PSEUDOSSN_TBL from staging
5. Load archive (HI_ARCH_PSEUDOSSN_TBL)
6. Verify SDA header date
7. Load from SDA
8. Load SDA records
9. Update timekeeper number
10. Write counters and email

**PySpark Implementation**: `PseudossnJob` class
- Deterministic dedup: `row_number()` over `Window.partitionBy("PSEUDO_SSN").orderBy(F.desc("EFFDT"))`
- Record count reconciliation: source read = target written + errors
- Archive load with date-based partitioning
- SDA file processing with header validation

**Transformation Mapping**:

| Informatica | PySpark |
|------------|---------|
| Lookup: lkp_PAY_PERIOD (Use Any Value) | Deterministic join with `row_number()` |
| Sorter: SRT_PSEUDOSSN_EFF_DT | `df.orderBy("PSEUDO_SSN", F.desc("PSEUDOSSN_EFF_DT"))` |
| Aggregator: AGG_DEDUP | `Window.partitionBy().orderBy()` + `row_number() == 1` |
| Normalizer/Joiner | `F.explode()` + `df.join()` |
| Update Strategy DD_INSERT | `db_manager.write_jdbc(df, table, mode="append")` |

### 5.4 Job 4: FDA Leave Validation (`wf_FDA_Leave`)

**Informatica Workflow**: 10 sessions with cross-table lookups
- Validates FDA transactions against CPM staging tables
- 7+ lookup transformations checking YTD, PAD, MER, NEWPAY tables
- Post-SQL DELETE of employees without `fda_rec_type='12'`
- Error logging for every failed validation

**PySpark Implementation**: `FDALeaveJob` class
- Cross-table consistency checks via broadcast joins
- Error routing to ERROR_TBL for failed lookups
- Post-session DELETE via `db_manager.execute_sql()`
- Full row count reconciliation

**Transformation Mapping**:

| Informatica | PySpark |
|------------|---------|
| Lookup: lkp_CPM_YTD_DETAIL_STG_TBL | `df.join(F.broadcast(ytd_df), keys, "left")` |
| Router (valid/invalid) | `valid = df.filter(F.col("lookup_key").isNotNull())` |
| Post-SQL DELETE | `db_manager.execute_sql("DELETE FROM ... WHERE ...")` |
| ERROR_TBL writes | `counter_manager.write_error(...)` |

### 5.5 Job 5: CPM Agency Extracts (`wf_CPM_NIH`, `wf_CPM_CDC`)

**Informatica Workflow**: 6+6 sessions (NIH and CDC variants)
- 501-field source from CPM_NEWPAY_TBL
- Agency-specific filtering (NIH: agency codes, CDC: change detection)
- Financial field formatting with DecimalType
- PowerExchange targets for file distribution

**PySpark Implementation**: `CPMJob` class (parameterized for NIH/CDC)
- `DecimalType(15, 2)` for all financial fields (was float in Informatica)
- Agency filter: `df.filter(F.col("AGENCY_CODE").isin(nih_codes))`
- Aggregation: `df.groupBy("AGENCY_CODE").agg(F.sum(...), F.count(...))`

**Transformation Mapping**:

| Informatica | PySpark |
|------------|---------|
| Expression: DECODE(AGENCY_CODE, ...) | `F.when(F.col("AGENCY_CODE") == ..., ...)` |
| Filter: AGENCY_CODE IN (...) | `df.filter(F.col("AGENCY_CODE").isin([...]))` |
| Expression: format financial | `F.format_number(F.col("amount"), 2)` with DecimalType |
| PowerExchange target | `db_manager.write_jdbc()` or file output |

### 5.6 Job 6: EHRP2BIIS Update (`wf_EHRP2BIIS_UPDATE`)

**Informatica Workflow**: RUNFOREVER, 1 mapping with 9 lookups
- 246-field source from PS_GVT_JOB joined with NWK_NEW_EHRP_ACTIONS_TBL
- Cross-database lookups via INFO_NATE connection
- Pre-load KSH script, post-load SQL with stored procedures
- Multi-target writes: NWK_ACTION_PRIMARY_TBL, NWK_ACTION_SECONDARY_TBL, EHRP_RECS_TRACKING_TBL

**PySpark Implementation**: `EHRP2BIISJob` class
- `run_forever()` with configurable polling interval and signal handling (SIGTERM/SIGINT)
- Memory monitoring via `/proc/self/status`
- Cross-DB lookups with separate `OracleConnectionConfig`
- Pre-load: DELETE from tracking table
- Post-load: Stored procedures via `db_manager.execute_sql()`
- Graceful shutdown: completes current iteration before stopping

**Transformation Mapping**:

| Informatica | PySpark |
|------------|---------|
| RUNFOREVER schedule | `while not shutdown_flag: run(); sleep(interval)` |
| Signal handling | `signal.signal(signal.SIGTERM, handler)` |
| Pre-load KSH script | `_execute_preload()` with DELETE statements |
| 9 lookup transformations | `_execute_lookups()` with broadcast joins |
| Stored procedures (HISTDBA) | `db_manager.execute_sql("CALL ...")` |
| Multi-target router | `_write_to_targets()` with per-target DataFrames |
| Post-load afterload.sql | `_execute_afterload()` with SQL file execution |

---

## 6. Performance Comparison Templates

### Job 1: Pay Calendar

| Metric | Informatica (Before) | PySpark (After) |
|--------|---------------------|-----------------|
| Total Duration | ___ sec | ___ sec |
| Source Rows Read | ___ | ___ |
| Target Rows Written | ___ | ___ |
| Errors | ___ | ___ |
| Memory (Peak) | ___ MB | ___ MB |
| Shuffle Partitions | N/A | 4 |

### Job 2: COMPTIME

| Metric | Informatica (Before) | PySpark (After) |
|--------|---------------------|-----------------|
| Total Duration | ___ sec | ___ sec |
| Source Rows Read | ___ | ___ |
| Target Rows Written | ___ | ___ |
| Errors | ___ | ___ |
| Memory (Peak) | ___ MB | ___ MB |
| Shuffle Partitions | N/A | 4 |

### Job 3: Pseudossn

| Metric | Informatica (Before) | PySpark (After) |
|--------|---------------------|-----------------|
| Total Duration | ___ sec | ___ sec |
| Source Rows Read | ___ | ___ |
| Target Rows Written | ___ | ___ |
| Errors | ___ | ___ |
| Memory (Peak) | ___ MB | ___ MB |
| Shuffle Partitions | N/A | 4 |

### Job 4: FDA Leave

| Metric | Informatica (Before) | PySpark (After) |
|--------|---------------------|-----------------|
| Total Duration | ___ sec | ___ sec |
| Source Rows Read | ___ | ___ |
| Target Rows Written | ___ | ___ |
| Errors | ___ | ___ |
| Memory (Peak) | ___ MB | ___ MB |
| Shuffle Partitions | N/A | 4 |

### Job 5: CPM (NIH + CDC)

| Metric | Informatica (Before) | PySpark (After) |
|--------|---------------------|-----------------|
| Total Duration | ___ sec | ___ sec |
| Source Rows Read | ___ | ___ |
| Target Rows Written | ___ | ___ |
| Errors | ___ | ___ |
| Memory (Peak) | ___ MB | ___ MB |
| Shuffle Partitions | N/A | 4 |

### Job 6: EHRP2BIIS

| Metric | Informatica (Before) | PySpark (After) |
|--------|---------------------|-----------------|
| Total Duration (per iteration) | ___ sec | ___ sec |
| Source Rows Read | ___ | ___ |
| Target Rows Written | ___ | ___ |
| Errors | ___ | ___ |
| Memory (Peak) | ___ MB | ___ MB |
| Shuffle Partitions | N/A | 4 |
| Polling Interval | ___ sec | 30 sec (configurable) |

### PySpark Performance Test Results (Synthetic Data)

| Test | Data Size | Duration | Throughput |
|------|-----------|----------|------------|
| CPM_NEWPAY_TBL Read | 100,502 rows | 2.03s | 49,527 rows/sec |
| PSEUDOSSN_TBL Read | 50,200 rows | 1.05s | 47,761 rows/sec |
| PS_GVT_JOB Read | 10,103 rows | 0.33s | 30,764 rows/sec |
| Dedup 50K Pseudossn | 50,200 rows | 1.44s | 34,958 rows/sec |
| Broadcast Join 10K | 10,000 rows | 0.50s | 20,157 rows/sec |
| Aggregation 100K | 100,502 rows | 3.64s | 27,597 rows/sec |
| Filter 100K CPM | 100,502 rows | 1.90s | 52,787 rows/sec |
| Write 10K Rows | 10,000 rows | 0.60s | 16,671 rows/sec |

---

## 7. Informatica-to-PySpark Glossary

| Informatica Term | PySpark Equivalent | Notes |
|-----------------|-------------------|-------|
| Workflow | Job class (`PayCalendarJob`, etc.) | One class per workflow |
| Session | Method on job class | `_session_reset_pay_calendar()` |
| Mapping | Transformation logic within session method | DataFrame operations |
| Source Qualifier | `spark.read.jdbc()` / `db_manager.read_jdbc()` | With pushdown query |
| Expression | `df.withColumn()` + `F.when()`, `F.lit()` | Column-level transforms |
| Filter | `df.filter()` / `df.where()` | Row filtering |
| Lookup (cached) | `df.join(F.broadcast(lookup_df), keys)` | Broadcast for small tables |
| Lookup (uncached) | `df.join(lookup_df, keys)` | Regular join |
| Aggregator | `df.groupBy().agg()` | Group-by aggregation |
| Router | Multiple `df.filter()` branches | Produces separate DataFrames |
| Sorter | `df.orderBy()` | With sort columns |
| Joiner | `df.join(other_df, keys, how)` | Inner/left/right/full |
| Normalizer | `F.explode()` / `F.arrays_zip()` | Flatten repeating groups |
| Update Strategy DD_INSERT | `db_manager.write_jdbc(mode="append")` | Insert rows |
| Update Strategy DD_UPDATE | `db_manager.execute_sql("UPDATE ...")` | Update rows |
| Sequence Generator | `F.monotonically_increasing_id()` | Or `F.row_number()` |
| ABORT() | `raise RuntimeError(message)` | After logging |
| $$WF_* variables | Instance attributes on job class | `self.wf_pp_end_year` |
| $SESSSTARTTIME | `datetime.now()` captured at session start | Stored in metrics |
| $PMMappingName | Python string constant | In module docstring |
| Reject file (.bad) | Side-output DataFrame or CSV | Failed rows |
| Pre/Post Session SQL | `db_manager.execute_sql()` | Before/after main logic |
| Email task | `email_service.send()` | With env prefix |
| Connection object | `OracleConnectionConfig` dataclass | Per-connection config |
| Parameter file (.iparms) | Environment variables | Via `python-dotenv` |
| Stored Procedure | `db_manager.execute_sql("CALL ...")` | Via JDBC |
| RUNFOREVER | `run_forever()` with polling loop | Signal handling |
| DTM buffer size | `spark.sql.shuffle.partitions` | Memory tuning |
| Max Memory | `spark.executor.memory` / `spark.driver.memory` | Spark config |
| High Precision | `DecimalType(p, s)` | For financial data |
| Dynamic Partitioning | Spark AQE + partition config | Adaptive execution |
| Collect performance data | `SessionMetrics` / `JobMetrics` | Always enabled |
| Enable Recovery | Spark checkpointing | For long-running jobs |
| Rollback on Errors | `db_manager` transaction rollback | JDBC-level |
| Stop on errors | Configurable error threshold | `max_errors` param |

---

## 8. Optimization & Data Quality Gap Analysis

### 8.1 Informatica Settings Audit (All 40 Sessions)

| Setting | Informatica Value | Risk | PySpark Fix |
|---------|------------------|------|-------------|
| Collect performance data | DISABLED (all) | CRITICAL | SessionMetrics on every method |
| Write perf to repository | DISABLED (all) | CRITICAL | JobMetrics JSON output |
| Enable Recovery | DISABLED (all) | HIGH | Spark checkpointing |
| Rollback on Errors | DISABLED (all) | HIGH | JDBC transaction rollback |
| Stop on errors (threshold) | 0 (ignore all) | MEDIUM | Configurable max_errors |
| Enable HA recovery | DISABLED (all) | MEDIUM | Spark driver restart |
| DTM buffer size | Default (auto) | LOW | spark.sql.shuffle.partitions=4 |
| High Precision | DISABLED | HIGH | DecimalType for financials |
| Lookup: Use Any Value | ALL lookups | HIGH | Deterministic row_number() |
| Dynamic Lookup Cache | DISABLED | LOW | Broadcast joins |

### 8.2 Data Quality Gap Matrix (7 DAMA Dimensions x 6 Jobs)

| Dimension | Job 1 | Job 2 | Job 3 | Job 4 | Job 5 | Job 6 |
|-----------|-------|-------|-------|-------|-------|-------|
| **Completeness** | Partial | Missing | Partial | Missing | Missing | Missing |
| **Validity** | Covered | Partial | Missing | Missing | Partial | Missing |
| **Uniqueness** | N/A | Missing | Covered | Missing | Missing | Missing |
| **Consistency** | Partial | Missing | Partial | Partial | Missing | Missing |
| **Accuracy** | Partial | Missing | Missing | Missing | Partial | Missing |
| **Timeliness** | Missing | Missing | Missing | Missing | Missing | Missing |
| **Integrity** | Missing | Missing | Partial | Missing | Missing | Missing |

**Legend**: Covered = fully addressed; Partial = some checks exist; Missing = no checks; N/A = not applicable

### 8.3 PySpark Remediation (All Dimensions Now Covered)

The PySpark implementation adds data quality checks via `test_data_quality.py` (35 tests):

1. **Completeness**: Threshold check (`tgt >= src * 0.95`), zero-source abort, truncated-source detection
2. **Validity**: PP_NUM range (1-26), PP_END_YEAR range, SSN format validation, date format checks
3. **Uniqueness**: Duplicate SSN+date detection, deterministic dedup, PK violation detection
4. **Consistency**: COUNTER_TBL vs actual COUNT(*) reconciliation, per-pay-period checks
5. **Accuracy**: Out-of-range values routed to ERROR_TBL, domain validation
6. **Timeliness**: Data freshness check (MAX(EFFDT) within pay period window), SLA monitoring
7. **Integrity**: Row count reconciliation (source = target + errors + rejects), checksum validation

---

## 9. Test Results Summary

### Test Execution

```
Total:     207 passed, 1 skipped, 0 failed
Coverage:  89% on pyspark_migration/jobs/
```

### Per-File Coverage

| File | Statements | Missing | Coverage |
|------|-----------|---------|----------|
| jobs/__init__.py | 7 | 0 | 100% |
| jobs/job1_pay_calendar.py | 139 | 13 | 91% |
| jobs/job2_comptime.py | 106 | 8 | 92% |
| jobs/job3_pseudossn.py | 212 | 36 | 83% |
| jobs/job4_fda_leave.py | 221 | 39 | 82% |
| jobs/job5_cpm.py | 169 | 21 | 88% |
| jobs/job6_ehrp2biis.py | 214 | 1 | 99% |
| **TOTAL** | **1068** | **118** | **89%** |

### Test Categories

| Category | Tests | Description |
|----------|-------|-------------|
| Unit (per-job) | 88 | Filter logic, expressions, lookups, aggregations, params, error handling |
| Unit (coverage) | 44 | Extended session method coverage for job2, job3, job6 |
| Common utilities | 16 | Config, DB manager, email, counters, metrics |
| Enhanced unit | 13 | Cross-job patterns (dedup, reconciliation, financial types) |
| Data quality | 35 | All 7 DAMA dimensions with explicit pass/fail criteria |
| Integration | 18 | End-to-end against Oracle XE with functional test data |
| Performance | 13 | Read/write/transform throughput with 10K-100K rows |

---

## 10. Rollback Plan

If issues are discovered after cutover to PySpark:

### Immediate Rollback Steps

1. **Stop PySpark jobs**: Kill any running PySpark processes or containers
2. **Restore Informatica schedules**: Re-enable Informatica workflow schedules in the PowerCenter Admin Console
3. **Verify Informatica connectivity**: Confirm all connection objects (`INFO_TARGET`, `$Source`, `INFO_NATE`) are still valid
4. **Run Informatica validation**: Execute `wf_Pay_Calendar` first (simplest) to verify the environment is functional

### Artifacts to Preserve

- [ ] All Informatica XML exports (already in repository under `XML/`)
- [ ] Credential files: `/home/sa-biisint/.use1`, `.pw1` (backup to secure vault)
- [ ] Parameter files: `/data/BIISINT/control/BIIS_parms.iparms`
- [ ] Shell scripts: `ehrp2biis_preload`, `actstage_load`, all transfer scripts
- [ ] SQL scripts: `ehrp2biis_afterload.sql`

### RUNFOREVER Stop/Restart Procedure (EHRP2BIIS)

**To stop the PySpark RUNFOREVER job**:
```bash
# Send SIGTERM for graceful shutdown (completes current iteration)
kill -TERM <pyspark_pid>

# Wait up to 60 seconds for graceful completion
# If still running after 60s, send SIGKILL
kill -9 <pyspark_pid>
```

**To restart Informatica RUNFOREVER**:
```bash
# Via pmcmd
pmcmd startworkflow -sv IntegrationService \
  -d Domain_biisint -u admin -p <pwd> \
  -f BIISINT -w wf_EHRP2BIIS_UPDATE -nowait
```

### Data Reconciliation After Rollback

After switching back to Informatica, run these checks:
1. Compare `COUNTER_TBL` values between PySpark and Informatica runs
2. Compare target table row counts
3. Verify `CURR_PP_FLAG` is correct in `PAY_PERIOD`
4. Check `ehrp_recs_tracking_tbl` for any unprocessed records

---

## 11. Environment Configuration

All credentials and connection details are loaded from environment variables:

| Variable | Description | Default |
|----------|-------------|---------|
| `ORACLE_HOST` | Oracle database host | localhost |
| `ORACLE_PORT` | Oracle database port | 1521 |
| `ORACLE_SERVICE_NAME` | Oracle service name | XEPDB1 |
| `ORACLE_USERNAME` | Primary DB username | biis_user |
| `ORACLE_PASSWORD` | Primary DB password | (required) |
| `ORACLE_CROSS_DB_HOST` | Cross-DB host (NATE) | (same as ORACLE_HOST) |
| `ORACLE_CROSS_DB_USERNAME` | Cross-DB username | nate_user |
| `ORACLE_CROSS_DB_PASSWORD` | Cross-DB password | (same as ORACLE_PASSWORD) |
| `ORACLE_SYS_PASSWORD` | System password for schema setup | (required for setup only) |
| `SMTP_HOST` | Email SMTP server | localhost |
| `SMTP_PORT` | Email SMTP port | 25 |
| `EMAIL_SENDER` | Email from address | biis-migration@hhs.gov |
| `EMAIL_RECIPIENTS` | Comma-separated recipients | (configured per environment) |

### Running Tests

```bash
# Unit tests only (no Oracle needed)
PYTHONPATH=. python -m pytest pyspark_migration/tests/ -q

# Full suite with Oracle
export ORACLE_PASSWORD=<password>
PYTHONPATH=. python -m pytest pyspark_migration/tests/ -v --tb=short

# Coverage report
PYTHONPATH=. python -m pytest pyspark_migration/tests/ \
  --cov=pyspark_migration/jobs --cov-report=term-missing

# Single job
python -m pyspark_migration.scripts.run_job --job pay_calendar \
  --pp-end-year 2026 --pp-num 5
```

### Docker Compose

```bash
# Start Oracle XE + migration service
docker-compose up -d

# Run schema setup
docker-compose exec migration python -m pyspark_migration.scripts.setup_oracle_schema

# Run all jobs
docker-compose exec migration python -m pyspark_migration.scripts.run_all_jobs
```

---

## 12. Known Caveats

1. **IICS DTT vs PowerCenter**: This migration is from PowerCenter 9.6.1 XML exports. IICS Data Transformation Tool (DTT) uses a different format; do not use DTT to auto-convert these workflows.

2. **Oracle XE vs Production**: Testing is done against Oracle XE (21-slim). Production Oracle may have different optimizer behavior, tablespace limits, or stored procedure versions. Test job classes separately against production-equivalent Oracle.

3. **Stored Procedures**: Job 6 calls HISTDBA stored procedures (`UPDATE_ERP2BIIS_NO900S01_p`, etc.) which are not migrated — they remain in Oracle. The PySpark job calls them via JDBC `CALL` statements.

4. **File Transfer**: Transfer scripts (NIH, OIG, FDA, CDC SFTP) are not part of this PySpark migration. They remain as KSH scripts. Consider migrating to Python `paramiko`/`fabric` in a future phase.

5. **PowerExchange Targets**: CPM jobs write to PowerExchange targets for file distribution. The PySpark equivalent writes to Oracle tables; file generation needs separate handling.

6. **Scheduler Integration**: Informatica's scheduler is replaced by external scheduling (cron, Airflow, etc.). The PySpark jobs provide CLI entry points but do not include their own scheduler.

7. **Cross-Database Lookups**: Job 6 uses `INFO_NATE` connection for lookups against a separate schema. In testing, this is simulated with a second Oracle user (`nate_user`). In production, verify the cross-DB connection config matches the actual NATE database.
