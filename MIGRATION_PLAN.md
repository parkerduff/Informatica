# Phased Migration Plan: Informatica PowerCenter 9.6.1 to PySpark

## Overview

This document describes a phased migration plan for the HHS BIIS (Business Intelligence Information System) ETL codebase from Informatica PowerCenter 9.6.1 to PySpark. The system processes federal payroll and employee data through a series of workflows defined as PowerCenter XML exports in the `XML/` directory.

### Source Workflows

| Workflow | XML Source File | Description | Schedule |
|---|---|---|---|
| `wf_Pay_Calendar` | `XML/Pay_Calendar` (1,083 lines) | Sets current pay period in `PAY_PERIOD` table | On-demand |
| `wf_COMPTIME` | `XML/COMPTIME` (1,044 lines) | Processes comp time flat files into `COMP_TIME_DAILY_TBL` | On-demand |
| `wf_EHRP2BIIS_UPDATE` | `XML/EHRP2BIIS_UPDATE` (2,828 lines) | Daily HR-to-BIIS update | Recurring (every 1 day) |
| `wf_FDA_Leave` | `XML/FDA_Leave` (7,284 lines) | FDA leave processing with multiple lookup validations | On-demand |

### Migration Order Rationale

All workflows depend on the `PAY_PERIOD` table having exactly one row with `CURR_PP_FLAG = 'Y'`. The `wf_Pay_Calendar` workflow is the one that sets this flag, making it the prerequisite for every other workflow. The migration order is therefore:

1. **Infrastructure & Shared Foundations** (Phase 1)
2. **`wf_Pay_Calendar`** (Phase 2) -- must be first, sets `CURR_PP_FLAG`
3. **`wf_COMPTIME`** (Phase 3) -- moderate complexity, flat file ingest
4. **`wf_EHRP2BIIS_UPDATE`** (Phase 4) -- daily recurring, Oracle-to-Oracle
5. **`wf_FDA_Leave`** (Phase 5) -- most complex, multiple lookups/error branches/normalizers/joiners

---

## Phase 1: Infrastructure & Shared Foundations

This phase establishes the PySpark project scaffolding and shared utilities that every downstream migration phase depends on.

### 1.1 -- Spark Session and Config Layer

**What it replaces:** The Informatica parameter file `/data/BIISINT/control/BIIS_parms.iparms` (referenced in `XML/COMPTIME` line 1020) and workflow-level variables.

The Informatica sessions reference parameters via `$Param_Root_Directory`, `$Param_COMPTIME_filename`, and workflow variables like `$$WF_PP_END_YEAR`, `$$WF_PP_NUM`, `$$WF_PP_YEAR_NUM` (`XML/COMPTIME` lines 651--662). Replace these with a Python config dictionary or config file:

```python
config = {
    "param_root_directory": "/data/BIISINT",
    "comptime_filename": "u0287d01.csv",
    "wf_pp_end_year": None,   # Set at runtime or from parameters
    "wf_pp_num": None,        # Set at runtime or from parameters
    "wf_pp_year_num": None,   # Derived: str(pp_end_year) + str(pp_num).zfill(2)
    "env": "Prod",            # Replaces $PMRepositoryServiceName prefix detection
    "comptime_email_list": "",
    "pay_calendar_email_list": "",
    "fda_leave_email_list": "",
}

spark = SparkSession.builder \
    .appName("BIIS_Migration") \
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
    .getOrCreate()
```

**Environment detection:** The current XML files detect the environment by inspecting `$PMRepositoryServiceName` (e.g., `XML/Pay_Calendar` lines 443):
```
DECODE(SUBSTR($PMRepositoryServiceName, 1, 4),
    'Dev_', 'Dev: ',
    'Test', 'Test: ',
    'Prod', 'Prod: ')
```
Replace with `config["env"]` set from an environment variable.

**JDBC connection properties:** All Oracle connections use named connections like `INFO_TARGET` (`XML/COMPTIME` line 672, 914) and `INFO_NATE` (`XML/EHRP2BIIS_UPDATE` line 2688). Define these centrally:

```python
jdbc_props = {
    "user": os.environ["ORACLE_USER"],
    "password": os.environ["ORACLE_PASSWORD"],
    "driver": "oracle.jdbc.driver.OracleDriver",
}
jdbc_url = "jdbc:oracle:thin:@//host:port/service"
```

### 1.2 -- PAY_PERIOD Broadcast (Universal Dependency)

**What it replaces:** The `lkp_PAY_PERIOD` lookup transformation found in every workflow. This lookup is configured with:
- **Lookup table:** `PAY_PERIOD` (`XML/COMPTIME` line 179, 346)
- **Lookup condition:** `CURR_PP_FLAG = in_CURR_PP_FLAG` where input is always `'Y'` (`XML/COMPTIME` lines 183, 350)
- **Lookup policy on multiple match:** `Use Any Value` (`XML/COMPTIME` lines 182, 349)
- **Output fields:** `PP_NUM` (decimal 2,0), `PP_END_YEAR` (decimal 4,0) as `LOOKUP/OUTPUT` ports (`XML/COMPTIME` lines 170--171, 337--338)
- **Additional lookup fields (non-output):** `PP_START_DTE`, `PP_END_DTE`, `LV_NUM`, `LV_YEAR`, `PAY_DTE`, `CURR_PP_FLAG` (`XML/COMPTIME` lines 172--177)
- **Connection:** `$Target` (Oracle) (`XML/COMPTIME` line 184)

**PAY_PERIOD table schema** (from `XML/COMPTIME` lines 29--39, `XML/FDA_Leave` lines 60--70):

| Field | Type | Key | Nullable |
|---|---|---|---|
| `PP_NUM` | number(2,0) | PRIMARY KEY | NOT NULL |
| `PP_END_YEAR` | number(4,0) | PRIMARY KEY | NOT NULL |
| `PP_START_DTE` | date | -- | NULL |
| `PP_END_DTE` | date | -- | NULL |
| `LV_NUM` | number(2,0) | -- | NULL |
| `LV_YEAR` | number(4,0) | -- | NULL |
| `PAY_DTE` | date | -- | NULL |
| `CURR_PP_FLAG` | varchar2(1) | -- | NULL |
| `HOLIDAY_1` | date | -- | NULL |
| `HOLIDAY_2` | date | -- | NULL |

**PySpark implementation:**

```python
def load_current_pay_period(spark, jdbc_url, jdbc_props):
    """Load and broadcast the current pay period row.
    
    Replaces lkp_PAY_PERIOD across all workflows.
    Handles 'Use Any Value' policy by deduplicating before broadcast.
    """
    pay_period_df = (
        spark.read.jdbc(jdbc_url, "PAY_PERIOD", properties=jdbc_props)
        .filter(col("CURR_PP_FLAG") == "Y")
        .dropDuplicates()  # Handle 'Use Any Value' multi-match policy
    )
    pp_row = pay_period_df.collect()[0]
    return {
        "PP_NUM": pp_row["PP_NUM"],
        "PP_END_YEAR": pp_row["PP_END_YEAR"],
        "PP_START_DTE": pp_row["PP_START_DTE"],
        "PP_END_DTE": pp_row["PP_END_DTE"],
        "PP_YEAR_NUM": int(str(pp_row["PP_END_YEAR"]) + str(pp_row["PP_NUM"]).zfill(2)),
        "broadcast_df": broadcast(pay_period_df),
    }
```

This replaces the Informatica workflow variables `$$WF_PP_NUM`, `$$WF_PP_END_YEAR`, and `$$WF_PP_YEAR_NUM` that flow through pre/post session variable assignments (`XML/COMPTIME` lines 651--662).

### 1.3 -- Shared Error Table Writer

**What it replaces:** The error branch pattern `exp_Format_*` -> `fil_Errors_*` -> `ERROR_TBL` found throughout the XML files. In `XML/FDA_Leave`, there are four separate error table instances for different lookup types:
- `ERROR_TBL_CPM` (line 1307) -- fed by `exp_Format_CPM` -> `fil_Errors_CPM`
- `ERROR_TBL_MER` (line 1308) -- fed by `exp_Format_MER` -> `fil_Errors_MER`
- `ERROR_TBL_YTD` (line 1309) -- fed by `exp_Format_YTD` -> `fil_Errors_YTD`
- `ERROR_TBL_PAD` (line 1310) -- fed by `exp_Format_PAD` -> `fil_Errors_PAD`

**ERROR_TBL schema** (from `XML/FDA_Leave` lines 83--91):

| Field | Type | Precision |
|---|---|---|
| `PROCESS_NAME` | varchar2 | 100 |
| `ERROR_MESSAGE` | varchar2 | 200 |
| `SOURCE_KEY` | varchar2 | 50 |
| `ERROR_DATE` | date | 19 |
| `PP_END_YEAR` | number(p,s) | 4,0 |
| `PP_NUM` | number(p,s) | 2,0 |
| `CYCLE_ID` | number(p,s) | 3,0 |
| `ERROR_CODE` | varchar2 | 50 |

**Error fields mapping** (from `XML/FDA_Leave` connector lines 1329--1356): Each `exp_Format_*` transformation produces:
- `ERR_PP_END_YEAR` -> `PP_END_YEAR`
- `ERR_PP_NUM` -> `PP_NUM`
- `ERR_TS` -> `ERROR_DATE`
- `ERR_EMPL_ID` -> `SOURCE_KEY`
- `ERR_DESC` -> `ERROR_MESSAGE`
- `PROCESS_NAME` -> `PROCESS_NAME`
- `ERR_CYCLE_ID` -> `CYCLE_ID`

**PySpark implementation:**

```python
from datetime import datetime

def write_error_table(error_df, process_name, pp_end_year, pp_num, cycle_id,
                      jdbc_url, jdbc_props):
    """Write error records to ERROR_TBL via JDBC append.
    
    Replaces the exp_Format_* -> fil_Errors_* -> ERROR_TBL pattern.
    """
    formatted_errors = error_df.select(
        lit(process_name).alias("PROCESS_NAME"),
        col("error_message").alias("ERROR_MESSAGE"),
        col("source_key").alias("SOURCE_KEY"),
        current_timestamp().alias("ERROR_DATE"),
        lit(pp_end_year).alias("PP_END_YEAR"),
        lit(pp_num).alias("PP_NUM"),
        lit(cycle_id).alias("CYCLE_ID"),
        col("error_code").alias("ERROR_CODE"),
    )
    formatted_errors.write.jdbc(
        jdbc_url, "ERROR_TBL", mode="append", properties=jdbc_props
    )
```

### 1.4 -- Shared Counter Table Writer

**What it replaces:** The `exp_Counters` -> `exp_Final` -> `COUNTER_TBL` chain found in `XML/COMPTIME` (lines 721--747) and `XML/FDA_Leave` (`m_0500_PM_FDA_IO_Counter`).

**COUNTER_TBL schema** (from `XML/COMPTIME` lines 41--48):

| Field | Type | Precision |
|---|---|---|
| `RUN_DATE` | date | -- |
| `PROCESS_NAME` | varchar2 | 100 |
| `COUNTER_DESCRIPTION` | varchar2 | 200 |
| `COUNTER_VALUE` | number | 15,0 |
| `PP_END_YEAR` | number(p,s) | 4,0 |
| `PP_NUM` | number(p,s) | 2,0 |
| `CYCLE_ID` | number(p,s) | 1,0 |

**Connector mapping** (from `XML/COMPTIME` lines 231--234):
- `exp_Final.o_RUN_DATE` -> `COUNTER_TBL.RUN_DATE`
- `exp_Final.COUNTER_DESCRIPTION` -> `COUNTER_TBL.COUNTER_DESCRIPTION`
- `exp_Final.COUNTER_VALUE` -> `COUNTER_TBL.COUNTER_VALUE`
- `exp_Final.o_PROCESS_NAME` -> `COUNTER_TBL.PROCESS_NAME`

**PySpark implementation:**

```python
def write_counter_table(process_name, description, value, pp_num, pp_end_year,
                        cycle_id, jdbc_url, jdbc_props, spark):
    """Write a single counter row to COUNTER_TBL.
    
    Replaces exp_Counters -> exp_Final -> COUNTER_TBL chain.
    """
    counter_row = spark.createDataFrame([{
        "RUN_DATE": datetime.now(),
        "PROCESS_NAME": process_name,
        "COUNTER_DESCRIPTION": description,
        "COUNTER_VALUE": value,
        "PP_END_YEAR": pp_end_year,
        "PP_NUM": pp_num,
        "CYCLE_ID": cycle_id,
    }])
    counter_row.write.jdbc(
        jdbc_url, "COUNTER_TBL", mode="append", properties=jdbc_props
    )
```

### 1.5 -- Orchestration Framework Setup

**What it replaces:** Informatica `<WORKFLOW>` / `<WORKFLOWLINK>` / `<SCHEDULER>` elements.

Each workflow in the XML files defines a sequence of sessions connected by `<WORKFLOWLINK>` elements with conditions like `$session.Status = Succeeded`. For example, `wf_COMPTIME` (`XML/COMPTIME` lines 959--962):

```xml
<WORKFLOWLINK CONDITION="" FROMTASK="Start" TOTASK="s_COMPTIME_Current_Pay_Period"/>
<WORKFLOWLINK CONDITION="$s_COMPTIME_Current_Pay_Period.Status = Succeeded"
              FROMTASK="s_COMPTIME_Current_Pay_Period"
              TOTASK="s_COMPTIME_Load_COMP_TIME_DAILY_TBL"/>
<WORKFLOWLINK CONDITION="$s_COMPTIME_Load_COMP_TIME_DAILY_TBL.Status = Succeeded"
              FROMTASK="s_COMPTIME_Load_COMP_TIME_DAILY_TBL"
              TOTASK="s_COMPTIME_Build_Message_Counters"/>
<WORKFLOWLINK CONDITION="$s_COMPTIME_Build_Message_Counters.Status = Succeeded"
              FROMTASK="s_COMPTIME_Build_Message_Counters"
              TOTASK="email_COMPTIME_Complete"/>
```

**Replace with Apache Airflow DAGs** (or Databricks Workflows):

- Each `<WORKFLOW>` becomes an Airflow DAG
- Each `<SESSION>` becomes a Python function wrapped in a `PythonOperator` task
- Each `<WORKFLOWLINK CONDITION="$session.Status = Succeeded">` becomes a DAG task dependency (`>>` operator)
- Workflow variables (`$$WF_SUBJECT`, `$$WF_MESSAGE`, `$$WF_PP_NUM`, `$$WF_PP_END_YEAR` -- `XML/COMPTIME` lines 1014--1019) become Airflow **XCom** values
- The `<SCHEDULER>` element determines the DAG schedule:
  - `SCHEDULETYPE="ONDEMAND"` -> no `schedule_interval` (trigger manually)
  - `SCHEDULETYPE="RECURRING"` with `DAYS="1"` (`XML/EHRP2BIIS_UPDATE` lines 2604--2610) -> `schedule_interval="@daily"`
- Email tasks (`XML/COMPTIME` line 628--631, `XML/Pay_Calendar` lines 589--592) become Airflow `EmailOperator` or Python `smtplib`

**Example Airflow DAG skeleton for wf_COMPTIME:**

```python
from airflow import DAG
from airflow.operators.python import PythonOperator

with DAG("wf_COMPTIME", schedule_interval=None) as dag:
    t1 = PythonOperator(task_id="s_COMPTIME_Current_Pay_Period", ...)
    t2 = PythonOperator(task_id="s_COMPTIME_Load_COMP_TIME_DAILY_TBL", ...)
    t3 = PythonOperator(task_id="s_COMPTIME_Build_Message_Counters", ...)
    t4 = PythonOperator(task_id="email_COMPTIME_Complete", ...)
    t1 >> t2 >> t3 >> t4
```

---

## Phase 2: Migrate `wf_Pay_Calendar` (Must Be First -- Prerequisite for All Others)

**Source file:** `XML/Pay_Calendar` (1,083 lines)

**Why first:** This workflow sets `CURR_PP_FLAG = 'Y'` on the `PAY_PERIOD` table. Every other workflow depends on this state. Without a current pay period, all downstream lookups (`lkp_PAY_PERIOD`) will fail.

**Workflow description** (from `XML/Pay_Calendar` line 585):
> "This workflow will update the Pay Period table and notify the appropriate parties of the new Current Pay Period."

**Schedule:** On-demand (`SCHEDULETYPE="ONDEMAND"` -- line 587)

### Workflow Sequence

From `XML/Pay_Calendar` workflow links (lines 985--989 region):

```
Start -> s_Pay_Calendar_Reset_Pay_Calendar
      -> s_Pay_Calendar_Set_Pay_Calendar
      -> s_Pay_Calendar_Verify_Pay_Calendar
      -> s_Pay_Calendar_Build_Message
      -> Email_Pay_Calendar
```

Each link has condition `$previous_session.Status = Succeeded`.

### Session 1: `s_Pay_Calendar_Reset_Pay_Calendar`

**Mapping:** `m_Pay_Calendar_Reset_Pay_Calendar` (`XML/Pay_Calendar` lines 480--543)

**Description** (line 480): "This mapping retrieves the record from the Pay Period table currently set to current and resets that record to the default."

**Transformation pipeline:**
1. **`SQ_PAY_PERIOD_RESET`** (Source Qualifier, lines 481--501): Reads from `PAY_PERIOD` with source filter `PAY_PERIOD.CURR_PP_FLAG = 'Y'` (line 492). Uses custom SQL override (none -- reads all fields matching the filter).
2. **`exp_Initial`** (Expression, lines 502--508): Sets `o_CURR_PP_FLAG = NULL` (line 506). Passes through `PP_NUM` and `PP_END_YEAR`.
3. **`upd_Reset_Current_PP`** (Update Strategy, lines 509--516): Uses `DD_UPDATE` strategy (line 513). Forward Rejected Rows = YES.
4. **Target: `RESET_PAY_PERIOD`** (mapped to `PAY_PERIOD` table): Receives `PP_NUM`, `PP_END_YEAR`, `CURR_PP_FLAG` (lines 524--526).

**Session config** (lines 595--672):
- `Treat source rows as = Data driven` (line 655)
- `Rollback Transactions on Errors = NO` (line 659)
- Target writer: Oracle `INFO_TARGET` connection, `Insert=YES`, `Update as Update=YES`, `Delete=YES` (lines 631--641)

**PySpark implementation:**

```python
def reset_pay_calendar(spark, jdbc_url, jdbc_props):
    """Reset CURR_PP_FLAG to NULL for all rows where it is currently 'Y'.
    
    Replaces m_Pay_Calendar_Reset_Pay_Calendar.
    Uses JDBC execute for the UPDATE since PySpark doesn't natively support
    in-place updates. Alternatively, use Delta Lake MERGE.
    """
    import jaydebeapi  # or cx_Oracle
    conn = jaydebeapi.connect(...)
    cursor = conn.cursor()
    cursor.execute(
        "UPDATE PAY_PERIOD SET CURR_PP_FLAG = NULL WHERE CURR_PP_FLAG = 'Y'"
    )
    conn.commit()
    cursor.close()
    conn.close()
```

### Session 2: `s_Pay_Calendar_Set_Pay_Calendar`

**Mapping:** `m_Pay_Calendar_Set_Pay_Calendar` (`XML/Pay_Calendar` lines 136--407)

**Description** (line 759): "This session uses the contents of a parameter file to determine whether a row will be set to current on the Pay Period table. If the parameters within the parameter file are set, the values within the parameter file will be used to determine the current pay period. If the parameters within the parameter file are empty, the system date will be used to determine the current pay period."

**Pre-session variable assignment** (lines 807--808):
- `$$PP_END_YEAR` <- `$$WF_PP_END_YEAR`
- `$$PP_NUM` <- `$$WF_PP_NUM`

**Transformation pipeline:**

1. **`SQ_PAY_PERIOD`** (Source Qualifier, lines 306--326): Custom SQL override (line 315):
   ```sql
   SELECT MAX(PAY_PERIOD.PP_NUM) PP_NUM, MAX(PAY_PERIOD.PP_END_YEAR) PP_END_YEAR
   FROM PAY_PERIOD
   ```
   This reads a single row with the max PP_NUM and PP_END_YEAR values.

2. **`exp_Initial`** (Expression, lines 327--337): Evaluates parameters:
   - `v_PARAM_PP_END_YEAR = IIF(NOT IS_NUMBER($$PP_END_YEAR), 0, TO_DECIMAL($$PP_END_YEAR))` (line 332)
   - `v_PARAM_PP_NUM = IIF(NOT IS_NUMBER($$PP_NUM), 0, TO_DECIMAL($$PP_NUM))` (line 333)
   - `v_TEN_DAYS_AGO = ADD_TO_DATE(SESSSTARTTIME, 'D', -10)` (line 331)
   - Outputs: `o_PARAM_PP_END_YEAR`, `o_PARAM_PP_NUM`

3. **`lkp_Existing_Pay_Period`** (Lookup, lines 186--235): Looks up PAY_PERIOD matching the parameter values:
   - Condition: `PP_NUM = in_PARAM_PP_NUM AND PP_END_YEAR = in_PARAM_PP_END_YEAR` (line 208)

4. **`exp_Determine_Parameters_Exist`** (Expression, lines 236--246): Sets flag:
   - `o_PARAM_EXISTS_FLAG = IIF(NOT ISNULL(lkp_PP_NUM), TRUE, FALSE)` (line 244)

5. **`rtr_Parameter_Non_Parameter`** (Router): Routes to two paths based on `PARAM_EXISTS_FLAG`:
   - **Parameter path:** When parameters exist, routes to `exp_Set_Current_Pay_Period_Param` -> `upd_Set_Current_PP_Param` -> `PAY_PERIOD_PARAM` target
   - **Non-parameter path:** Routes to `exp_Set_Date` -> `lkp_New_Current_Pay_Period` -> `exp_Set_Current_Pay_Period_Non_Param` -> `upd_Set_Current_PP_Non_Param` -> `PAY_PERIOD` target

6. **`lkp_New_Current_Pay_Period`** (Lookup, lines 247--289): For the non-parameter path, finds the pay period matching the current date:
   - Condition: `PP_START_DTE <= in_CURRENT_DATE AND PP_END_DTE >= in_CURRENT_DATE` (line 262)

7. **`exp_Set_Current_Pay_Period_Param`** / **`exp_Set_Current_Pay_Period_Non_Param`**: Both set `o_CURR_PP_FLAG = 'Y'` (lines 342, equivalent in Non_Param)

8. **`upd_Set_Current_PP_Param`** / **`upd_Set_Current_PP_Non_Param`**: Both use `DD_UPDATE` strategy (lines 298--304, 290--297)

**Session config** (lines 759--883):
- `Treat source rows as = Data driven` (line 866)
- `Rollback Transactions on Errors = NO` (line 870)
- `$Target connection value = Relational:INFO_TARGET` (line 865)

**PySpark implementation:**

```python
def set_pay_calendar(spark, jdbc_url, jdbc_props, pp_end_year=None, pp_num=None):
    """Set CURR_PP_FLAG = 'Y' for the target pay period.
    
    If pp_end_year and pp_num are provided, use those parameters.
    Otherwise, find the pay period matching the current date.
    """
    conn = get_jdbc_connection(jdbc_url, jdbc_props)
    cursor = conn.cursor()
    
    if pp_end_year and pp_num:
        # Parameter path: use provided values
        cursor.execute(
            "UPDATE PAY_PERIOD SET CURR_PP_FLAG = 'Y' "
            "WHERE PP_NUM = :1 AND PP_END_YEAR = :2",
            (pp_num, pp_end_year)
        )
    else:
        # Non-parameter path: use current date
        cursor.execute(
            "UPDATE PAY_PERIOD SET CURR_PP_FLAG = 'Y' "
            "WHERE PP_START_DTE <= SYSDATE AND PP_END_DTE >= SYSDATE"
        )
    
    conn.commit()
    cursor.close()
    conn.close()
```

### Session 3: `s_Pay_Calendar_Verify_Pay_Calendar`

**Mapping:** `m_Pay_Calendar_Verify_Pay_Calendar` (`XML/Pay_Calendar` lines 885--891)

**Description** (line 885): "This session verifies that only one row within the Pay Period table is set to current. If there are more than one rows set to current or none, the workflow will fail and an email will be sent to the appropriate parties."

The verification mapping uses `lkp_Current_Pay_Period` (found in `XML/FDA_Leave` line 1564) with SQL:
```sql
SELECT COUNT(*) as COUNT_CURRENT FROM PAY_PERIOD WHERE CURR_PP_FLAG = 'Y'
```

**PySpark implementation:**

```python
def verify_pay_calendar(spark, jdbc_url, jdbc_props):
    """Verify exactly one row has CURR_PP_FLAG = 'Y'.
    
    Replaces m_Pay_Calendar_Verify_Pay_Calendar.
    """
    count = (
        spark.read.jdbc(jdbc_url, "PAY_PERIOD", properties=jdbc_props)
        .filter(col("CURR_PP_FLAG") == "Y")
        .count()
    )
    if count != 1:
        raise RuntimeError(
            f"Pay Calendar verification failed: expected 1 current pay period, "
            f"got {count}"
        )
    return True
```

### Session 4: `s_Pay_Calendar_Build_Message`

**Mapping:** `m_Pay_Calendar_Build_Message` (`XML/Pay_Calendar` lines 408--479)

**Description** (line 408): "This mapping queries the Pay Calendar table for the record marked current and uses that record to build the subject and message for an email message."

**Source:** `SQ_PAY_PERIOD` with source filter `PAY_PERIOD.CURR_PP_FLAG = 'Y'` (line 425)

**Transformation: `exp_Initial`** (lines 435--449):
- `v_PP_NUM`: Left-pads PP_NUM to 2 digits (line 442):
  ```
  IIF(PP_NUM < 10, LPAD(TO_CHAR(PP_NUM), 2, '0'), TO_CHAR(PP_NUM))
  ```
- `v_ENVIRONMENT`: Detects environment from `$PMRepositoryServiceName` (line 443):
  ```
  DECODE(SUBSTR($PMRepositoryServiceName, 1, 4),
      'Dev_', 'Dev: ', 'Test', 'Test: ', 'Prod', 'Prod: ')
  ```
- `v_SUBJECT` (line 444):
  ```
  v_ENVIRONMENT || 'Pay Calendar Process Completed Successfully for: '
      || TO_CHAR(PP_END_YEAR) || '-' || v_PP_NUM
  ```
- `v_MESSAGE` (lines 446--447):
  ```
  'Current Pay Period = ' || v_PP_NUM || CHR(10) ||
  'Begin Date         = ' || TO_CHAR(PP_START_DTE, 'MM/DD/YYYY') || CHR(10) ||
  'End Date           = ' || TO_CHAR(PP_END_DTE, 'MM/DD/YYYY')
  ```
- Sets mapping variables via `SETVARIABLE($$MAP_SUBJECT, v_SUBJECT)` (line 445) and `SETVARIABLE($$MAP_MESSAGE, v_MESSAGE)` (line 447)

**Post-session variable assignment** (lines 700--701): Propagates `$$MAP_SUBJECT` -> `$$WF_SUBJECT` and `$$MAP_MESSAGE` -> `$$WF_MESSAGE`.

**Target:** `PAY_PERIOD_MESSAGE_FILE` flat file (output filename: `pay_period_message_file.txt` -- line 726)

**Email task** `Email_Pay_Calendar` (lines 589--592): Sends to `$$WF_PAY_CALENDAR_EMAIL_LIST` with subject `$$WF_SUBJECT` and text `$$WF_MESSAGE`.

**PySpark implementation:**

```python
def build_pay_calendar_message(spark, jdbc_url, jdbc_props, config):
    """Build and send success email for pay calendar update.
    
    Replaces m_Pay_Calendar_Build_Message + Email_Pay_Calendar task.
    """
    pp_row = (
        spark.read.jdbc(jdbc_url, "PAY_PERIOD", properties=jdbc_props)
        .filter(col("CURR_PP_FLAG") == "Y")
        .collect()[0]
    )
    
    env_prefix = f"{config['env']}: "
    pp_num_str = str(pp_row["PP_NUM"]).zfill(2)
    
    subject = (
        f"{env_prefix}Pay Calendar Process Completed Successfully for: "
        f"{pp_row['PP_END_YEAR']}-{pp_num_str}"
    )
    message = (
        f"Current Pay Period = {pp_num_str}\n"
        f"Begin Date         = {pp_row['PP_START_DTE'].strftime('%m/%d/%Y')}\n"
        f"End Date           = {pp_row['PP_END_DTE'].strftime('%m/%d/%Y')}"
    )
    
    # Send email (replace with Airflow EmailOperator or smtplib)
    send_email(config["pay_calendar_email_list"], subject, message)
    return subject, message
```

---

## Phase 3: Migrate `wf_COMPTIME`

**Source file:** `XML/COMPTIME` (1,044 lines)

**Schedule:** On-demand (`SCHEDULETYPE="ONDEMAND"` -- line 625)

### Workflow Sequence

From `XML/COMPTIME` lines 959--962:

```
Start -> s_COMPTIME_Current_Pay_Period     (condition: always)
      -> s_COMPTIME_Load_COMP_TIME_DAILY_TBL  (condition: previous succeeded)
      -> s_COMPTIME_Build_Message_Counters     (condition: previous succeeded)
      -> email_COMPTIME_Complete               (condition: previous succeeded)
```

**Workflow variables** (from `XML/COMPTIME` lines 1014--1019):
- `$$WF_SUBJECT`, `$$WF_MESSAGE` -- email content
- `$$WF_PP_END_YEAR`, `$$WF_PP_NUM`, `$$WF_PP_YEAR_NUM` -- pay period state
- `$$WF_COMPTIME_EMAIL_LIST` -- email recipients (line 629)

### Session 1: `s_COMPTIME_Current_Pay_Period`

**Mapping:** `m_COMPTIME_Current_Pay_Period` (`XML/COMPTIME` lines 633--643)

**Pre-session variable assignment** (lines 651--653):
- `$$MAP_PP_END_YEAR` <- `$$WF_PP_END_YEAR`
- `$$MAP_PP_NUM` <- `$$WF_PP_NUM`
- `$$MAP_PP_YEAR_NUM` <- `$$WF_PP_YEAR_NUM`

**Post-session success variable assignment** (lines 660--662): Reverse -- propagates mapping vars back to workflow vars.

**Purpose:** Read the current `PAY_PERIOD` row and write pay period dates to `COMP_TIME_DATE_FILE` flat file output (`comp_time_date_file.txt` -- line 687). This session uses the shared `load_current_pay_period()` utility from Phase 1.2.

**Source:** `PAY_PERIOD` table via Oracle `INFO_TARGET` connection (line 672)
**Target:** `COMP_TIME_DATE_FILE` flat file (line 642--643)

### Session 2: `s_COMPTIME_Load_COMP_TIME_DAILY_TBL`

**Mapping:** `m_COMPTIME_Load_COMP_TIME_DAILY_TBL` (`XML/COMPTIME` lines 275--577)

This is the main data pipeline for COMPTIME processing.

#### Step 1: Read CSV Source

**Source definition `U0287D01`** (flat file, `XML/COMPTIME` lines 6--28):
- Delimiter: comma (`,`) with double-quote quoting (line 7)
- Source file directory: `$Param_Root_Directory/data/int/in/COMPTIME` (line 906)
- Source filename: `$Param_COMPTIME_filename` (line 907)

**Schema** (lines 16--27):

| Field | Type | Precision | Scale | Offset |
|---|---|---|---|---|
| `SSN` | string | 9 | 0 | 0 |
| `NAME` | string | 30 | 0 | 9 |
| `CURRENT_ACCT` | string | 6 | 0 | 39 |
| `CURRENT_ORG` | string | 7 | 0 | 45 |
| `FLSA_STATUS` | string | 1 | 0 | 52 |
| `COMP_TIME_CUR_BAL` | number | 8 | 2 | 53 |
| `COMP_TIME_YEAR_EARNED` | number | 4 | 0 | 61 |
| `PP_END_DATE` | string | 8 | 0 | 65 |
| `DAILY_DATE_EARNED` | string | 8 | 0 | 73 |
| `COMP_TIME_RATE` | number | 6 | 2 | 81 |
| `COMP_TIME_HOURS` | number | 8 | 2 | 87 |
| `COMP_TIME_UNDEF` | number | 6 | 0 | 95 |

```python
from pyspark.sql.types import StructType, StructField, StringType, DecimalType

comptime_schema = StructType([
    StructField("SSN", StringType()),
    StructField("NAME", StringType()),
    StructField("CURRENT_ACCT", StringType()),
    StructField("CURRENT_ORG", StringType()),
    StructField("FLSA_STATUS", StringType()),
    StructField("COMP_TIME_CUR_BAL", DecimalType(8, 2)),
    StructField("COMP_TIME_YEAR_EARNED", DecimalType(4, 0)),
    StructField("PP_END_DATE", StringType()),
    StructField("DAILY_DATE_EARNED", StringType()),
    StructField("COMP_TIME_RATE", DecimalType(6, 2)),
    StructField("COMP_TIME_HOURS", DecimalType(8, 2)),
    StructField("COMP_TIME_UNDEF", DecimalType(6, 0)),
])

df = spark.read.csv(
    f"{config['param_root_directory']}/data/int/in/COMPTIME/{config['comptime_filename']}",
    schema=comptime_schema,
    header=False,
    quote='"',
)
```

#### Step 2: `exp_Initial` -- Classify Record Type

**Transformation** (`XML/COMPTIME` lines 300--316):
- Passes through all 12 source fields
- Adds `o_CURR_PP_FLAG = 'Y'` (line 313) -- hardcoded constant for lookup
- Adds `o_VALID_RECORD_FLAG` (line 314):
  ```
  DECODE(TRUE, IS_NUMBER(SSN), 1, 0)
  ```
  This checks if SSN is numeric. If yes, flag = 1 (valid); otherwise 0.

```python
df = df.withColumn("CURR_PP_FLAG", lit("Y"))
df = df.withColumn(
    "VALID_RECORD_FLAG",
    when(col("SSN").rlike("^[0-9]+$"), 1).otherwise(0)
)
```

Note: The user's description mentions a `RECORD_TYPE_FLAG` with `'H'` (header) vs `'D'` (detail) classification. Looking at the XML, the `fil_Detail` in the `m_COMPTIME_Build_Message_Counters` mapping (lines 98--104) filters on `RECORD_TYPE_FLAG`, while the load mapping uses `VALID_RECORD_FLAG` from `IS_NUMBER(SSN)`. The load mapping filters valid records (numeric SSN), while the counter mapping separately filters detail records.

#### Step 3: `fil_Valid_Records` -- Filter Valid Records

**Transformation** (`XML/COMPTIME` lines 400--417):
- Filter condition: `VALID_RECORD_FLAG = TRUE` (line 415)
- Passes through all fields plus `CURR_PP_FLAG` and `VALID_RECORD_FLAG`

```python
valid_df = df.filter(col("VALID_RECORD_FLAG") == 1)
```

#### Step 4: `lkp_PAY_PERIOD` -- Join with Current Pay Period

**Transformation** (`XML/COMPTIME` lines 335--377):
- Lookup table: `PAY_PERIOD` (line 346)
- Lookup condition: `CURR_PP_FLAG = in_CURR_PP_FLAG` (line 350) -- matches on `'Y'`
- Output fields: `PP_NUM`, `PP_END_YEAR` (lines 337--338)
- Policy on multiple match: `Use Any Value` (line 349)

```python
# Use the broadcast pay period from Phase 1.2
pp = load_current_pay_period(spark, jdbc_url, jdbc_props)
valid_df = valid_df.withColumn("lkp_PP_NUM", lit(pp["PP_NUM"]))
valid_df = valid_df.withColumn("lkp_PP_END_YEAR", lit(pp["PP_END_YEAR"]))
```

#### Step 5: `exp_Convert` -- Compute Derived Fields

**Transformation** (`XML/COMPTIME` lines 378--398):
- `o_PP_END_DATE`: Convert string to date (line 387):
  ```
  IIF(IS_DATE(PP_END_DATE, 'YYYYMMDD'), TO_DATE(PP_END_DATE, 'YYYYMMDD'))
  ```
- `o_DAILY_DATE_EARNED`: Convert string to date (line 389):
  ```
  IIF(IS_DATE(DAILY_DATE_EARNED, 'YYYYMMDD'), TO_DATE(DAILY_DATE_EARNED, 'YYYYMMDD'))
  ```
- `o_PP_END_YEAR`: pass-through from lookup (line 395)
- `o_PP_NUM`: pass-through from lookup (line 396)
- `o_PP_YEAR_NUM` (line 397):
  ```
  TO_DECIMAL(TO_CHAR(lkp_PP_END_YEAR) || LPAD(TO_CHAR(lkp_PP_NUM), 2, '0'))
  ```

```python
from pyspark.sql.functions import to_date, concat, lpad, col, when, lit

valid_df = valid_df.withColumn(
    "PP_END_DATE_converted",
    to_date(col("PP_END_DATE"), "yyyyMMdd")
).withColumn(
    "DAILY_DATE_EARNED_converted",
    to_date(col("DAILY_DATE_EARNED"), "yyyyMMdd")
).withColumn(
    "PP_YEAR_NUM",
    concat(
        col("lkp_PP_END_YEAR").cast("string"),
        lpad(col("lkp_PP_NUM").cast("string"), 2, "0")
    ).cast("decimal(6,0)")
)
```

#### Step 6: Write to `COMP_TIME_DAILY_TBL`

**Target** (`XML/COMPTIME` lines 913--924):
- Connection: Oracle `INFO_TARGET` (line 914)
- `Insert = YES`, `Update as Update = YES`, `Delete = YES` (lines 916--920)
- `Treat source rows as = Insert` (line 936)
- `Rollback Transactions on Errors = NO` (line 940)

**Target fields** (from connectors lines 428--442): `COMP_TIME_RATE`, `COMP_TIME_HOURS`, `COMP_TIME_UNDEF`, `PP_END_YEAR`, `PP_NUM`, `PP_YEAR_NUM`, `SSN`, `NAME`, `CURRENT_ACCT`, `CURRENT_ORG`, `FLSA_STATUS`, `COMP_TIME_CUR_BAL`, `COMP_TIME_YEAR_EARNED`, `PP_END_DATE`, `DAILY_DATE_EARNED`

```python
output_df = valid_df.select(
    col("SSN"), col("NAME"), col("CURRENT_ACCT"), col("CURRENT_ORG"),
    col("FLSA_STATUS"), col("COMP_TIME_CUR_BAL"), col("COMP_TIME_YEAR_EARNED"),
    col("PP_END_DATE_converted").alias("PP_END_DATE"),
    col("DAILY_DATE_EARNED_converted").alias("DAILY_DATE_EARNED"),
    col("COMP_TIME_RATE"), col("COMP_TIME_HOURS"), col("COMP_TIME_UNDEF"),
    col("lkp_PP_END_YEAR").alias("PP_END_YEAR"),
    col("lkp_PP_NUM").alias("PP_NUM"),
    col("PP_YEAR_NUM"),
)

output_df.write.jdbc(
    jdbc_url, "COMP_TIME_DAILY_TBL", mode="append", properties=jdbc_props
)
```

#### Step 7: Post-Session -- Archive Source File

**Post-session success command** (`XML/COMPTIME` line 761):
```
mv $Param_Root_Directory/data/int/in/COMPTIME/$Param_COMPTIME_filename
   $Param_Root_Directory/data/archive/COMPTIME/u0827d01_P$$WF_PP_YEAR_NUM.txt
```

```python
import shutil

src = f"{config['param_root_directory']}/data/int/in/COMPTIME/{config['comptime_filename']}"
dst = f"{config['param_root_directory']}/data/archive/COMPTIME/u0827d01_P{pp['PP_YEAR_NUM']}.txt"
shutil.move(src, dst)
```

### Session 3: `s_COMPTIME_Build_Message_Counters`

**Mapping:** `m_COMPTIME_Build_Message_Counters` (`XML/COMPTIME` lines 720--858)

**Pipeline:**
1. Read same CSV source (`U0287D01`) with same schema
2. `exp_Initial` -> `fil_Detail` (filter `RECORD_TYPE_FLAG = 'D'`) -> `agg_ALL_RECORDS` (COUNT of SSN)
3. `exp_Detail_Count` -> `lkp_PAY_PERIOD` -> `exp_Counters` -> `exp_Build_Message` -> `exp_Final_Message`
4. Write to `COUNTER_TBL` (Oracle, lines 786--797) and `COMPTIME_MESSAGE_FILE` (flat file `comptime_message_file.txt`, lines 798--816)

**Pre-session variable assignment** (lines 769--770): `$$MAP_SUBJECT` <- `$$WF_SUBJECT`, `$$MAP_MESSAGE` <- `$$WF_MESSAGE`
**Post-session success** (lines 777--778): Reverse propagation

**Connectors** (lines 231--256):
- `exp_Final` -> `COUNTER_TBL`: `o_RUN_DATE`, `COUNTER_DESCRIPTION`, `COUNTER_VALUE`, `o_PROCESS_NAME`
- `exp_Final_Message` -> `COMPTIME_MESSAGE_FILE`: `SUBJECT`, `MESSAGE`

```python
def build_comptime_counters(spark, jdbc_url, jdbc_props, config, pp):
    """Count records and write counters + build email message."""
    # Read and count detail records
    df = spark.read.csv(source_path, schema=comptime_schema, header=False, quote='"')
    detail_count = df.filter(col("SSN").rlike("^[0-9]+$")).count()
    
    # Write counter
    write_counter_table(
        process_name="COMPTIME",
        description="Detail Record Count",
        value=detail_count,
        pp_num=pp["PP_NUM"],
        pp_end_year=pp["PP_END_YEAR"],
        cycle_id=0,
        jdbc_url=jdbc_url,
        jdbc_props=jdbc_props,
        spark=spark,
    )
    
    # Build and send email
    subject = f"{config['env']}: COMPTIME completed - {detail_count} records"
    message = f"Detail records processed: {detail_count}"
    send_email(config["comptime_email_list"], subject, message)
```

### Email Task: `email_COMPTIME_Complete`

**Task** (`XML/COMPTIME` lines 628--631):
- Email User Name: `$$WF_COMPTIME_EMAIL_LIST`
- Email Subject: `$$WF_SUBJECT`
- Email Text: `$$WF_MESSAGE`

Replace with Python `smtplib` or Airflow `EmailOperator`.

---

## Phase 4: Migrate `wf_EHRP2BIIS_UPDATE`

**Source file:** `XML/EHRP2BIIS_UPDATE` (2,828 lines)

### Schedule

**Recurring daily** (`XML/EHRP2BIIS_UPDATE` lines 2604--2612):
```xml
<SCHEDULER>
    <SCHEDULEINFO>
        <STARTOPTIONS STARTDATE="9/28/2018" STARTTIME="12:00"/>
        <SCHEDULEOPTIONS SCHEDULETYPE="RECURRING">
            <RECURRING DAYS="1" HOURS="0" MINUTES="0"/>
        </SCHEDULEOPTIONS>
        <ENDOPTIONS ENDTYPE="RUNFOREVER" RUNFOREVER="YES"/>
    </SCHEDULEINFO>
</SCHEDULER>
```

Replace with Airflow `schedule_interval="@daily"` or Databricks daily trigger.

### Workflow Structure

**Single session workflow** (line 2614--2615):
```
Start -> s_m_EHRP2BIIS_UPDATE
```

**Mapping:** `m_EHRP2BIIS_UPDATE` -- a single large mapping with multiple lookups.

### Source Tables

1. **`PS_GVT_JOB`** (Oracle, lines 3--116): Primary source with 100+ fields covering employee job data including:
   - Employee ID fields: `EMPLID`, `EMPL_RCD`, `POSITION_NBR`
   - Job fields: `JOBCODE`, `DEPTID`, `LOCATION`, `COMPANY`
   - Government-specific: `GVT_PAY_PLAN`, `GVT_GRADE`, `GVT_STEP`, `GVT_NOA_CODE`
   - Dates: `EFFDT`, `GVT_EFFDT`, `GVT_EFFDT_PROPOSED`, `GVT_PAR_NTE_DATE`
   - Many more fields (80+ total from the source definition)

2. **`NWK_NEW_EHRP_ACTIONS_TBL`** (Oracle): Secondary source for new EHRP actions

### Lookup Transformations

The mapping performs multiple lookups against PeopleSoft tables (all via Oracle connections):
- `lkp_PS_GVT_EMPLOYMENT` (line 2621) -- employment details
- `lkp_PS_GVT_PERS_NID` (line 2624) -- personnel national ID
- `lkp_PS_GVT_AWD_DATA` (line 2630) -- award data
- `lkp_PS_GVT_EE_DATA_TRK` (line 2633) -- employee data tracking
- `lkp_PS_HE_FILL_POS` (line 2636) -- position fill
- `lkp_PS_GVT_CITIZENSHIP` (line 2639) -- citizenship
- `lkp_PS_GVT_PERS_DATA` (line 2642) -- personal data (sex, veteran pref, military status, etc.)
- `lkp_OLD_SEQUENCE_NUMBER` (line 2652) -- sequence numbers
- `lkp_PS_JPM_JP_ITEMS` (line 2655) -- job profile items (uses `INFO_NATE` connection -- line 2657)

### Expression Transformations

- `exp_MAIN2BIIS` (line 2627) -- main transformation logic
- `exp_PERS_DATA` (line 2645) -- personal data transformation
- `exp_GET_EFFDT_YEAR` (line 2649) -- extract effective date year

### Target Tables

**Target load order** (`XML/EHRP2BIIS_UPDATE` lines 2558--2560):
1. `NWK_ACTION_SECONDARY_TBL` (Oracle `INFO_NATE`, Insert=YES, Update=YES, Delete=YES -- lines 2687--2698)
2. `NWK_ACTION_PRIMARY_TBL` (Oracle `INFO_NATE`, Insert=YES, Update=YES, Delete=YES -- lines 2699--2710)
3. `EHRP_RECS_TRACKING_TBL` (Oracle `INFO_NATE`, Insert=YES, Update=YES, Delete=YES -- lines 2711--2722)

### Session Config

- `Stop on errors = 0` (line 2586)
- `Maximum Memory = 2GB` (line 2660)
- Success email to `Chingchiuan.Chen@hhs.gov` (line 2664)

### PySpark Implementation Strategy

```python
def ehrp2biis_update(spark, jdbc_url_source, jdbc_url_target, jdbc_props):
    """Daily EHRP to BIIS update pipeline.
    
    Replaces wf_EHRP2BIIS_UPDATE.
    """
    # 1. Read primary source
    ps_gvt_job = spark.read.jdbc(jdbc_url_source, "PS_GVT_JOB", properties=jdbc_props)
    nwk_actions = spark.read.jdbc(jdbc_url_target, "NWK_NEW_EHRP_ACTIONS_TBL",
                                   properties=jdbc_props)
    
    # 2. Perform lookups (broadcast small lookup tables)
    employment = broadcast(spark.read.jdbc(jdbc_url_source, "PS_GVT_EMPLOYMENT", ...))
    pers_nid = broadcast(spark.read.jdbc(jdbc_url_source, "PS_GVT_PERS_NID", ...))
    awd_data = broadcast(spark.read.jdbc(jdbc_url_source, "PS_GVT_AWD_DATA", ...))
    # ... additional lookups ...
    
    # 3. Join and transform
    result = (ps_gvt_job
        .join(employment, on=join_condition, how="left")
        .join(pers_nid, on=join_condition, how="left")
        # ... additional joins and withColumn transformations ...
    )
    
    # 4. Write to targets
    primary_df = result.select(...)  # NWK_ACTION_PRIMARY_TBL fields
    secondary_df = result.select(...)  # NWK_ACTION_SECONDARY_TBL fields
    tracking_df = result.select(...)  # EHRP_RECS_TRACKING_TBL fields
    
    primary_df.write.jdbc(jdbc_url_target, "NWK_ACTION_PRIMARY_TBL",
                          mode="append", properties=jdbc_props)
    secondary_df.write.jdbc(jdbc_url_target, "NWK_ACTION_SECONDARY_TBL",
                            mode="append", properties=jdbc_props)
    tracking_df.write.jdbc(jdbc_url_target, "EHRP_RECS_TRACKING_TBL",
                           mode="append", properties=jdbc_props)
```

---

## Phase 5: Migrate `wf_FDA_Leave` (Most Complex -- Last)

**Source file:** `XML/FDA_Leave` (7,284 lines)

**Description** (line 5945): "This workflow will read the FDA Leave input file from ITAS, and append leave records for each FDA employee. This file will then reside on a server for the FDA to read/process. An email will be generated upon completion. This workflow may be optionally run with pay period parameters. If the pay period parameters are not set, the current pay period will be used."

### Workflow Sequence

From `XML/FDA_Leave` lines 7108--7117:

```
Start -> s_0010_PM_FDA_Verify_File
      -> s_0020_PM_FDA_Set_CPM_Calendar
      -> s_0025_PM_FDA_Set_Pay_Calendar
      -> s_0050_PM_FDA_Update_CPM_CYCLE_TBL_FDA
      -> s_0100_PM_FDA_Load_TATRAN_To_DB
      -> s_0150_PM_FDA_Error_Counter
      -> s_0200_PM_FDA_Create_200_Rows
      -> s_0300_PM_FDA_Create_Output_File
      -> s_0500_PM_FDA_IO_Counter
      -> s_1100_PM_FDA_Send_Email
```

Each link has condition `$previous_session.Status = Succeeded`.

### Key Implementation Steps

#### 5.1 -- Pay Calendar Validation (`s_0010` through `s_0025`)

These initial sessions verify the input file exists, set the CPM calendar, and set the pay calendar. They are analogous to `wf_COMPTIME`'s `s_COMPTIME_Current_Pay_Period` and use the shared `load_current_pay_period()` utility.

The pay calendar set session uses the same parameter/non-parameter router pattern as `wf_Pay_Calendar`'s `m_Pay_Calendar_Set_Pay_Calendar`.

#### 5.2 -- Load TATRAN to Database (`s_0100_PM_FDA_Load_TATRAN_To_DB`)

This session reads the FDA Leave ITAS flat file and loads records into `HI_PM_FDA_TATRAN_TBL`.

**FDA Leave flat file source** (`XML/FDA_Leave` lines 73--80):
- Fixed-width format (not delimited)
- Fields: `FDA_TK_NO` (5), `FDA_EMP_ID` (9), `FDA_PP_YEAR` (4), `FDA_PP_NUM` (2), `FDA_REC_TYPE` (2), `FDA_DATA` (118)

#### 5.3 -- Lookup Validation with Error Branching (`m_0150_PM_FDA_Error_Counter` mapping)

This is the most complex part. The mapping at `XML/FDA_Leave` lines 166--1421 performs multiple lookup validations, each with its own error branch:

**Lookup transformations and their error branches:**

| Lookup | Error Expression | Error Filter | Error Target |
|---|---|---|---|
| `lkp_CPM_PAD_DETAIL_STG_TBL` (line 422) | `exp_Format_PAD` (line 399) | `fil_Errors_PAD` (line 413) | `ERROR_TBL_PAD` (line 1310) |
| `lkp_CPM_MER_DETAIL_STG_TBL` (line 619) | `exp_Format_MER` (line 754) | `fil_Errors_MER` (line 610) | `ERROR_TBL_MER` (line 1308) |
| `lkp_CPM_YTD_DETAIL_STG_TBL` (line 180) | `exp_Format_YTD` (line 166) | `fil_Errors_YTD` (line 1298) | `ERROR_TBL_YTD` (line 1309) |
| `lkp_CPM_NEWPAY_TBL` (line 768) | `exp_Format_CPM` (line 1275) | `fil_Errors_CPM` (line 1289) | `ERROR_TBL_CPM` (line 1307) |

**PySpark pattern for each lookup validation:**

```python
# For each lookup (PAD, MER, YTD, NEWPAY/CPM):
joined = df.join(broadcast(lookup_df), on=join_condition, how="left")
error_rows = joined.filter(col("lookup_key_field").isNull())
valid_rows = joined.filter(col("lookup_key_field").isNotNull())

# Write errors to ERROR_TBL using shared utility
write_error_table(error_rows, process_name="FDA_Leave_PAD", ...)

# Continue pipeline with valid_rows
df = valid_rows
```

#### 5.4 -- `nrm_Normalize_200_Records` -- Normalizer for 200-Series Records

**Transformation** (`XML/FDA_Leave` lines 1958 onwards):

This normalizer unpivots 14 record type columns into individual rows. The input fields are `FDA_SEQ_in1` through `FDA_SEQ_in14` and `FDA_DATA_in1` through `FDA_DATA_in14` (from connectors at lines 3422--3448). The common fields `FDA_TK_NO`, `FDA_EMP_ID`, `FDA_PP_YEAR`, `FDA_PP_NUM`, `FDA_REC_TYPE`, `FDA_BATCH_ID` are replicated for each output row.

Output fields: `FDA_TK_NO`, `FDA_EMP_ID`, `FDA_PP_YEAR`, `FDA_PP_NUM`, `FDA_REC_TYPE`, `FDA_DATA`, `FDA_SEQ`, `FDA_BATCH_ID` (connectors lines 3285--3292).

**PySpark implementation:**

```python
# Build stack expression for 14 record types
stack_expr = "stack(14, " + ", ".join(
    f"FDA_DATA_in{i}, FDA_SEQ_in{i}" for i in range(1, 15)
) + ") as (FDA_DATA, FDA_SEQ)"

normalized_df = df.selectExpr(
    "FDA_TK_NO", "FDA_EMP_ID", "FDA_PP_YEAR", "FDA_PP_NUM",
    "FDA_REC_TYPE", "FDA_BATCH_ID",
    stack_expr
).filter(col("FDA_DATA").isNotNull())
```

#### 5.5 -- `nrm_Counters_Message` -- Normalizer for Counter Rows

**Transformation** (`XML/FDA_Leave` lines 3594--3614):

This normalizer in mapping `m_0500_PM_FDA_IO_Counter` unpivots 6 counter column pairs into individual rows:
- Input: `COUNTER_DESCRIPTION_in1` through `COUNTER_DESCRIPTION_in6` (string, precision 200)
- Input: `COUNTER_VALUE_in1` through `COUNTER_VALUE_in6` (decimal, precision 10)
- `OCCURS = 6` on both source fields
- Output: `COUNTER_DESCRIPTION` (string, 200), `COUNTER_VALUE` (decimal, 10)
- Generated keys: `GK_COUNTER_DESCRIPTION`, `GCID_COUNTER_DESCRIPTION`, `GCID_COUNTER_VALUE`

**PySpark implementation:**

```python
counter_df = df.selectExpr(
    "stack(6, "
    "COUNTER_DESCRIPTION_in1, COUNTER_VALUE_in1, "
    "COUNTER_DESCRIPTION_in2, COUNTER_VALUE_in2, "
    "COUNTER_DESCRIPTION_in3, COUNTER_VALUE_in3, "
    "COUNTER_DESCRIPTION_in4, COUNTER_VALUE_in4, "
    "COUNTER_DESCRIPTION_in5, COUNTER_VALUE_in5, "
    "COUNTER_DESCRIPTION_in6, COUNTER_VALUE_in6"
    ") as (COUNTER_DESCRIPTION, COUNTER_VALUE)"
).filter(col("COUNTER_DESCRIPTION").isNotNull())
```

#### 5.6 -- `jnr_All_Counts` -- Joiner

**Transformation** (`XML/FDA_Leave` lines 3922--3942):

Joins the error count stream with the record count stream:
- **Detail (left) side:** `COUNT_READ_IN`, `LEAVE_REC_COUNT`, `WRITTEN_REC_COUNT`, `PP_NUM`, `PP_END_YEAR`, `CONSTANT`
- **Master (right) side:** `ERROR_REC_COUNT` (PORTTYPE=`INPUT/OUTPUT/MASTER`), `CONSTANT_ERROR` (PORTTYPE=`INPUT/MASTER`)
- **Join condition:** `CONSTANT_ERROR = CONSTANT` (line 3938) -- a constant-based join to combine the two single-row aggregates
- **Join type:** `Normal Join` (line 3939)

**PySpark implementation:**

```python
# Both streams produce single rows, join on the constant
record_counts = record_count_df.withColumn("join_key", lit(1))
error_counts = error_count_df.withColumn("join_key", lit(1))

all_counts = record_counts.join(error_counts, on="join_key", how="inner").drop("join_key")
```

#### 5.7 -- `srt_Distinct_File_Names` -- Sorter with Distinct

**Transformation** (`XML/FDA_Leave` lines 4982--4992):
- Sort key: `CurrentlyProcessedFileName` (string, 256), direction `ASCENDING` (line 4983)
- `Distinct = YES` (line 4988)
- `Case Sensitive = YES` (line 4985)

**PySpark implementation:**

```python
distinct_files = df.orderBy("CurrentlyProcessedFileName") \
    .dropDuplicates(["CurrentlyProcessedFileName"])
```

#### 5.8 -- Post-Load SQL on `HI_PM_FDA_TATRAN_TBL`

**Post SQL** (`XML/FDA_Leave` lines 5107--5109):

The target instance `HI_PM_FDA_TATRAN_TBL` has a Post SQL attribute:
```sql
delete
from HI_PM_FDA_TATRAN_TBL
where fda_emp_id not in
 (select distinct(fda_emp_id)
   from HI_PM_FDA_TATRAN_TBL
   where fda_rec_type = '12')
```

This cleanup removes all records for employees who don't have a type '12' record (which is a control/summary record).

**PySpark implementation:**

```python
def post_load_cleanup(jdbc_url, jdbc_props):
    """Execute post-load SQL cleanup on HI_PM_FDA_TATRAN_TBL.
    
    Deletes employees without a type '12' record.
    """
    conn = get_jdbc_connection(jdbc_url, jdbc_props)
    cursor = conn.cursor()
    cursor.execute("""
        DELETE FROM HI_PM_FDA_TATRAN_TBL
        WHERE fda_emp_id NOT IN (
            SELECT DISTINCT fda_emp_id
            FROM HI_PM_FDA_TATRAN_TBL
            WHERE fda_rec_type = '12'
        )
    """)
    conn.commit()
    cursor.close()
    conn.close()
```

#### 5.9 -- `exp_Convert_Minutes_REPLACESTR` -- Minutes Conversion

**Transformation** (`XML/FDA_Leave` line 1449):

Description: Converts quarter-hour notation to decimal hours using nested REPLACESTR:
```
REPLACESTR(0,
  REPLACESTR(0,
    REPLACESTR(0, i_FDA_DATA, '.25|', '.15|'),
    '.50|', '.30|'),
  '.75|', '.45|')
```

**PySpark implementation:**

```python
converted_df = df.withColumn(
    "FDA_DATA",
    regexp_replace(
        regexp_replace(
            regexp_replace(col("FDA_DATA"), "\\.25\\|", ".15|"),
            "\\.50\\|", ".30|"),
        "\\.75\\|", ".45|")
)
```

---

## Transformation-to-PySpark Mapping Reference

| Informatica Concept | PySpark / Python Equivalent |
|---|---|
| `<WORKFLOW>` + `<WORKFLOWLINK>` | Airflow DAG / Databricks Workflow |
| `<SESSION>` | Python function / Airflow task |
| `SQ_*` (Oracle source) | `spark.read.jdbc(...)` |
| `SQ_*` (Flat File source) | `spark.read.csv(...)` or fixed-width parsing |
| `exp_*` expression transformation | `.withColumn(...)` |
| `fil_*` filter transformation | `.filter(...)` / `.where(...)` |
| `lkp_*` lookup transformation | `.join(broadcast(...), how="left")` |
| `agg_*` aggregator transformation | `.groupBy().agg()` |
| `nrm_*` normalizer transformation | `stack()` SQL function / `selectExpr("stack(...)")` |
| `jnr_*` joiner transformation | `.join()` |
| `srt_*` + Distinct flag | `.orderBy().dropDuplicates()` |
| `rtr_*` router transformation | Multiple `.filter()` branches |
| `upd_*` Update Strategy (`DD_UPDATE`) | JDBC `UPDATE` statement or Delta Lake `MERGE INTO` |
| `$$MAP_*` mapping variables | Python variables (collected via `.collect()`) |
| `$$WF_*` workflow variables | Airflow XCom / Python state dict |
| `SESSSTARTTIME` | `datetime.now()` |
| `$PMMappingName` | Python function/job name string constant |
| `$PMRepositoryServiceName` | `config["env"]` environment variable |
| `Post SQL` on target | `jdbc_conn.execute(sql)` after write |
| Post-session `mv` shell command | `shutil.move(src, dst)` |
| Email task | Python `smtplib` / Airflow `EmailOperator` |
| Parameter file (`BIIS_parms.iparms`) | Python config dict / environment variables |
| `SETVARIABLE($$VAR, value)` | Python variable assignment |
| `IS_NUMBER(field)` | `col.rlike("^[0-9]+$")` |
| `REPLACESTR(0, str, old, new)` | `regexp_replace(col, old, new)` |
| `TO_DATE(str, 'YYYYMMDD')` | `to_date(col, "yyyyMMdd")` |
| `LPAD(str, n, '0')` | `lpad(col, n, "0")` |
| `DECODE(TRUE, cond, val, default)` | `when(cond, val).otherwise(default)` |

---

## Key Notes

### Migration Order Rationale
`Pay_Calendar` must be migrated first because all other workflows depend on the `PAY_PERIOD` table having exactly one row with `CURR_PP_FLAG = 'Y'`. The order is: `Pay_Calendar` -> `COMPTIME` -> `EHRP2BIIS_UPDATE` -> `FDA_Leave`.

### PAY_PERIOD as Shared State
Load and broadcast `PAY_PERIOD` once at the start of every pipeline -- it is universally used as a lookup in every mapping across all XML files. The `lkp_PAY_PERIOD` lookup appears in `XML/COMPTIME` (lines 168--210, 335--377), `XML/FDA_Leave` (line 353), and implicitly in all others.

### Error Handling Policy
The Informatica session config across all workflows uses:
- `Stop on errors = 0` (`XML/COMPTIME` line 605, `XML/Pay_Calendar` line 567, `XML/EHRP2BIIS_UPDATE` line 2586)
- `Rollback Transactions on Errors = NO` (`XML/COMPTIME` lines 705, 845, 940; `XML/Pay_Calendar` lines 659, 870)

Mirror this in PySpark with try/except around each write with a continue-on-error policy. Invalid records should be routed to the error table, not halt the pipeline.

### Lookup "Use Any Value" Policy
`lkp_PAY_PERIOD` is configured with `Lookup policy on multiple match = Use Any Value` (`XML/COMPTIME` lines 182, 349; `XML/Pay_Calendar` line 207). In PySpark, handle this by deduplicating the lookup DataFrame before broadcasting (`.dropDuplicates()`).

### COMPTIME Note
The `stop on errors = 0` setting means invalid records should be routed to the error table (via the `VALID_RECORD_FLAG` check in `exp_Initial`), not halt the pipeline. Records with non-numeric SSN are filtered out by `fil_Valid_Records` (line 415).

### Oracle Connection Names
The codebase uses two primary Oracle connection names:
- **`INFO_TARGET`**: Used by `wf_Pay_Calendar` and `wf_COMPTIME` for PAY_PERIOD and COMP_TIME_DAILY_TBL
- **`INFO_NATE`**: Used by `wf_EHRP2BIIS_UPDATE` for PeopleSoft tables and NWK_ACTION tables

### DateTime Format
The default session config DateTime Format String is `MM/DD/YYYY HH24:MI:SS.US` (`XML/COMPTIME` line 576, `XML/Pay_Calendar` line 556, `XML/EHRP2BIIS_UPDATE` line 2575). Ensure PySpark date parsing respects this format where applicable.

### Flat File Encoding
All flat file sources use `CODEPAGE="MS1252"` (Windows Latin-1). Set `encoding="cp1252"` when reading files in PySpark if the default UTF-8 doesn't work.
