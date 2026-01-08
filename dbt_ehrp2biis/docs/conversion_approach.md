# EHRP2BIIS Conversion Approach: Oracle/Informatica to Snowflake/dbt

## Overview

This document explains the technical approach used to convert the EHRP2BIIS pipeline from Oracle/Informatica to Snowflake/dbt, including mapping decisions, transformation logic, and architectural choices.

## Architecture Comparison

### Original Architecture (Oracle/Informatica)

```
┌─────────────────┐     ┌──────────────────┐     ┌─────────────────┐
│  EHRP Source    │────▶│   Informatica    │────▶│  Oracle BIIS    │
│  (PS_GVT_JOB)   │     │  PowerCenter     │     │  Database       │
└─────────────────┘     └──────────────────┘     └─────────────────┘
                               │
                               ▼
                        ┌──────────────────┐
                        │  Stored Procs    │
                        │  (HISTDBA)       │
                        └──────────────────┘
```

### New Architecture (Snowflake/dbt)

```
┌─────────────────┐     ┌──────────────────┐     ┌─────────────────┐
│  EHRP Source    │────▶│   Snowflake      │────▶│  dbt Models     │
│  (Replicated)   │     │   Streams/CDC    │     │  (Transform)    │
└─────────────────┘     └──────────────────┘     └─────────────────┘
                                                        │
                                                        ▼
                                                 ┌──────────────────┐
                                                 │  Snowflake DW    │
                                                 │  (Marts/Snaps)   │
                                                 └──────────────────┘
```

## Component Mapping

### Source Tables

| Oracle Table | Snowflake Table | Notes |
|--------------|-----------------|-------|
| `nknight.nwk_new_ehrp_actions_tbl` | `EHRP_RAW.NKNIGHT.NWK_NEW_EHRP_ACTIONS_TBL` | Trigger table, replaced by Streams |
| `ehrp.ps_gvt_job` | `EHRP_RAW.NKNIGHT.PS_GVT_JOB` | Complete job data |
| `nknight.sequence_num_tbl` | `EHRP_RAW.NKNIGHT.SEQUENCE_NUM_TBL` | Sequence control |

### Target Tables

| Oracle Table | dbt Model | Notes |
|--------------|-----------|-------|
| `nknight.ehrp_recs_tracking_tbl` | `fct_ehrp_recs_tracking` | Processing status |
| `nknight.nwk_action_primary_tbl` | `fct_action_primary` | Primary actions |
| `nknight.nwk_action_secondary_tbl` | `fct_action_secondary` | Secondary actions |
| `nknight.nwk_action_remarks_tbl` | `fct_action_remarks` | Generated remarks |
| `histdba.action_primary_all` | `snp_action_primary_all` | Historical snapshot |
| `histdba.action_secondary_all` | `snp_action_secondary_all` | Historical snapshot |
| `histdba.action_remarks_all` | `snp_action_remarks_all` | Historical snapshot |

## Transformation Mapping

### Informatica Transformations to dbt

#### Source Qualifier (SQ_PS_GVT_JOB)

**Original Informatica:**
```sql
SELECT * FROM PS_GVT_JOB, NWK_NEW_EHRP_ACTIONS_TBL
WHERE NWK_NEW_EHRP_ACTIONS_TBL.EMPLID = PS_GVT_JOB.EMPLID
  AND NWK_NEW_EHRP_ACTIONS_TBL.EMPL_RCD = PS_GVT_JOB.EMPL_RCD
  AND NWK_NEW_EHRP_ACTIONS_TBL.EFFDT = PS_GVT_JOB.EFFDT
  AND NWK_NEW_EHRP_ACTIONS_TBL.EFFSEQ = PS_GVT_JOB.EFFSEQ
ORDER BY PS_GVT_JOB.EFFDT
```

**dbt Equivalent (int_ehrp_job_actions.sql):**
```sql
select j.*
from stg_nwk_new_ehrp_actions a
inner join stg_ps_gvt_job j
    on a.emplid = j.emplid
    and a.empl_rcd = j.empl_rcd
    and a.effdt = j.effdt
    and a.effseq = j.effseq
order by j.effdt
```

#### Expression: exp_GET_EFFDT_YEAR

**Original Informatica:**
```
v_curr_year = GET_DATE_PART(EFFDT, 'YYYY')
o_CURRENT_YEAR = v_curr_year
```

**dbt Equivalent:**
```sql
date_part('year', effdt)::integer as effdt_year
```

#### Lookup: lkp_OLD_SEQUENCE_NUMBER

**Original Informatica:**
- Lookup table: `SEQUENCE_NUM_TBL`
- Condition: `EHRP_YEAR = o_CURRENT_YEAR`
- Returns: `EHRP_SEQ_NUMBER`

**dbt Equivalent:**
```sql
left join stg_sequence_num_tbl sn
    on ja.effdt_year = sn.ehrp_year
```

#### Expression: exp_MAIN2BIIS (Event ID Generation)

**Original Informatica:**
```
v_CURRENT_YEAR = GET_DATE_PART(EFFDT, 'YYYY')
v_EVENT_ID = IIF(v_CURRENT_YEAR = v_PREVIOUS_YEAR, v_EVENT_ID + 1, EHRP_SEQ_NUMBER + 1)
v_PREVIOUS_YEAR = v_CURRENT_YEAR
o_EVENT_ID = v_EVENT_ID
```

**dbt Equivalent (generate_biis_event_id macro):**
```sql
(
    effdt_year * 1000000 + 
    base_sequence_number + 
    row_number() over (
        partition by effdt_year 
        order by effdt, emplid, empl_rcd, effseq
    )
)
```

## Stored Procedure Conversions

### UPDT_ERP2BIIS_CRE8_REMARKS01_P

**Purpose:** Creates remarks for standard (non-900 series) actions

**Conversion Approach:**
- Converted to dbt macro `updt_erp2biis_cre8_remarks01()`
- Uses CASE statements to generate remarks based on NOA code patterns
- Integrated into `fct_action_remarks` model

### UPDATE_ERP2BIIS_NO900S01_p

**Purpose:** Processes non-900 series actions with additional formatting

**Conversion Approach:**
- Converted to dbt macro `update_erp2biis_no900s01()`
- Joins primary and secondary tables for complete context
- Generates additional remarks (M02 codes)

### ERP2BIIS_CRE8_REMARKS_900s01

**Purpose:** Creates remarks for 900-series actions

**Conversion Approach:**
- Converted to dbt macro `erp2biis_cre8_remarks_900s01()`
- Filters for event_id >= 9000000000
- Generates M90 remark codes

### UPDATE_ERP2BIIS_900SONLY01_P

**Purpose:** Formats 900-series only actions

**Conversion Approach:**
- Converted to dbt macro `update_erp2biis_900sonly01()`
- Generates M91 remark codes with correction/cancellation details

### UPDT_ORIG_CANCELLED_TRANS01_P

**Purpose:** Handles cancelled transactions

**Conversion Approach:**
- Converted to dbt macro `updt_orig_cancelled_trans01()`
- Identifies cancelled actions via `biis_wip_status_changed_dt`
- dbt snapshots handle the delete/re-insert pattern automatically

## Historical Table Pattern

### Oracle Pattern
```sql
-- Insert today's records
INSERT INTO action_primary_all
SELECT * FROM nwk_action_primary_tbl
WHERE load_date = TRUNC(sysdate);

-- Handle cancelled actions
DELETE FROM action_primary_all
WHERE event_id IN (SELECT biis_event_id FROM ehrp_recs_tracking_tbl
                   WHERE biis_wip_status_changed_dt = TRUNC(sysdate));

INSERT INTO action_primary_all
SELECT * FROM nwk_action_primary_tbl
WHERE event_id IN (SELECT biis_event_id FROM ehrp_recs_tracking_tbl
                   WHERE biis_wip_status_changed_dt = TRUNC(sysdate));
```

### dbt Snapshot Pattern
```sql
{% snapshot snp_action_primary_all %}
{{
    config(
        strategy='timestamp',
        updated_at='_loaded_at',
        unique_key='event_id',
        invalidate_hard_deletes=True
    )
}}
select * from {{ ref('fct_action_primary') }}
{% endsnapshot %}
```

The dbt snapshot automatically:
- Tracks all changes with SCD Type 2
- Sets `dbt_valid_to` when records are updated
- Maintains complete history without manual delete/insert

## Data Type Mapping

| Oracle Type | Snowflake Type | Notes |
|-------------|----------------|-------|
| `VARCHAR2(n)` | `VARCHAR(n)` | Direct mapping |
| `NUMBER(p,s)` | `NUMBER(p,s)` | Direct mapping |
| `DATE` | `DATE` | Date only |
| `DATE` (with time) | `TIMESTAMP_NTZ` | Date and time |
| `CLOB` | `VARCHAR(16777216)` | Large text |

## Sequence Number Management

### Oracle Pattern
- Uses `SEQUENCE_NUM_TBL` to track last used sequence per year
- Informatica lookup retrieves base sequence
- Expression increments for each record

### Snowflake Pattern
- Seed file `sequence_control.csv` provides initial values
- `ROW_NUMBER()` window function generates sequential IDs
- Event ID = year * 1000000 + base_sequence + row_number

## Change Data Capture

### Oracle Pattern
- Trigger table `nwk_new_ehrp_actions_tbl` populated externally
- Contains keys of records to process
- Truncated after each run

### Snowflake Pattern
- Snowflake Streams on source tables
- Automatically captures INSERT/UPDATE/DELETE
- Stream consumed by dbt incremental models

## Email Notifications

### Oracle Pattern
- Shell scripts use `mailx` command
- Sends to: peter.chen@hhs.gov, nathan.knight@hhs.gov, marvin.simon@hhs.gov

### Snowflake/dbt Pattern
- dbt Cloud notifications or
- Airflow email operators or
- Snowflake tasks with email integration

## Scheduling

### Oracle Pattern
- Autosys or cron jobs
- Daily at 12:00 starting 9/28/2018

### Snowflake/dbt Pattern
- dbt Cloud scheduled jobs or
- Airflow DAGs or
- Snowflake Tasks

## Testing Strategy

1. **Schema Tests:** Not null, unique, relationships
2. **Data Tests:** Custom SQL validations
3. **Parallel Run:** Compare Oracle vs Snowflake outputs
4. **Regression Tests:** Validate against known good data
