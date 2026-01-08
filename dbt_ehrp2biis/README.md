# dbt_ehrp2biis - EHRP2BIIS Pipeline Conversion

This dbt project is a conversion of the EHRP2BIIS_UPDATE Informatica PowerCenter ETL pipeline to run in Snowflake. The pipeline extracts federal employee HR action data from PeopleSoft and loads it into BIIS (Business Intelligence Information System) target tables.

## Project Overview

The original Informatica pipeline processed government employee personnel actions from the Enterprise Human Resources Platform (EHRP) and loaded them into three target tables. This dbt project replicates all functionality including source joins, lookup enrichments, field transformations, and event ID generation.

## Pipeline Architecture

### Data Flow

```
Source Tables (EHRP/NKNIGHT schemas)
    |
    v
Staging Models (views)
    |
    v
Intermediate Models (views)
    - Sequence Generation
    - Employee Enrichment (7 lookup joins)
    |
    v
Mart Models (tables)
    - fct_action_primary
    - fct_action_secondary
    - fct_tracking
    |
    v
Historical Model (incremental)
    - hist_action_primary
```

### Source Tables

The pipeline reads from two primary source tables:

1. **PS_GVT_JOB** (EHRP schema) - PeopleSoft government job records containing 246+ fields including employee ID, position, grade, salary, leave balances, and TSP information.

2. **NWK_NEW_EHRP_ACTIONS_TBL** (NKNIGHT schema) - Trigger table that identifies which employee actions need processing. Records are joined on EMPLID, EMPL_RCD, EFFDT, and EFFSEQ.

### Target Tables

The pipeline produces three main output tables:

1. **fct_action_primary** - Primary employee action data with 100+ fields including personal information, employment details, leave balances, and salary data.

2. **fct_action_secondary** - Secondary action data including TSP information, leave details, bonus amounts, and retention data.

3. **fct_tracking** - Tracking table for processed records with event IDs and WIP status.

## Key Transformations

### Event ID Generation

The pipeline generates unique event IDs using a year-based sequence:

1. Extract year from effective date: `EXTRACT(YEAR FROM effdt)`
2. Look up the last sequence number for that year from `SEQUENCE_NUM_TBL`
3. Generate sequential IDs using `ROW_NUMBER()` partitioned by year

### Data Enrichment Lookups

The intermediate layer performs LEFT JOINs to enrich data from seven lookup tables:

- **PS_GVT_EMPLOYMENT** - Employment dates, tenure, probation info
- **PS_GVT_PERS_NID** - Social Security Number
- **PS_GVT_PERS_DATA** - Personal data (name, address, demographics)
- **PS_GVT_AWD_DATA** - Award and bonus information
- **PS_GVT_CITIZENSHIP** - Citizenship status
- **PS_HE_FILL_POS** - Position filling codes
- **PS_JPM_JP_ITEMS** - Education level and instructional program codes

### Field Transformations

Key transformations implemented in the mart models:

| Transformation | Original Logic | dbt Implementation |
|---------------|----------------|-------------------|
| Agency Assignment Code | `COMPANY \|\| GVT_SUB_AGENCY` | `company \|\| gvt_sub_agency` |
| Zero-to-NULL Conversion | `IIF(field=0, NULL, field)` | `CASE WHEN field = 0 THEN NULL ELSE field END` |
| Base Hours Calculation | `IIF(GVT_WORK_SCHED='I', STD_HOURS, STD_HOURS*2)` | `CASE WHEN gvt_work_sched = 'I' THEN std_hours ELSE std_hours * 2 END` |
| Prior Agency Subelement | `IIF(IS_SPACES(field) OR ISNULL(field), NULL, field \|\| '00')` | `CASE WHEN field IS NULL OR TRIM(field) = '' THEN NULL ELSE field \|\| '00' END` |

## Project Structure

```
dbt_ehrp2biis/
├── dbt_project.yml           # Project configuration
├── models/
│   ├── staging/
│   │   ├── stg_ps_gvt_job.sql           # Staged job records
│   │   └── stg_nwk_new_ehrp_actions.sql # Staged trigger table
│   ├── intermediate/
│   │   ├── int_sequence_generation.sql   # Event ID generation
│   │   └── int_employee_enrichment.sql   # Lookup enrichments
│   ├── marts/
│   │   ├── fct_action_primary.sql        # Primary action facts
│   │   ├── fct_action_secondary.sql      # Secondary action facts
│   │   ├── fct_tracking.sql              # Tracking records
│   │   └── hist_action_primary.sql       # Historical incremental
│   ├── validation/
│   │   └── reconciliation_report.sql     # Record count validation
│   └── schema.yml                        # Model documentation and tests
├── macros/
│   └── generate_event_id.sql             # Reusable transformation macros
└── README.md
```

## Configuration

### Variables

Configure the following variables in your `dbt_project.yml` or via command line:

```yaml
vars:
  source_database: 'EHRP_SOURCE'      # Source database name
  source_schema_ehrp: 'EHRP'          # EHRP schema name
  source_schema_nknight: 'NKNIGHT'    # NKNIGHT schema name
  target_schema: 'INFO_TARGET_DEV'    # Target schema name
```

### Profile Setup

Create a profile in `~/.dbt/profiles.yml`:

```yaml
ehrp2biis:
  target: dev
  outputs:
    dev:
      type: snowflake
      account: <your_account>
      user: <your_user>
      password: <your_password>
      role: <your_role>
      database: <your_database>
      warehouse: <your_warehouse>
      schema: <your_schema>
      threads: 4
```

## Running the Project

### Full Refresh

```bash
cd dbt_ehrp2biis
dbt run
```

### Incremental Load (Historical Table)

```bash
dbt run --select hist_action_primary
```

### Run Tests

```bash
dbt test
```

### Generate Documentation

```bash
dbt docs generate
dbt docs serve
```

## Validation

### Reconciliation Report

The `reconciliation_report` model provides record counts across all layers to verify data consistency:

```sql
SELECT * FROM {{ ref('reconciliation_report') }}
```

This report compares:
- Source table counts
- Staging model counts
- Intermediate model counts
- Mart model counts

### Data Tests

The schema.yml file includes tests for:

- **Event ID**: Uniqueness and not null
- **EMPLID**: Not null across all models
- **Referential Integrity**: fct_action_secondary.event_id references fct_action_primary.event_id
- **Required Fields**: Not null tests on critical fields

## Migration Notes

### Differences from Informatica Pipeline

1. **Sequence Generation**: The original pipeline used session variables to track previous year and increment event IDs. The dbt implementation uses `ROW_NUMBER()` with partitioning for equivalent functionality.

2. **Lookup Caching**: Informatica used lookup caching for performance. In Snowflake, this is handled by the query optimizer and result caching.

3. **Commit Intervals**: The original pipeline used 10,000 row commit intervals. In dbt, the entire transformation is atomic.

4. **Post-Load Processing**: The `ehrp2biis_afterload.sql` stored procedures are not included in this dbt project. These should be converted to separate dbt models or Snowflake stored procedures as needed.

### Post-Load Procedures to Convert

The following stored procedures from the afterload script need separate implementation:

- `UPDT_ERP2BIIS_CRE8_REMARKS01_P`
- `UPDATE_ERP2BIIS_NO900S01_p`
- `ERP2BIIS_CRE8_REMARKS_900s01`
- `UPDATE_ERP2BIIS_900SONLY01_P`
- `UPDT_ORIG_CANCELLED_TRANS01_P`

## Macros

The project includes reusable macros in `macros/generate_event_id.sql`:

- `generate_event_id(year_column, base_sequence_column)` - Generates unique event IDs
- `zero_to_null(column_name)` - Converts zero values to NULL
- `calculate_base_hours(work_schedule_column, std_hours_column)` - Calculates base hours
- `build_agency_assignment_code(company_column, sub_agency_column)` - Builds agency code
- `build_prior_agency_subelement(xfer_from_agcy_column)` - Builds prior agency code
- `build_legal_authority_text(auth_d1_column, auth_d1_2_column)` - Builds legal authority text

## Support

For questions about this conversion, refer to the original Informatica pipeline documentation in the `XML/EHRP2BIIS_UPDATE` folder.
