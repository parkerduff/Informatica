# EHRP2BIIS dbt Project

This dbt project converts the EHRP2BIIS_UPDATE Informatica PowerCenter ETL workflow to run in Snowflake. The original pipeline extracts federal employee HR action data from PeopleSoft and loads it into BIIS (Business Intelligence Information System) target tables.

## Project Overview

The EHRP2BIIS pipeline processes government employee personnel actions including hires, promotions, separations, and other HR events. This dbt implementation replicates all functionality of the original Informatica pipeline.

## Original Pipeline Components

### Source Tables (EHRP schema)
- **PS_GVT_JOB** - PeopleSoft government job records with 246 fields including employee ID, position, grade, salary, leave balances, and TSP information
- **NWK_NEW_EHRP_ACTIONS_TBL** (NKNIGHT schema) - Trigger table that identifies which employee actions need processing

### Target Tables
- **NWK_ACTION_PRIMARY_TBL** - Primary employee action data (200+ fields)
- **NWK_ACTION_SECONDARY_TBL** - Secondary action data including TSP, leave, and bonus details
- **EHRP_RECS_TRACKING_TBL** - Tracking table for processed records

## Project Structure

```
dbt_ehrp2biis/
├── dbt_project.yml           # Project configuration
├── profiles.yml.example      # Example Snowflake connection profile
├── models/
│   ├── staging/
│   │   ├── sources.yml       # Source table definitions
│   │   ├── stg_ps_gvt_job.sql
│   │   └── stg_nwk_new_ehrp_actions.sql
│   ├── intermediate/
│   │   ├── int_employee_enrichment.sql
│   │   └── int_sequence_generation.sql
│   ├── marts/
│   │   ├── fct_action_primary.sql
│   │   ├── fct_action_secondary.sql
│   │   ├── fct_tracking.sql
│   │   └── hist_action_primary.sql
│   ├── validation/
│   │   └── reconciliation_report.sql
│   └── schema.yml            # Model documentation and tests
└── macros/
    └── generate_event_id.sql # Event ID generation and transformation macros
```

## Key Transformations

### Event ID Generation
The original Informatica pipeline generates unique event IDs by extracting the year from the effective date and incrementing a sequence number. This is replicated using the `generate_event_id` macro which uses Snowflake's `ROW_NUMBER()` function.

### Data Enrichment via Lookups
The `int_employee_enrichment` model performs all lookup joins that were in the original pipeline:
- `PS_GVT_EMPLOYMENT` - Employment dates, tenure, probation info
- `PS_GVT_PERS_NID` - Social Security Number
- `PS_GVT_PERS_DATA` - Personal data (name, address, demographics)
- `PS_GVT_AWD_DATA` - Award and bonus information
- `PS_GVT_CITIZENSHIP` - Citizenship status
- `PS_HE_FILL_POS` - Position filling codes
- `PS_JPM_JP_ITEMS` - Education level and instructional program codes

### Field Transformations
The following transformations from the original `exp_MAIN2BIIS` expression are implemented:

1. **Agency Assignment Code**: Concatenates COMPANY and GVT_SUB_AGENCY
   ```sql
   company || gvt_sub_agency
   ```

2. **Zero-to-Null Conversion**: Converts zero values to NULL for leave balance fields
   ```sql
   nullif(he_al_balance, 0)
   ```

3. **Base Hours Calculation**: Doubles standard hours for non-intermittent schedules
   ```sql
   case when gvt_work_sched = 'I' then std_hours else std_hours * 2 end
   ```

## Setup Instructions

### Prerequisites
- Snowflake account with appropriate permissions
- dbt Core 1.0+ or dbt Cloud
- Python 3.8+

### Installation

1. Clone the repository:
   ```bash
   git clone <repository-url>
   cd dbt_ehrp2biis
   ```

2. Copy and configure the profiles file:
   ```bash
   cp profiles.yml.example ~/.dbt/profiles.yml
   ```

3. Edit `~/.dbt/profiles.yml` with your Snowflake credentials:
   ```yaml
   ehrp2biis:
     target: dev
     outputs:
       dev:
         type: snowflake
         account: your_account
         user: your_user
         password: your_password
         role: your_role
         database: BIIS_DEV
         warehouse: BIIS_WH
         schema: DBT_DEV
         threads: 4
   ```

4. Install dbt dependencies:
   ```bash
   dbt deps
   ```

### Running the Project

1. Test the connection:
   ```bash
   dbt debug
   ```

2. Run all models:
   ```bash
   dbt run
   ```

3. Run tests:
   ```bash
   dbt test
   ```

4. Generate documentation:
   ```bash
   dbt docs generate
   dbt docs serve
   ```

### Running Specific Models

Run only staging models:
```bash
dbt run --select staging
```

Run only mart models:
```bash
dbt run --select marts
```

Run the reconciliation report:
```bash
dbt run --select reconciliation_report
```

## Validation

The `reconciliation_report` model provides a comprehensive validation report that compares record counts across:
- Source tables (PS_GVT_JOB, NWK_NEW_EHRP_ACTIONS)
- Staging models
- Intermediate models
- Mart models

Query the report to verify consistency:
```sql
SELECT * FROM {{ ref('reconciliation_report') }}
WHERE report_section = 'CROSS_LAYER_VALIDATION';
```

## Data Tests

The following tests are configured in `schema.yml`:

- **Event ID uniqueness and not null** - Ensures each action has a unique identifier
- **EMPLID not null** - Validates employee ID is present
- **Referential integrity** - Validates relationships between primary, secondary, and tracking tables
- **Zero-to-null conversion** - Validates leave balance fields are properly converted

Run all tests:
```bash
dbt test
```

## Migration Notes

### Differences from Informatica Pipeline

1. **Sequence Generation**: The original pipeline used a stateful variable to track the previous year and increment the sequence. In dbt, we use `ROW_NUMBER()` partitioned by year to achieve the same result.

2. **Commit Intervals**: The original pipeline used commit intervals of 10,000 rows. In Snowflake, transactions are handled automatically.

3. **Stored Procedures**: The afterload stored procedures (`UPDT_ERP2BIIS_CRE8_REMARKS01_P`, etc.) are not included in this dbt project. These should be converted to dbt models or Snowflake stored procedures separately.

4. **Trigger Table Truncation**: The original pipeline truncated the trigger table after processing. This should be handled as a separate operation outside of dbt.

### Post-Migration Steps

After running the dbt models, you may need to:

1. Update the `SEQUENCE_NUM_TBL` with the new sequence numbers
2. Execute any downstream stored procedures
3. Truncate the trigger table for the next run

## Contributing

1. Create a feature branch
2. Make your changes
3. Run tests: `dbt test`
4. Submit a pull request

## Support

For questions or issues, contact the BIIS data team.
