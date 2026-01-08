# dbt_ehrp2biis

A dbt project that converts the EHRP2BIIS pipeline from Oracle/Informatica to Snowflake/dbt.

## Overview

The EHRP2BIIS pipeline integrates employee personnel action data from the Enterprise Human Resources Platform (EHRP) into the Business Intelligence Information System (BIIS). This dbt project replicates the functionality of the original Oracle/Informatica implementation using modern data platform capabilities.

## Project Structure

```
dbt_ehrp2biis/
├── models/
│   ├── staging/           # Source data staging
│   │   ├── stg_nwk_new_ehrp_actions.sql
│   │   ├── stg_ps_gvt_job.sql
│   │   └── stg_sequence_num_tbl.sql
│   ├── intermediate/      # Transformation logic
│   │   ├── int_ehrp_job_actions.sql
│   │   └── int_biis_event_generation.sql
│   └── marts/             # Final output tables
│       ├── fct_action_primary.sql
│       ├── fct_action_secondary.sql
│       ├── fct_action_remarks.sql
│       └── fct_ehrp_recs_tracking.sql
├── snapshots/             # Historical tracking (SCD Type 2)
│   ├── snp_action_primary_all.sql
│   ├── snp_action_secondary_all.sql
│   └── snp_action_remarks_all.sql
├── macros/                # Reusable transformation logic
│   ├── generate_biis_event_id.sql
│   ├── format_remarks.sql
│   ├── updt_erp2biis_cre8_remarks01.sql
│   ├── update_erp2biis_no900s01.sql
│   ├── erp2biis_cre8_remarks_900s01.sql
│   ├── update_erp2biis_900sonly01.sql
│   └── updt_orig_cancelled_trans01.sql
├── seeds/                 # Static reference data
│   └── sequence_control.csv
├── tests/                 # Data validation tests
│   ├── validate_record_counts.sql
│   ├── validate_action_code_distribution.sql
│   ├── validate_biis_event_id.sql
│   └── validate_historical_sync.sql
└── docs/                  # Documentation
    ├── migration_plan.md
    └── conversion_approach.md
```

## Prerequisites

- Snowflake account with appropriate permissions
- dbt Core 1.0+ or dbt Cloud
- Python 3.8+

## Installation

1. Clone the repository:
```bash
git clone https://github.com/parkerduff/Informatica.git
cd Informatica/dbt_ehrp2biis
```

2. Install dbt-snowflake:
```bash
pip install dbt-snowflake
```

3. Configure your profile in `~/.dbt/profiles.yml`:
```yaml
ehrp2biis:
  target: dev
  outputs:
    dev:
      type: snowflake
      account: your_account
      user: your_user
      password: your_password
      role: TRANSFORMER
      database: EHRP_DW
      warehouse: EHRP_WH
      schema: DEV
      threads: 4
```

4. Set environment variables:
```bash
export SNOWFLAKE_ACCOUNT=your_account
export SNOWFLAKE_USER=your_user
export SNOWFLAKE_PASSWORD=your_password
export SNOWFLAKE_ROLE=TRANSFORMER
```

## Usage

### Run all models
```bash
dbt run
```

### Run specific models
```bash
dbt run --select staging
dbt run --select marts
dbt run --select fct_action_primary
```

### Run snapshots
```bash
dbt snapshot
```

### Run tests
```bash
dbt test
```

### Generate documentation
```bash
dbt docs generate
dbt docs serve
```

## Key Conversions

### Informatica Mapping → dbt Models

| Informatica Component | dbt Equivalent |
|----------------------|----------------|
| Source Qualifier (SQ_PS_GVT_JOB) | `int_ehrp_job_actions` |
| exp_GET_EFFDT_YEAR | `date_part('year', effdt)` |
| lkp_OLD_SEQUENCE_NUMBER | Join to `stg_sequence_num_tbl` |
| exp_MAIN2BIIS | `generate_biis_event_id` macro |

### Oracle Stored Procedures → dbt Macros

| Oracle Procedure | dbt Macro |
|-----------------|-----------|
| UPDT_ERP2BIIS_CRE8_REMARKS01_P | `updt_erp2biis_cre8_remarks01()` |
| UPDATE_ERP2BIIS_NO900S01_p | `update_erp2biis_no900s01()` |
| ERP2BIIS_CRE8_REMARKS_900s01 | `erp2biis_cre8_remarks_900s01()` |
| UPDATE_ERP2BIIS_900SONLY01_P | `update_erp2biis_900sonly01()` |
| UPDT_ORIG_CANCELLED_TRANS01_P | `updt_orig_cancelled_trans01()` |

### Historical Tables → dbt Snapshots

| Oracle Table | dbt Snapshot |
|--------------|--------------|
| action_primary_all | `snp_action_primary_all` |
| action_secondary_all | `snp_action_secondary_all` |
| action_remarks_all | `snp_action_remarks_all` |

## Validation

Run validation tests to compare Snowflake output with Oracle baseline:

```bash
# Set expected counts from Oracle
dbt test --vars '{"expected_primary_count": 1000000, "expected_secondary_count": 1000000}'
```

## Documentation

- [Migration Plan](docs/migration_plan.md) - Detailed migration strategy and timeline
- [Conversion Approach](docs/conversion_approach.md) - Technical conversion details

## Original Pipeline

The original Oracle/Informatica pipeline consists of:
1. `ehrp2biis_preload` - Preload preparation script
2. `m_EHRP2BIIS_UPDATE` - Main Informatica mapping
3. `ehrp2biis_afterload.sql` - Post-load processing
4. `actstage_load` - Action staging script

## Support

For questions or issues, contact:
- peter.chen@hhs.gov
- nathan.knight@hhs.gov
- marvin.simon@hhs.gov
