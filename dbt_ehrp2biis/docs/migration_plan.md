# EHRP2BIIS Migration Plan: Oracle/Informatica to Snowflake/dbt

## Executive Summary

This document outlines the migration strategy for converting the EHRP2BIIS pipeline from Oracle/Informatica to Snowflake/dbt. The migration preserves all existing business logic while leveraging modern data platform capabilities.

## Migration Phases

### Phase 1: Infrastructure Setup (Week 1-2)

#### Snowflake Environment
1. Create Snowflake account and configure warehouses
2. Set up databases: `EHRP_RAW`, `EHRP_DW`
3. Create schemas: `NKNIGHT` (staging), `BIIS` (marts), `HISTDBA` (historical)
4. Configure role-based access control (RBAC)
5. Set up Snowflake Streams for CDC on source tables

#### dbt Environment
1. Install dbt-snowflake adapter
2. Configure profiles.yml with environment variables
3. Set up CI/CD pipeline for dbt runs
4. Configure dbt Cloud or Airflow for scheduling

### Phase 2: Data Migration (Week 3-4)

#### Source Data Migration
1. Extract historical data from Oracle tables
2. Load into Snowflake staging tables using Snowpipe or COPY INTO
3. Validate record counts and data integrity
4. Set up ongoing replication from EHRP source system

#### Reference Data
1. Migrate SEQUENCE_NUM_TBL to Snowflake
2. Load seed data for sequence control
3. Validate sequence number continuity

### Phase 3: Model Development (Week 5-8)

#### Staging Models
- `stg_nwk_new_ehrp_actions` - Trigger table replacement
- `stg_ps_gvt_job` - Complete job data staging
- `stg_sequence_num_tbl` - Sequence number staging

#### Intermediate Models
- `int_ehrp_job_actions` - Source qualifier join logic
- `int_biis_event_generation` - Event ID generation

#### Mart Models
- `fct_action_primary` - Primary action records (260 fields)
- `fct_action_secondary` - Secondary action records (209 fields)
- `fct_action_remarks` - Generated remarks
- `fct_ehrp_recs_tracking` - Processing status tracking

#### Snapshots
- `snp_action_primary_all` - Historical primary records
- `snp_action_secondary_all` - Historical secondary records
- `snp_action_remarks_all` - Historical remarks

### Phase 4: Macro Development (Week 9-10)

#### Core Macros
- `generate_biis_event_id` - Event ID generation logic
- `format_remarks` - Remark formatting utilities

#### Stored Procedure Conversions
- `updt_erp2biis_cre8_remarks01` - Standard remarks creation
- `update_erp2biis_no900s01` - Non-900 series processing
- `erp2biis_cre8_remarks_900s01` - 900-series remarks
- `update_erp2biis_900sonly01` - 900-series only processing
- `updt_orig_cancelled_trans01` - Cancelled transaction handling

### Phase 5: Testing & Validation (Week 11-12)

#### Unit Tests
- Column-level tests (not_null, unique, relationships)
- Custom data tests for business rules

#### Integration Tests
- Record count validation
- Action code distribution validation
- BIIS Event ID validation
- Historical sync validation

#### Parallel Run
1. Run both Oracle and Snowflake pipelines simultaneously
2. Compare outputs daily for 2 weeks
3. Document and resolve any discrepancies

### Phase 6: Cutover (Week 13-14)

#### Pre-Cutover
1. Final data sync from Oracle to Snowflake
2. Freeze Oracle pipeline
3. Validate final record counts

#### Cutover Steps
1. Disable Oracle/Informatica jobs
2. Enable dbt scheduled runs
3. Configure email notifications
4. Update downstream system connections

#### Post-Cutover
1. Monitor first 5 production runs
2. Validate outputs against Oracle baseline
3. Address any issues immediately
4. Decommission Oracle pipeline after 30-day stability period

## Rollback Plan

If critical issues are discovered post-cutover:

1. Disable dbt scheduled runs
2. Re-enable Oracle/Informatica jobs
3. Sync any missing data from Snowflake to Oracle
4. Investigate and resolve issues
5. Re-attempt cutover after fixes

## Risk Mitigation

| Risk | Mitigation |
|------|------------|
| Data loss during migration | Full backup before migration, parallel run validation |
| Performance degradation | Warehouse sizing, query optimization, incremental processing |
| Business logic discrepancies | Comprehensive testing, parallel run comparison |
| Downstream system impact | Staged rollout, communication plan |

## Success Criteria

1. All record counts match between Oracle and Snowflake
2. BIIS Event IDs follow the same pattern
3. Historical tables contain complete data
4. Daily processing completes within SLA
5. No data quality issues reported for 30 days

## Timeline Summary

| Phase | Duration | Dates |
|-------|----------|-------|
| Infrastructure Setup | 2 weeks | Week 1-2 |
| Data Migration | 2 weeks | Week 3-4 |
| Model Development | 4 weeks | Week 5-8 |
| Macro Development | 2 weeks | Week 9-10 |
| Testing & Validation | 2 weeks | Week 11-12 |
| Cutover | 2 weeks | Week 13-14 |
| **Total** | **14 weeks** | |

## Contacts

- Project Lead: [TBD]
- Technical Lead: [TBD]
- DBA Support: peter.chen@hhs.gov, nathan.knight@hhs.gov
- Business Owner: marvin.simon@hhs.gov
