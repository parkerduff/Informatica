{{
    config(
        materialized='view'
    )
}}

with source_counts as (
    select
        'PS_GVT_JOB' as table_name,
        'source' as layer,
        count(*) as record_count
    from {{ source('ehrp', 'ps_gvt_job') }}
    
    union all
    
    select
        'NWK_NEW_EHRP_ACTIONS_TBL' as table_name,
        'source' as layer,
        count(*) as record_count
    from {{ source('nknight', 'nwk_new_ehrp_actions_tbl') }}
    
    union all
    
    select
        'PS_GVT_EMPLOYMENT' as table_name,
        'source' as layer,
        count(*) as record_count
    from {{ source('ehrp', 'ps_gvt_employment') }}
    
    union all
    
    select
        'PS_GVT_PERS_NID' as table_name,
        'source' as layer,
        count(*) as record_count
    from {{ source('ehrp', 'ps_gvt_pers_nid') }}
    
    union all
    
    select
        'PS_GVT_PERS_DATA' as table_name,
        'source' as layer,
        count(*) as record_count
    from {{ source('ehrp', 'ps_gvt_pers_data') }}
    
    union all
    
    select
        'PS_GVT_AWD_DATA' as table_name,
        'source' as layer,
        count(*) as record_count
    from {{ source('ehrp', 'ps_gvt_awd_data') }}
    
    union all
    
    select
        'PS_GVT_CITIZENSHIP' as table_name,
        'source' as layer,
        count(*) as record_count
    from {{ source('ehrp', 'ps_gvt_citizenship') }}
),

staging_counts as (
    select
        'stg_ps_gvt_job' as table_name,
        'staging' as layer,
        count(*) as record_count
    from {{ ref('stg_ps_gvt_job') }}
    
    union all
    
    select
        'stg_nwk_new_ehrp_actions' as table_name,
        'staging' as layer,
        count(*) as record_count
    from {{ ref('stg_nwk_new_ehrp_actions') }}
),

intermediate_counts as (
    select
        'int_employee_enrichment' as table_name,
        'intermediate' as layer,
        count(*) as record_count
    from {{ ref('int_employee_enrichment') }}
    
    union all
    
    select
        'int_sequence_generation' as table_name,
        'intermediate' as layer,
        count(*) as record_count
    from {{ ref('int_sequence_generation') }}
),

mart_counts as (
    select
        'fct_action_primary' as table_name,
        'marts' as layer,
        count(*) as record_count
    from {{ ref('fct_action_primary') }}
    
    union all
    
    select
        'fct_action_secondary' as table_name,
        'marts' as layer,
        count(*) as record_count
    from {{ ref('fct_action_secondary') }}
    
    union all
    
    select
        'fct_tracking' as table_name,
        'marts' as layer,
        count(*) as record_count
    from {{ ref('fct_tracking') }}
),

all_counts as (
    select * from source_counts
    union all
    select * from staging_counts
    union all
    select * from intermediate_counts
    union all
    select * from mart_counts
),

validation_summary as (
    select
        layer,
        table_name,
        record_count,
        current_timestamp() as report_generated_at
    from all_counts
),

cross_layer_validation as (
    select
        'Trigger Table to Staging' as validation_check,
        (select record_count from staging_counts where table_name = 'stg_nwk_new_ehrp_actions') as expected_count,
        (select record_count from intermediate_counts where table_name = 'int_employee_enrichment') as actual_count,
        case 
            when (select record_count from staging_counts where table_name = 'stg_nwk_new_ehrp_actions') = 
                 (select record_count from intermediate_counts where table_name = 'int_employee_enrichment')
            then 'PASS'
            else 'FAIL - Record count mismatch'
        end as validation_status
    
    union all
    
    select
        'Intermediate to Marts' as validation_check,
        (select record_count from intermediate_counts where table_name = 'int_sequence_generation') as expected_count,
        (select record_count from mart_counts where table_name = 'fct_action_primary') as actual_count,
        case 
            when (select record_count from intermediate_counts where table_name = 'int_sequence_generation') = 
                 (select record_count from mart_counts where table_name = 'fct_action_primary')
            then 'PASS'
            else 'FAIL - Record count mismatch'
        end as validation_status
    
    union all
    
    select
        'Primary to Secondary Alignment' as validation_check,
        (select record_count from mart_counts where table_name = 'fct_action_primary') as expected_count,
        (select record_count from mart_counts where table_name = 'fct_action_secondary') as actual_count,
        case 
            when (select record_count from mart_counts where table_name = 'fct_action_primary') = 
                 (select record_count from mart_counts where table_name = 'fct_action_secondary')
            then 'PASS'
            else 'FAIL - Record count mismatch'
        end as validation_status
    
    union all
    
    select
        'Primary to Tracking Alignment' as validation_check,
        (select record_count from mart_counts where table_name = 'fct_action_primary') as expected_count,
        (select record_count from mart_counts where table_name = 'fct_tracking') as actual_count,
        case 
            when (select record_count from mart_counts where table_name = 'fct_action_primary') = 
                 (select record_count from mart_counts where table_name = 'fct_tracking')
            then 'PASS'
            else 'FAIL - Record count mismatch'
        end as validation_status
)

select 
    'RECORD_COUNTS' as report_section,
    layer,
    table_name,
    record_count,
    null as expected_count,
    null as actual_count,
    null as validation_status,
    current_timestamp() as report_generated_at
from validation_summary

union all

select 
    'CROSS_LAYER_VALIDATION' as report_section,
    null as layer,
    validation_check as table_name,
    null as record_count,
    expected_count,
    actual_count,
    validation_status,
    current_timestamp() as report_generated_at
from cross_layer_validation

order by report_section, layer, table_name
