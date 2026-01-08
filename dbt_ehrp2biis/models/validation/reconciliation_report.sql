{{
    config(
        materialized='view',
        schema='validation'
    )
}}

with source_ps_gvt_job as (
    select count(*) as record_count
    from {{ source('ehrp', 'ps_gvt_job') }}
),

source_trigger_table as (
    select count(*) as record_count
    from {{ source('nknight', 'nwk_new_ehrp_actions_tbl') }}
),

staging_ps_gvt_job as (
    select count(*) as record_count
    from {{ ref('stg_ps_gvt_job') }}
),

staging_trigger_table as (
    select count(*) as record_count
    from {{ ref('stg_nwk_new_ehrp_actions') }}
),

intermediate_sequence as (
    select count(*) as record_count
    from {{ ref('int_sequence_generation') }}
),

intermediate_enrichment as (
    select count(*) as record_count
    from {{ ref('int_employee_enrichment') }}
),

mart_action_primary as (
    select count(*) as record_count
    from {{ ref('fct_action_primary') }}
),

mart_action_secondary as (
    select count(*) as record_count
    from {{ ref('fct_action_secondary') }}
),

mart_tracking as (
    select count(*) as record_count
    from {{ ref('fct_tracking') }}
),

reconciliation as (
    select 'source' as layer, 'ps_gvt_job' as model_name, record_count from source_ps_gvt_job
    union all
    select 'source' as layer, 'nwk_new_ehrp_actions_tbl' as model_name, record_count from source_trigger_table
    union all
    select 'staging' as layer, 'stg_ps_gvt_job' as model_name, record_count from staging_ps_gvt_job
    union all
    select 'staging' as layer, 'stg_nwk_new_ehrp_actions' as model_name, record_count from staging_trigger_table
    union all
    select 'intermediate' as layer, 'int_sequence_generation' as model_name, record_count from intermediate_sequence
    union all
    select 'intermediate' as layer, 'int_employee_enrichment' as model_name, record_count from intermediate_enrichment
    union all
    select 'mart' as layer, 'fct_action_primary' as model_name, record_count from mart_action_primary
    union all
    select 'mart' as layer, 'fct_action_secondary' as model_name, record_count from mart_action_secondary
    union all
    select 'mart' as layer, 'fct_tracking' as model_name, record_count from mart_tracking
),

with_validation as (
    select
        layer,
        model_name,
        record_count,
        case
            when layer = 'staging' and model_name = 'stg_ps_gvt_job' 
                then (select record_count from source_ps_gvt_job)
            when layer = 'staging' and model_name = 'stg_nwk_new_ehrp_actions' 
                then (select record_count from source_trigger_table)
            when layer = 'intermediate' 
                then (select record_count from staging_trigger_table)
            when layer = 'mart' 
                then (select record_count from intermediate_enrichment)
            else null
        end as expected_count,
        current_timestamp() as report_generated_at
    from reconciliation
)

select
    layer,
    model_name,
    record_count,
    expected_count,
    case 
        when expected_count is null then 'N/A'
        when record_count = expected_count then 'PASS'
        else 'FAIL'
    end as validation_status,
    case 
        when expected_count is null or expected_count = 0 then null
        else round((record_count - expected_count) * 100.0 / expected_count, 2)
    end as variance_pct,
    report_generated_at
from with_validation
order by 
    case layer
        when 'source' then 1
        when 'staging' then 2
        when 'intermediate' then 3
        when 'mart' then 4
    end,
    model_name
