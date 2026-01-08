/*
    Validation test: Record count comparison between Oracle and Snowflake
    
    This test validates that the total number of records in the Snowflake
    staging tables matches the expected counts from the Oracle implementation.
    
    Usage:
    - Set the expected_count variable based on Oracle baseline
    - Run: dbt test --select validate_record_counts
*/

{% set expected_primary_count = var('expected_primary_count', 0) %}
{% set expected_secondary_count = var('expected_secondary_count', 0) %}
{% set expected_tracking_count = var('expected_tracking_count', 0) %}

with primary_count as (
    select count(*) as cnt from {{ ref('fct_action_primary') }}
),

secondary_count as (
    select count(*) as cnt from {{ ref('fct_action_secondary') }}
),

tracking_count as (
    select count(*) as cnt from {{ ref('fct_ehrp_recs_tracking') }}
),

validation as (
    select
        'fct_action_primary' as table_name,
        p.cnt as snowflake_count,
        {{ expected_primary_count }} as oracle_count,
        case 
            when {{ expected_primary_count }} = 0 then 'SKIP'
            when p.cnt = {{ expected_primary_count }} then 'PASS'
            else 'FAIL'
        end as validation_status
    from primary_count p
    
    union all
    
    select
        'fct_action_secondary' as table_name,
        s.cnt as snowflake_count,
        {{ expected_secondary_count }} as oracle_count,
        case 
            when {{ expected_secondary_count }} = 0 then 'SKIP'
            when s.cnt = {{ expected_secondary_count }} then 'PASS'
            else 'FAIL'
        end as validation_status
    from secondary_count s
    
    union all
    
    select
        'fct_ehrp_recs_tracking' as table_name,
        t.cnt as snowflake_count,
        {{ expected_tracking_count }} as oracle_count,
        case 
            when {{ expected_tracking_count }} = 0 then 'SKIP'
            when t.cnt = {{ expected_tracking_count }} then 'PASS'
            else 'FAIL'
        end as validation_status
    from tracking_count t
)

select *
from validation
where validation_status = 'FAIL'
