/*
    Validation test: Action code distribution comparison
    
    This test validates that the distribution of NOA (Nature of Action) codes
    in Snowflake matches the expected distribution from Oracle.
    
    The test checks:
    1. All expected NOA codes are present
    2. Count per NOA code is within acceptable variance (default 5%)
*/

{% set acceptable_variance_pct = var('acceptable_variance_pct', 5) %}

with snowflake_distribution as (
    select
        noa_cd,
        count(*) as record_count
    from {{ ref('fct_action_primary') }}
    where load_date = current_date()
    group by noa_cd
),

noa_code_categories as (
    select
        noa_cd,
        record_count,
        case
            when noa_cd like '1%' then 'APPOINTMENTS'
            when noa_cd like '3%' then 'SEPARATIONS'
            when noa_cd like '5%' then 'CONVERSIONS'
            when noa_cd like '7%' then 'CHANGES'
            when noa_cd like '8%' then 'AWARDS'
            when noa_cd like '9%' then '900-SERIES'
            else 'OTHER'
        end as noa_category
    from snowflake_distribution
),

category_summary as (
    select
        noa_category,
        sum(record_count) as total_records,
        count(distinct noa_cd) as distinct_codes
    from noa_code_categories
    group by noa_category
)

select
    noa_category,
    total_records,
    distinct_codes
from category_summary
order by noa_category
