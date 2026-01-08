/*
    Validation test: BIIS Event ID generation validation
    
    This test validates that:
    1. All event IDs are unique
    2. Event IDs follow the expected pattern (year * 1000000 + sequence)
    3. No gaps in sequence numbers within a year
    4. Event IDs are properly partitioned by year
*/

with event_id_analysis as (
    select
        event_id,
        floor(event_id / 1000000) as extracted_year,
        mod(event_id, 1000000) as extracted_sequence,
        date_part('year', eff_dte) as actual_year,
        load_date
    from {{ ref('fct_action_primary') }}
),

year_validation as (
    select
        event_id,
        extracted_year,
        actual_year,
        case 
            when extracted_year = actual_year then 'PASS'
            else 'FAIL'
        end as year_match_status
    from event_id_analysis
),

sequence_gaps as (
    select
        extracted_year,
        extracted_sequence,
        lag(extracted_sequence) over (
            partition by extracted_year 
            order by extracted_sequence
        ) as prev_sequence,
        extracted_sequence - lag(extracted_sequence) over (
            partition by extracted_year 
            order by extracted_sequence
        ) as sequence_gap
    from event_id_analysis
),

duplicate_check as (
    select
        event_id,
        count(*) as occurrence_count
    from {{ ref('fct_action_primary') }}
    group by event_id
    having count(*) > 1
)

select 'YEAR_MISMATCH' as validation_type, count(*) as issue_count
from year_validation
where year_match_status = 'FAIL'

union all

select 'SEQUENCE_GAP' as validation_type, count(*) as issue_count
from sequence_gaps
where sequence_gap > 1

union all

select 'DUPLICATE_EVENT_ID' as validation_type, count(*) as issue_count
from duplicate_check
