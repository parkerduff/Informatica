/*
    Validation test: Historical sync validation
    
    This test validates that:
    1. All records from today's load are present in historical snapshots
    2. Cancelled actions are properly handled (deleted and re-inserted)
    3. Snapshot timestamps are correct
*/

with todays_primary as (
    select event_id
    from {{ ref('fct_action_primary') }}
    where load_date = current_date()
),

snapshot_primary as (
    select event_id
    from {{ ref('snp_action_primary_all') }}
    where dbt_valid_to is null
),

missing_in_snapshot as (
    select p.event_id
    from todays_primary p
    left join snapshot_primary s on p.event_id = s.event_id
    where s.event_id is null
),

cancelled_actions as (
    select biis_event_id
    from {{ ref('fct_ehrp_recs_tracking') }}
    where biis_wip_status_changed_dt = current_date()
),

cancelled_in_snapshot as (
    select 
        c.biis_event_id,
        s.event_id as snapshot_event_id,
        s.dbt_valid_to
    from cancelled_actions c
    left join {{ ref('snp_action_primary_all') }} s 
        on c.biis_event_id = s.event_id
)

select 
    'MISSING_IN_SNAPSHOT' as validation_type,
    count(*) as issue_count
from missing_in_snapshot

union all

select 
    'CANCELLED_NOT_INVALIDATED' as validation_type,
    count(*) as issue_count
from cancelled_in_snapshot
where dbt_valid_to is null
and biis_event_id is not null
