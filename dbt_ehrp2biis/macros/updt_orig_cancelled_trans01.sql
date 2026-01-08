{% macro updt_orig_cancelled_trans01() %}
{#
    Converts Oracle stored procedure HISTDBA.UPDT_ORIG_CANCELLED_TRANS01_P to dbt SQL.
    
    This procedure updates original cancelled transactions by:
    1. Identifying records where WIP status has changed
    2. Deleting old versions from historical tables
    3. Re-inserting current versions with updated status
    
    In dbt, this is handled through incremental models and the snapshot
    strategy for historical tracking.
    
    Original Oracle procedure logic:
    - Identifies cancelled actions via biis_wip_status_changed_dt = trunc(sysdate)
    - Deletes from action_primary_all, action_secondary_all, action_remarks_all
    - Re-inserts current versions from nwk_action_* tables
#}

with cancelled_events as (
    select distinct biis_event_id
    from {{ ref('fct_ehrp_recs_tracking') }}
    where biis_wip_status_changed_dt = current_date()
),

current_primary as (
    select p.*
    from {{ ref('fct_action_primary') }} p
    inner join cancelled_events c
        on p.event_id = c.biis_event_id
),

current_secondary as (
    select s.*
    from {{ ref('fct_action_secondary') }} s
    inner join cancelled_events c
        on s.event_id = c.biis_event_id
),

current_remarks as (
    select r.*
    from {{ ref('fct_action_remarks') }} r
    inner join cancelled_events c
        on r.event_id = c.biis_event_id
)

select 
    'primary' as record_type,
    event_id,
    current_timestamp() as updated_at
from current_primary

union all

select 
    'secondary' as record_type,
    event_id,
    current_timestamp() as updated_at
from current_secondary

union all

select 
    'remarks' as record_type,
    event_id,
    current_timestamp() as updated_at
from current_remarks

{% endmacro %}


{% macro check_wip_status_changes() %}
{#
    Checks for WIP status changes between EHRP source and BIIS tracking table.
    
    This replicates the logic from chk_ehrp2biis_wip_status_p procedure.
#}

with tracking_records as (
    select
        t.biis_event_id,
        t.emplid,
        t.empl_rcd,
        t.effdt,
        t.effseq,
        t.gvt_wip_status as old_status,
        j.gvt_wip_status as new_status
    from {{ ref('fct_ehrp_recs_tracking') }} t
    inner join {{ ref('stg_ps_gvt_job') }} j
        on t.emplid = j.emplid
        and t.empl_rcd = j.empl_rcd
        and t.effdt = j.effdt
        and t.effseq = j.effseq
    where t.gvt_wip_status != j.gvt_wip_status
    and t.changed_wip_status is null
)

select * from tracking_records
order by effdt, biis_event_id

{% endmacro %}
