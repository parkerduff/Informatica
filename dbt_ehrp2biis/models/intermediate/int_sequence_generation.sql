{{
    config(
        materialized='view',
        schema='intermediate'
    )
}}

with job_data as (
    select * from {{ ref('stg_ps_gvt_job') }}
),

trigger_table as (
    select * from {{ ref('stg_nwk_new_ehrp_actions') }}
),

sequence_lookup as (
    select * from {{ source('nknight', 'sequence_num_tbl') }}
),

joined_source as (
    select
        j.*,
        t.emplid as trigger_emplid
    from job_data j
    inner join trigger_table t
        on j.emplid = t.emplid
        and j.empl_rcd = t.empl_rcd
        and j.effdt = t.effdt
        and j.effseq = t.effseq
),

with_sequence as (
    select
        js.*,
        coalesce(seq.ehrp_seq_number, 0) as base_sequence_number
    from joined_source js
    left join sequence_lookup seq
        on js.effdt_year = seq.ehrp_year
),

with_event_id as (
    select
        *,
        {{ generate_event_id('effdt_year', 'base_sequence_number') }} as event_id
    from with_sequence
)

select * from with_event_id
