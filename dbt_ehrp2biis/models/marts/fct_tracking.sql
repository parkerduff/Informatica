{{
    config(
        materialized='table'
    )
}}

with enriched_data as (
    select * from {{ ref('int_employee_enrichment') }}
),

sequence_data as (
    select * from {{ ref('int_sequence_generation') }}
),

transformed as (
    select
        e.emplid,
        e.empl_rcd,
        e.effdt,
        e.effseq,
        e.event_submitted_dt,
        e.gvt_wip_status,
        e.gvt_noa_code as noa_cd,
        null as noa_suffix_cd,
        seq.event_id as biis_event_id,
        current_date() as load_date
        
    from enriched_data e
    inner join sequence_data seq
        on e.emplid = seq.emplid
        and e.empl_rcd = seq.empl_rcd
        and e.effdt = seq.effdt
        and e.effseq = seq.effseq
)

select * from transformed
