{{
    config(
        materialized='view'
    )
}}

with job_actions as (
    select
        j.emplid,
        j.empl_rcd,
        j.effdt,
        j.effseq,
        extract(year from j.effdt) as effdt_year
    from {{ ref('stg_ps_gvt_job') }} j
    inner join {{ ref('stg_nwk_new_ehrp_actions') }} a
        on j.emplid = a.emplid
        and j.empl_rcd = a.empl_rcd
        and j.effdt = a.effdt
        and j.effseq = a.effseq
),

sequence_lookup as (
    select
        ehrp_year,
        ehrp_seq_number
    from {{ source('nknight', 'sequence_num_tbl') }}
),

job_with_sequence as (
    select
        ja.*,
        coalesce(sl.ehrp_seq_number, 0) as base_seq_number
    from job_actions ja
    left join sequence_lookup sl
        on ja.effdt_year = sl.ehrp_year
),

final as (
    select
        emplid,
        empl_rcd,
        effdt,
        effseq,
        effdt_year,
        base_seq_number,
        {{ generate_event_id('effdt_year', 'base_seq_number') }} as event_id
    from job_with_sequence
)

select * from final
