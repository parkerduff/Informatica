{{
    config(
        materialized='ephemeral',
        tags=['intermediate', 'ehrp2biis']
    )
}}

with new_actions as (
    select * from {{ ref('stg_nwk_new_ehrp_actions') }}
),

job_data as (
    select * from {{ ref('stg_ps_gvt_job') }}
),

joined as (
    select
        j.*,
        date_part('year', j.effdt)::integer as effdt_year
    from new_actions a
    inner join job_data j
        on a.emplid = j.emplid
        and a.empl_rcd = j.empl_rcd
        and a.effdt = j.effdt
        and a.effseq = j.effseq
)

select * from joined
order by effdt, emplid, empl_rcd, effseq
