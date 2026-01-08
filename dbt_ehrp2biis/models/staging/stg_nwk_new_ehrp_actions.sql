{{
    config(
        materialized='view',
        tags=['staging', 'ehrp2biis']
    )
}}

with source as (
    select * from {{ source('ehrp_raw', 'nwk_new_ehrp_actions_tbl') }}
),

staged as (
    select
        emplid,
        empl_rcd::number(38,0) as empl_rcd,
        effdt::date as effdt,
        effseq::number(38,0) as effseq,
        current_timestamp() as _loaded_at
    from source
)

select * from staged
