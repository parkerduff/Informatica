{{
    config(
        materialized='view',
        schema='staging'
    )
}}

with source as (
    select * from {{ source('nknight', 'nwk_new_ehrp_actions_tbl') }}
),

staged as (
    select
        emplid,
        empl_rcd,
        effdt,
        effseq
    from source
)

select * from staged
