{{
    config(
        materialized='view',
        tags=['staging', 'ehrp2biis']
    )
}}

with source as (
    select * from {{ source('ehrp_raw', 'sequence_num_tbl') }}
),

staged as (
    select
        ehrp_year::number(4,0) as ehrp_year,
        ehrp_seq_number::number(10,0) as ehrp_seq_number,
        current_timestamp() as _loaded_at
    from source
)

select * from staged
