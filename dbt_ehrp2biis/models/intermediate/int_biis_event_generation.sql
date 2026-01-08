{{
    config(
        materialized='ephemeral',
        tags=['intermediate', 'ehrp2biis']
    )
}}

with job_actions as (
    select * from {{ ref('int_ehrp_job_actions') }}
),

sequence_numbers as (
    select * from {{ ref('stg_sequence_num_tbl') }}
),

actions_with_sequence as (
    select
        ja.*,
        coalesce(sn.ehrp_seq_number, 0) as base_sequence_number
    from job_actions ja
    left join sequence_numbers sn
        on ja.effdt_year = sn.ehrp_year
),

actions_with_event_id as (
    select
        *,
        {{ generate_biis_event_id('effdt_year', 'base_sequence_number') }} as biis_event_id
    from actions_with_sequence
)

select * from actions_with_event_id
