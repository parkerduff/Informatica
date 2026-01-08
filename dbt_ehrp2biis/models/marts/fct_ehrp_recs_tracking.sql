{{
    config(
        materialized='incremental',
        unique_key='biis_event_id',
        tags=['marts', 'ehrp2biis']
    )
}}

with event_data as (
    select * from {{ ref('int_biis_event_generation') }}
),

tracking_records as (
    select
        emplid,
        empl_rcd,
        effdt,
        effseq,
        gvt_effdt as event_submitted_dt,
        gvt_wip_status,
        gvt_noa_code as noa_cd,
        gvt_noac_suffix as noa_suffix_cd,
        biis_event_id,
        current_date() as load_date,
        null::date as changed_wip_status,
        null::date as biis_wip_status_changed_dt
    from event_data
)

select * from tracking_records

{% if is_incremental() %}
where load_date > (select max(load_date) from {{ this }})
{% endif %}
