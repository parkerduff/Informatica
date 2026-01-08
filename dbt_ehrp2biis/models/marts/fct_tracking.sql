{{
    config(
        materialized='table',
        schema='marts'
    )
}}

with enriched_data as (
    select * from {{ ref('int_employee_enrichment') }}
),

transformed as (
    select
        emplid,
        empl_rcd,
        effdt,
        effseq,
        gvt_wip_record_dt as event_submitted_dt,
        gvt_wip_status,
        gvt_noa_code as noa_cd,
        gvt_noa_suffix as noa_suffix_cd,
        event_id as biis_event_id,
        current_date() as load_date
    from enriched_data
)

select * from transformed
