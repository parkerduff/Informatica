{{
    config(
        materialized='table',
        schema='marts'
    )
}}

{#
    This model replicates the EHRP_RECS_TRACKING_TBL target from the Informatica pipeline.
    It tracks processed EHRP records with their event IDs for audit and reconciliation purposes.
    
    Target fields:
    - EMPLID, EMPL_RCD, EFFDT, EFFSEQ: Key fields identifying the source record
    - EVENT_SUBMITTED_DT: Date the event was submitted in EHRP
    - GVT_WIP_STATUS: Work-in-progress status from EHRP
    - NOA_CD, NOA_SUFFIX_CD: Nature of Action codes
    - BIIS_EVENT_ID: Generated event ID for BIIS
    - LOAD_DATE: Date the record was loaded
#}

with enriched_data as (
    select * from {{ ref('int_employee_enrichment') }}
),

tracking as (
    select
        emplid,
        empl_rcd,
        effdt,
        effseq,
        gvt_event_submit_dt as event_submitted_dt,
        gvt_wip_status,
        gvt_noa_code as noa_cd,
        gvt_noa_suffix as noa_suffix_cd,
        event_id as biis_event_id,
        current_date() as load_date
    from enriched_data
)

select * from tracking
