{{
    config(
        materialized='table',
        schema='intermediate'
    )
}}

{#
    This model replicates the Informatica sequence generation logic from:
    - exp_GET_EFFDT_YEAR: Extracts year from effective date
    - lkp_OLD_SEQUENCE_NUMBER: Looks up last sequence number for the year
    - exp_MAIN2BIIS: Generates the event ID by incrementing the sequence
    
    The Informatica logic:
    1. v_curr_year = GET_DATE_PART(EFFDT, 'YYYY')
    2. Lookup EHRP_SEQ_NUMBER from SEQUENCE_NUM_TBL where EHRP_YEAR = v_curr_year
    3. v_EVENT_ID = IIF(v_CURRENT_YEAR = v_PREVIOUS_YEAR, v_EVENT_ID + 1, EHRP_SEQ_NUMBER + 1)
    
    In Snowflake, we use ROW_NUMBER() to generate sequential IDs within each year,
    starting from the last known sequence number for that year.
#}

with job_actions as (
    select
        j.emplid,
        j.empl_rcd,
        j.effdt,
        j.effseq,
        EXTRACT(YEAR FROM j.effdt) as effdt_year
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
    from {{ ref('sequence_num_tbl') }}
),

with_sequence as (
    select
        ja.emplid,
        ja.empl_rcd,
        ja.effdt,
        ja.effseq,
        ja.effdt_year,
        COALESCE(sl.ehrp_seq_number, 0) as base_seq_number,
        ROW_NUMBER() OVER (
            PARTITION BY ja.effdt_year
            ORDER BY ja.effdt, ja.emplid, ja.empl_rcd, ja.effseq
        ) as row_seq
    from job_actions ja
    left join sequence_lookup sl
        on ja.effdt_year = sl.ehrp_year
)

select
    emplid,
    empl_rcd,
    effdt,
    effseq,
    effdt_year,
    base_seq_number,
    row_seq,
    (effdt_year * 1000000 + base_seq_number + row_seq) as event_id
from with_sequence
