{{
    config(
        materialized='table',
        schema='intermediate'
    )
}}

{#
    This model replicates the Informatica lookup transformations:
    - lkp_PS_GVT_EMPLOYMENT: Employment dates, tenure, probation info
    - lkp_PS_GVT_PERS_NID: Social Security Number
    - lkp_PS_GVT_PERS_DATA: Personal data (name, address, demographics)
    - lkp_PS_GVT_AWD_DATA: Award and bonus information
    - lkp_PS_GVT_CITIZENSHIP: Citizenship status
    - lkp_PS_HE_FILL_POS: Position filling codes
    - lkp_PS_JPM_JP_ITEMS: Education level and instructional program codes
    
    All lookups join on EMPLID, EMPL_RCD, EFFDT, EFFSEQ
#}

with base_job_data as (
    select
        j.*,
        seq.event_id
    from {{ ref('stg_ps_gvt_job') }} j
    inner join {{ ref('int_sequence_generation') }} seq
        on j.emplid = seq.emplid
        and j.empl_rcd = seq.empl_rcd
        and j.effdt = seq.effdt
        and j.effseq = seq.effseq
),

employment_lookup as (
    select
        emplid,
        empl_rcd,
        effdt,
        effseq,
        hire_dt,
        service_dt,
        last_increase_dt,
        probation_dt,
        gvt_tenure,
        gvt_wgi_status,
        gvt_appt_expir_dt,
        gvt_cnv_begin_date,
        gvt_temp_pro_expir,
        gvt_temp_psn_expir,
        gvt_detail_expires,
        gvt_supv_prob_dt,
        gvt_comp_lvl_perm,
        gvt_spep,
        gvt_appt_limit_dys
    from {{ source('ehrp', 'ps_gvt_employment') }}
),

pers_nid_lookup as (
    select
        emplid,
        national_id
    from {{ source('ehrp', 'ps_gvt_pers_nid') }}
    where country = 'USA'
    and national_id_type = 'PR'
    qualify row_number() over (partition by emplid order by effdt desc) = 1
),

pers_data_lookup as (
    select
        emplid,
        last_name,
        first_name,
        middle_name,
        address1,
        city,
        state,
        postal,
        geo_code,
        sex,
        birthdate,
        military_status,
        gvt_cred_mil_svce,
        gvt_military_comp
    from {{ source('ehrp', 'ps_gvt_pers_data') }}
    qualify row_number() over (partition by emplid order by effdt desc) = 1
),

award_data_lookup as (
    select
        emplid,
        empl_rcd,
        effdt,
        effseq,
        gvt_award_type,
        gvt_award_amount,
        gvt_award_hours,
        gvt_award_pct
    from {{ source('ehrp', 'ps_gvt_awd_data') }}
),

citizenship_lookup as (
    select
        emplid,
        country,
        citizenship_status
    from {{ source('ehrp', 'ps_gvt_citizenship') }}
    where country = 'USA'
    qualify row_number() over (partition by emplid order by effdt desc) = 1
),

fill_pos_lookup as (
    select
        emplid,
        empl_rcd,
        effdt,
        effseq,
        he_fill_position
    from {{ source('ehrp', 'ps_he_fill_pos') }}
),

jpm_items_lookup as (
    select
        emplid,
        jpm_cat_item_id as education_level_cd,
        major_code as instructional_program_cd
    from {{ source('ehrp', 'ps_jpm_jp_items') }}
    where jpm_cat_type = 'DEGREE'
    qualify row_number() over (partition by emplid order by effdt desc) = 1
),

enriched as (
    select
        b.*,
        
        -- Employment lookup fields
        emp.hire_dt as emp_eod_dte,
        emp.service_dt as lv_scd_dte,
        emp.last_increase_dt as awop_wgi_start_dte,
        emp.probation_dt,
        emp.gvt_tenure as tenure_cd,
        emp.gvt_wgi_status as wgi_status_cd,
        emp.gvt_appt_expir_dt as appt_nte_dte,
        emp.gvt_cnv_begin_date as career_start_dte,
        emp.gvt_temp_pro_expir as temp_promtn_exp_dte,
        emp.gvt_temp_psn_expir as position_change_end_dte,
        emp.gvt_detail_expires as suspension_end_dte,
        emp.gvt_supv_prob_dt as supervsry_mgrl_prob_start_dte,
        emp.gvt_comp_lvl_perm as competitive_level_cd,
        emp.gvt_spep as special_program_cd,
        emp.gvt_appt_limit_dys as appt_lmt_nte_90day_cd,
        
        -- Personal NID lookup (SSN)
        nid.national_id as ssn,
        
        -- Personal data lookup fields
        pd.last_name,
        pd.first_name,
        pd.middle_name,
        pd.address1 as emp_addr_line1_txt,
        pd.city as emp_city_nm,
        pd.state as emp_state_cd,
        pd.postal as emp_zip_cd,
        pd.geo_code,
        pd.sex as gender_cd,
        pd.birthdate as birth_dte,
        pd.military_status as military_status_cd,
        pd.gvt_cred_mil_svce as creditable_military_service_cd,
        pd.gvt_military_comp as military_comp_cd,
        
        -- Award data lookup fields
        awd.gvt_award_type as award_type_cd,
        awd.gvt_award_amount as cash_award_amt,
        awd.gvt_award_hours as award_hours,
        awd.gvt_award_pct as award_pct,
        
        -- Citizenship lookup fields
        cit.citizenship_status as us_citizenship_cd,
        
        -- Position filling lookup fields
        fp.he_fill_position as filling_position_cd,
        
        -- Education lookup fields
        jpm.education_level_cd,
        jpm.instructional_program_cd
        
    from base_job_data b
    left join employment_lookup emp
        on b.emplid = emp.emplid
        and b.empl_rcd = emp.empl_rcd
        and b.effdt = emp.effdt
        and b.effseq = emp.effseq
    left join pers_nid_lookup nid
        on b.emplid = nid.emplid
    left join pers_data_lookup pd
        on b.emplid = pd.emplid
    left join award_data_lookup awd
        on b.emplid = awd.emplid
        and b.empl_rcd = awd.empl_rcd
        and b.effdt = awd.effdt
        and b.effseq = awd.effseq
    left join citizenship_lookup cit
        on b.emplid = cit.emplid
    left join fill_pos_lookup fp
        on b.emplid = fp.emplid
        and b.empl_rcd = fp.empl_rcd
        and b.effdt = fp.effdt
        and b.effseq = fp.effseq
    left join jpm_items_lookup jpm
        on b.emplid = jpm.emplid
)

select * from enriched
