{{
    config(
        materialized='view'
    )
}}

with base_job_data as (
    select
        j.*
    from {{ ref('stg_ps_gvt_job') }} j
    inner join {{ ref('stg_nwk_new_ehrp_actions') }} a
        on j.emplid = a.emplid
        and j.empl_rcd = a.empl_rcd
        and j.effdt = a.effdt
        and j.effseq = a.effseq
),

employment_lookup as (
    select
        emplid,
        empl_rcd,
        effdt,
        effseq,
        hire_dt,
        rehire_dt,
        cmpny_seniority_dt,
        service_dt,
        termination_dt,
        last_date_worked,
        last_increase_dt,
        probation_dt,
        gvt_scd_retire,
        gvt_scd_tsp,
        gvt_scd_leo,
        gvt_scd_sevpay,
        gvt_sevpay_prv_wks,
        gvt_mand_ret_dt,
        gvt_wgi_status,
        gvt_intrm_days_wgi,
        gvt_nonpay_noa,
        gvt_nonpay_hrs_wgi,
        gvt_nonpay_hrs_scd,
        gvt_nonpay_hrs_tnr,
        gvt_nonpay_hrs_prb,
        gvt_temp_pro_expir,
        gvt_temp_psn_expir,
        gvt_detail_expires,
        gvt_sabbatic_expir,
        gvt_rtnd_grade_beg,
        gvt_rtnd_grade_exp,
        gvt_noa_code as emp_gvt_noa_code,
        gvt_curr_apt_auth1,
        gvt_curr_apt_auth2,
        gvt_appt_expir_dt,
        gvt_cnv_begin_date,
        gvt_career_cnv_due,
        gvt_career_cond_dt,
        gvt_appt_limit_hrs,
        gvt_appt_limit_dys,
        gvt_appt_limit_amt,
        gvt_supv_prob_dt,
        gvt_ses_prob_dt,
        gvt_sec_clr_status,
        gvt_clrnce_stat_dt,
        gvt_ern_pgm_perm,
        gvt_occ_sers_perm,
        gvt_grade_perm,
        gvt_comp_area_perm,
        gvt_comp_lvl_perm,
        gvt_change_flag,
        gvt_spep,
        gvt_wgi_due_date,
        gvt_dt_lei,
        gvt_fin_disclosure,
        gvt_fin_discl_date,
        gvt_tenure,
        gvt_detl_barg_unit,
        gvt_detl_union_cd,
        next_review_dt,
        gvt_welfare_wk_cd,
        tenure_accr_flg
    from {{ source('ehrp', 'ps_gvt_employment') }}
),

pers_nid_lookup as (
    select
        emplid,
        empl_rcd,
        effdt,
        effseq,
        national_id,
        national_id_type,
        country as nid_country,
        primary_nid
    from {{ source('ehrp', 'ps_gvt_pers_nid') }}
),

pers_data_lookup as (
    select
        emplid,
        empl_rcd,
        effdt,
        effseq,
        first_name,
        last_name,
        middle_name,
        address1,
        city,
        state,
        postal,
        geo_code,
        birthdate,
        sex,
        mar_status,
        ethnic_group,
        veteran_status,
        highest_educ_lvl
    from {{ source('ehrp', 'ps_gvt_pers_data') }}
),

award_data_lookup as (
    select
        emplid,
        empl_rcd,
        effdt,
        effseq,
        gvt_awd_class,
        oth_pay,
        oth_hrs,
        goal_amt,
        earnings_end_dt,
        gvt_use_by_date,
        gvt_award_group,
        gvt_tang_ben_amt,
        gvt_intang_ben_amt,
        gvt_suggestion_nbr,
        gvt_oblig_expir_dt
    from {{ source('ehrp', 'ps_gvt_awd_data') }}
),

citizenship_lookup as (
    select
        emplid,
        empl_rcd,
        effdt,
        effseq,
        citizenship_status
    from {{ source('ehrp', 'ps_gvt_citizenship') }}
),

fill_pos_lookup as (
    select
        emplid,
        empl_rcd,
        effdt,
        effseq,
        he_fill_cd
    from {{ source('ehrp', 'ps_he_fill_pos') }}
),

jpm_items_lookup as (
    select
        emplid,
        empl_rcd,
        effdt,
        effseq,
        jp_item_id,
        jp_item_value,
        jp_item_descr
    from {{ source('ehrp', 'ps_jpm_jp_items') }}
),

ee_data_trk_lookup as (
    select distinct
        emplid,
        empl_rcd,
        effdt,
        effseq,
        gvt_date_wrk,
        gvt_wip_status as trk_gvt_wip_status
    from {{ source('ehrp', 'ps_gvt_ee_data_trk') }}
),

enriched as (
    select
        b.*,
        
        emp.hire_dt,
        emp.rehire_dt,
        emp.cmpny_seniority_dt,
        emp.service_dt,
        emp.termination_dt,
        emp.last_date_worked,
        emp.last_increase_dt,
        emp.probation_dt,
        emp.gvt_scd_retire,
        emp.gvt_scd_tsp,
        emp.gvt_scd_leo,
        emp.gvt_scd_sevpay,
        emp.gvt_sevpay_prv_wks,
        emp.gvt_mand_ret_dt,
        emp.gvt_wgi_status,
        emp.gvt_intrm_days_wgi,
        emp.gvt_nonpay_noa,
        emp.gvt_nonpay_hrs_wgi,
        emp.gvt_nonpay_hrs_scd,
        emp.gvt_nonpay_hrs_tnr,
        emp.gvt_nonpay_hrs_prb,
        emp.gvt_temp_pro_expir,
        emp.gvt_temp_psn_expir,
        emp.gvt_detail_expires,
        emp.gvt_sabbatic_expir,
        emp.gvt_rtnd_grade_beg,
        emp.gvt_rtnd_grade_exp,
        emp.emp_gvt_noa_code,
        emp.gvt_curr_apt_auth1,
        emp.gvt_curr_apt_auth2,
        emp.gvt_appt_expir_dt,
        emp.gvt_cnv_begin_date,
        emp.gvt_career_cnv_due,
        emp.gvt_career_cond_dt,
        emp.gvt_appt_limit_hrs,
        emp.gvt_appt_limit_dys,
        emp.gvt_appt_limit_amt,
        emp.gvt_supv_prob_dt,
        emp.gvt_ses_prob_dt,
        emp.gvt_sec_clr_status,
        emp.gvt_clrnce_stat_dt,
        emp.gvt_ern_pgm_perm,
        emp.gvt_occ_sers_perm,
        emp.gvt_grade_perm,
        emp.gvt_comp_area_perm,
        emp.gvt_comp_lvl_perm,
        emp.gvt_change_flag,
        emp.gvt_spep,
        emp.gvt_wgi_due_date,
        emp.gvt_dt_lei,
        emp.gvt_fin_disclosure,
        emp.gvt_fin_discl_date,
        emp.gvt_tenure,
        emp.gvt_detl_barg_unit,
        emp.gvt_detl_union_cd,
        emp.next_review_dt,
        emp.gvt_welfare_wk_cd,
        emp.tenure_accr_flg,
        
        nid.national_id as ssn,
        nid.national_id_type,
        nid.nid_country,
        nid.primary_nid,
        
        pd.first_name,
        pd.last_name,
        pd.middle_name,
        pd.address1,
        pd.city,
        pd.state,
        pd.postal,
        pd.geo_code,
        pd.birthdate,
        pd.sex,
        pd.mar_status,
        pd.ethnic_group,
        pd.veteran_status,
        pd.highest_educ_lvl,
        
        awd.gvt_awd_class,
        awd.oth_pay,
        awd.oth_hrs,
        awd.goal_amt,
        awd.earnings_end_dt,
        awd.gvt_use_by_date,
        awd.gvt_award_group,
        awd.gvt_tang_ben_amt,
        awd.gvt_intang_ben_amt,
        awd.gvt_suggestion_nbr,
        awd.gvt_oblig_expir_dt,
        
        cit.citizenship_status,
        
        fp.he_fill_cd,
        
        jpm.jp_item_id,
        jpm.jp_item_value,
        jpm.jp_item_descr,
        
        trk.gvt_date_wrk as event_submitted_dt,
        trk.trk_gvt_wip_status
        
    from base_job_data b
    
    left join employment_lookup emp
        on b.emplid = emp.emplid
        and b.empl_rcd = emp.empl_rcd
        and b.effdt = emp.effdt
        and b.effseq = emp.effseq
    
    left join pers_nid_lookup nid
        on b.emplid = nid.emplid
        and b.empl_rcd = nid.empl_rcd
        and b.effdt = nid.effdt
        and b.effseq = nid.effseq
    
    left join pers_data_lookup pd
        on b.emplid = pd.emplid
        and b.empl_rcd = pd.empl_rcd
        and b.effdt = pd.effdt
        and b.effseq = pd.effseq
    
    left join award_data_lookup awd
        on b.emplid = awd.emplid
        and b.empl_rcd = awd.empl_rcd
        and b.effdt = awd.effdt
        and b.effseq = awd.effseq
    
    left join citizenship_lookup cit
        on b.emplid = cit.emplid
        and b.empl_rcd = cit.empl_rcd
        and b.effdt = cit.effdt
        and b.effseq = cit.effseq
    
    left join fill_pos_lookup fp
        on b.emplid = fp.emplid
        and b.empl_rcd = fp.empl_rcd
        and b.effdt = fp.effdt
        and b.effseq = fp.effseq
    
    left join jpm_items_lookup jpm
        on b.emplid = jpm.emplid
        and b.empl_rcd = jpm.empl_rcd
        and b.effdt = jpm.effdt
        and b.effseq = jpm.effseq
    
    left join ee_data_trk_lookup trk
        on b.emplid = trk.emplid
        and b.empl_rcd = trk.empl_rcd
        and b.effdt = trk.effdt
        and b.effseq = trk.effseq
)

select * from enriched
