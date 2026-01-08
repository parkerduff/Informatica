{{
    config(
        materialized='view',
        schema='intermediate'
    )
}}

with base_data as (
    select * from {{ ref('int_sequence_generation') }}
),

employment_lookup as (
    select * from {{ source('ehrp', 'ps_gvt_employment') }}
),

pers_nid_lookup as (
    select * from {{ source('ehrp', 'ps_gvt_pers_nid') }}
),

pers_data_lookup as (
    select * from {{ source('ehrp', 'ps_gvt_pers_data') }}
),

award_data_lookup as (
    select * from {{ source('ehrp', 'ps_gvt_awd_data') }}
),

citizenship_lookup as (
    select * from {{ source('ehrp', 'ps_gvt_citizenship') }}
),

fill_pos_lookup as (
    select * from {{ source('ehrp', 'ps_he_fill_pos') }}
),

jpm_items_lookup as (
    select * from {{ source('ehrp', 'ps_jpm_jp_items') }}
),

ee_data_trk_lookup as (
    select * from {{ source('ehrp', 'ps_gvt_ee_data_trk') }}
),

with_employment as (
    select
        b.*,
        emp.hire_dt,
        emp.rehire_dt,
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
        emp.fte_tenure,
        emp.eg_group,
        emp.fte_flx_srvc,
        emp.contract_length,
        emp.appoint_end_dt
    from base_data b
    left join employment_lookup emp
        on b.emplid = emp.emplid
        and b.empl_rcd = emp.empl_rcd
        and b.effdt = emp.effdt
        and b.effseq = emp.effseq
),

with_ssn as (
    select
        we.*,
        nid.national_id as ssn
    from with_employment we
    left join pers_nid_lookup nid
        on we.emplid = nid.emplid
        and nid.country = 'USA'
        and nid.national_id_type = 'PR'
),

with_pers_data as (
    select
        ws.*,
        pd.last_name,
        pd.first_name,
        pd.middle_name,
        pd.address1,
        pd.city,
        pd.state,
        pd.postal,
        pd.geo_code,
        pd.sex,
        pd.birthdate,
        pd.military_status,
        pd.gvt_cred_mil_svce,
        pd.gvt_military_comp,
        pd.gvt_handicap_cd,
        pd.gvt_vet_pref_appt,
        pd.gvt_vet_pref,
        pd.ethnic_group
    from with_ssn ws
    left join pers_data_lookup pd
        on ws.emplid = pd.emplid
),

with_award_data as (
    select
        wpd.*,
        awd.gvt_award_type,
        awd.gvt_award_amt,
        awd.gvt_award_pct,
        awd.gvt_award_hours
    from with_pers_data wpd
    left join award_data_lookup awd
        on wpd.emplid = awd.emplid
        and wpd.empl_rcd = awd.empl_rcd
        and wpd.effdt = awd.effdt
),

with_citizenship as (
    select
        wad.*,
        cit.country as citizenship_country,
        cit.citizenship_status
    from with_award_data wad
    left join citizenship_lookup cit
        on wad.emplid = cit.emplid
        and cit.country = 'USA'
),

with_fill_pos as (
    select
        wc.*,
        fp.he_fill_cd
    from with_citizenship wc
    left join fill_pos_lookup fp
        on wc.position_nbr = fp.position_nbr
        and wc.effdt = fp.effdt
),

with_jpm_items as (
    select
        wfp.*,
        jpm.gvt_educ_level,
        jpm.gvt_instr_pgm_cd,
        jpm.gvt_yr_of_degree
    from with_fill_pos wfp
    left join jpm_items_lookup jpm
        on wfp.emplid = jpm.emplid
),

with_ee_data_trk as (
    select
        wji.*,
        edt.gvt_prev_ret_covg,
        edt.gvt_prev_ret_ded,
        edt.gvt_fegli_asof_dt,
        edt.gvt_fehb_asof_dt
    from with_jpm_items wji
    left join ee_data_trk_lookup edt
        on wji.emplid = edt.emplid
        and wji.empl_rcd = edt.empl_rcd
)

select * from with_ee_data_trk
