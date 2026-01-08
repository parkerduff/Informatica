{{
    config(
        materialized='table'
    )
}}

with enriched_data as (
    select * from {{ ref('int_employee_enrichment') }}
),

sequence_data as (
    select * from {{ ref('int_sequence_generation') }}
),

transformed as (
    select
        seq.event_id,
        
        e.he_reg_military as mil_lv_cur_fy_hrs,
        e.he_spc_military as mil_lv_emerg_cur_fy_hrs,
        
        e.sal_admin_plan as pay_table_num,
        
        e.gvt_rtnd_grade as retnd1_grade_cd,
        case 
            when e.gvt_rtnd_step = '0.0000000000000' then null 
            else e.gvt_rtnd_step 
        end as retnd1_step_cd,
        e.gvt_rtnd_pay_plan as retnd1_pay_plan_cd,
        
        e.he_pp_uded_amt as tsp_emp_pp_und_ded_amt,
        e.he_no_tsp_payper as tsp_emp_pp_und_ded_pymt_count,
        e.he_tspa_sub_yr as tsp_emp_prev_govt_contb_amt,
        e.he_emp_uded_amt as tsp_emp_und_ded_outstndg_amt,
        e.he_gvt_uded_amt as tsp_govt_und_ded_outstndg_amt,
        e.he_tltr_no as tsp_und_ded_ltr_num,
        e.he_uded_pay_cd as tsp_und_ded_option_cd,
        e.he_tsp_canc_cd as tsp_und_ded_stop_option_cd,
        e.he_tsp_vest_cd as tsp_vesting_cd,
        
        e.he_recruit_exp_dt as recruitment_exp_dte,
        e.he_relocate_exp_dt as relocation_exp_dte,
        {{ nullif_zero('e.he_recruit_bonus') }} as recruitment_bonus_amt,
        {{ nullif_zero('e.he_relocate_bonus') }} as relocation_bonus_amt,
        
        {{ nullif_zero('e.he_time_off_hrs') }} as time_off_granted_hrs,
        {{ nullif_zero('e.he_time_off_amt') }} as time_off_award_amt,
        
        e.gvt_awd_class as award_class_cd,
        e.oth_pay as award_amt,
        e.oth_hrs as award_hrs,
        e.goal_amt as award_goal_amt,
        e.earnings_end_dt as award_earnings_end_dte,
        e.gvt_use_by_date as award_use_by_dte,
        e.gvt_award_group as award_group_cd,
        e.gvt_tang_ben_amt as award_tang_ben_amt,
        e.gvt_intang_ben_amt as award_intang_ben_amt,
        e.gvt_suggestion_nbr as award_suggestion_nbr,
        e.gvt_oblig_expir_dt as award_oblig_expir_dte,
        
        e.gvt_sevpay_prv_wks as sevpay_prev_wks,
        e.gvt_mand_ret_dt as mandatory_retire_dte,
        e.gvt_intrm_days_wgi as interim_days_wgi,
        e.gvt_nonpay_noa as nonpay_noa_cd,
        e.gvt_nonpay_hrs_wgi as nonpay_hrs_wgi,
        e.gvt_nonpay_hrs_scd as nonpay_hrs_scd,
        e.gvt_nonpay_hrs_tnr as nonpay_hrs_tenure,
        e.gvt_nonpay_hrs_prb as nonpay_hrs_probation,
        
        e.gvt_temp_pro_expir as temp_promo_expir_dte,
        e.gvt_temp_psn_expir as temp_posn_expir_dte,
        e.gvt_detail_expires as detail_expir_dte,
        e.gvt_sabbatic_expir as sabbatic_expir_dte,
        
        e.gvt_cnv_begin_date as career_cnv_begin_dte,
        e.gvt_career_cnv_due as career_cnv_due_dte,
        e.gvt_career_cond_dt as career_cond_dte,
        e.gvt_appt_limit_dys as appt_limit_days,
        e.gvt_appt_limit_amt as appt_limit_amt,
        
        e.gvt_supv_prob_dt as supv_prob_dte,
        e.gvt_ses_prob_dt as ses_prob_dte,
        e.gvt_sec_clr_status as sec_clr_status_cd,
        e.gvt_clrnce_stat_dt as sec_clr_status_dte,
        
        e.gvt_ern_pgm_perm as ern_pgm_perm_cd,
        e.gvt_occ_sers_perm as occ_sers_perm_cd,
        e.gvt_grade_perm as grade_perm_cd,
        e.gvt_comp_area_perm as comp_area_perm_cd,
        e.gvt_comp_lvl_perm as comp_lvl_perm_cd,
        
        e.gvt_change_flag as change_flag_cd,
        e.gvt_spep as spep_cd,
        e.gvt_dt_lei as dt_lei_dte,
        e.gvt_fin_disclosure as fin_disclosure_cd,
        e.gvt_fin_discl_date as fin_disclosure_dte,
        
        e.gvt_detl_barg_unit as detail_barg_unit_cd,
        e.gvt_detl_union_cd as detail_union_cd,
        e.next_review_dt as next_review_dte,
        e.gvt_welfare_wk_cd as welfare_work_cd,
        e.tenure_accr_flg as tenure_accr_flag,
        
        e.he_fill_cd as fill_position_cd,
        
        e.jp_item_id as educ_level_cd,
        e.jp_item_value as instr_program_cd,
        
        {{ nullif_zero('e.he_awop_wigi') }} as awop_wigi_hrs,
        
        current_date() as load_date
        
    from enriched_data e
    inner join sequence_data seq
        on e.emplid = seq.emplid
        and e.empl_rcd = seq.empl_rcd
        and e.effdt = seq.effdt
        and e.effseq = seq.effseq
)

select * from transformed
