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
        event_id,
        he_reg_military as mil_lv_cur_fy_hrs,
        he_spc_military as mil_lv_emerg_cur_fy_hrs,
        sal_admin_plan as pay_table_num,
        gvt_rtnd_grade as retnd1_grade_cd,
        case 
            when gvt_rtnd_step = '0.0000000000000' then null 
            else gvt_rtnd_step 
        end as retnd1_step_cd,
        gvt_rtnd_pay_plan as retnd1_pay_plan_cd,
        he_pp_uded_amt as tsp_emp_pp_und_ded_amt,
        he_no_tsp_payper as tsp_emp_pp_und_ded_pymt_count,
        he_tspa_sub_yr as tsp_emp_prev_govt_contb_amt,
        he_emp_uded_amt as tsp_emp_und_ded_outstndg_amt,
        he_gvt_uded_amt as tsp_govt_und_ded_outstndg_amt,
        he_tltr_no as tsp_und_ded_ltr_num,
        he_uded_pay_cd as tsp_und_ded_option_cd,
        he_tsp_canc_cd as tsp_und_ded_stop_option_cd,
        he_tsp_vesting_cd as tsp_vesting_cd,
        he_recruitment_exp_dt as recruitment_exp_dte,
        he_relocation_exp_dt as relocation_exp_dte,
        he_recruitment_bonus as recruitment_bonus_amt,
        he_relocation_bonus as relocation_bonus_amt,
        he_time_off_award as time_off_award_amt,
        he_time_off_hrs as time_off_granted_hrs,
        gvt_award_type as award_type_cd,
        gvt_award_amt as award_amt,
        gvt_award_pct as award_pct,
        gvt_award_hours as award_hours,
        gvt_prev_ret_covg as prev_retirement_coverage_cd,
        gvt_prev_ret_ded as prev_retirement_ded_amt,
        gvt_fegli_asof_dt as fegli_asof_dte,
        gvt_fehb_asof_dt as fehb_asof_dte,
        he_fill_cd as position_fill_cd,
        gvt_ern_pgm_perm as ern_pgm_perm_cd,
        gvt_occ_sers_perm as occ_sers_perm_cd,
        gvt_grade_perm as grade_perm_cd,
        gvt_comp_area_perm as comp_area_perm_cd,
        gvt_comp_lvl_perm as comp_lvl_perm_cd,
        gvt_change_flag as change_flag,
        gvt_spep as spep_cd,
        gvt_dt_lei as dt_lei,
        gvt_detl_barg_unit as detl_barg_unit_cd,
        gvt_detl_union_cd as detl_union_cd,
        next_review_dt as next_review_dte,
        gvt_welfare_wk_cd as welfare_wk_cd,
        tenure_accr_flg as tenure_accr_flag,
        fte_tenure,
        eg_group,
        fte_flx_srvc,
        contract_length,
        appoint_end_dt as appoint_end_dte,
        gvt_sevpay_prv_wks as sevpay_prv_wks,
        gvt_intrm_days_wgi as intrm_days_wgi,
        gvt_nonpay_noa as nonpay_noa_cd,
        gvt_nonpay_hrs_wgi as nonpay_hrs_wgi,
        gvt_nonpay_hrs_scd as nonpay_hrs_scd,
        gvt_nonpay_hrs_tnr as nonpay_hrs_tnr,
        gvt_nonpay_hrs_prb as nonpay_hrs_prb,
        gvt_temp_pro_expir as temp_pro_expir_dte,
        gvt_temp_psn_expir as temp_psn_expir_dte,
        gvt_detail_expires as detail_expires_dte,
        gvt_sabbatic_expir as sabbatic_expir_dte,
        current_date() as load_date
    from enriched_data
)

select * from transformed
