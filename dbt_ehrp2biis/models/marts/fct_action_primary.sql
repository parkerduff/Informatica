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
        
        {{ create_agency_assignment_code('e.company', 'e.gvt_sub_agency') }} as agcy_assign_cd,
        e.deptid as agcy_subelement_cd,
        {{ create_prior_agency_code('e.gvt_xfer_from_agcy') }} as agcy_subelement_prior_cd,
        
        null as ann_lv_45day_ceil_cd,
        null as ann_lv_catgry_cd,
        {{ nullif_zero('e.he_al_red_cred') }} as ann_lv_crdt_reductn_hrs,
        {{ nullif_zero('e.he_al_balance') }} as ann_lv_cur_bal_hrs,
        {{ nullif_zero('e.he_lump_hrs') }} as ann_lv_lump_sum_paid_hrs,
        {{ nullif_zero('e.he_al_carryover') }} as ann_lv_prior_year_bal_hrs,
        {{ nullif_zero('e.he_res_balance') }} as ann_lv_restored_bal_lv_hrs,
        {{ nullif_zero('e.he_res_lastyr') }} as ann_lv_restored_bal1_hrs,
        {{ nullif_zero('e.he_res_twoyrs') }} as ann_lv_restored_bal2_hrs,
        {{ nullif_zero('e.he_res_threeyrs') }} as ann_lv_restored_bal3_hrs,
        null as ann_lv_tranfr_in_bal_hrs,
        {{ nullif_zero('e.he_al_accrual') }} as ann_lv_ytd_accrd_hrs,
        {{ nullif_zero('e.he_al_total') }} as ann_lv_ytd_used_hrs,
        
        e.annual_rt as ann_salary_rate_amt,
        e.gvt_ann_ind as annuitant_ind_cd,
        null as appointing_office_change_cd,
        null as appt_lmt_nte_90day_cd,
        null as appt_lmt_nte_90day_dte,
        e.gvt_appt_limit_hrs as appt_lmt_nte_hrs,
        e.gvt_appt_expir_dt as appt_nte_dte,
        null as appt_status_cd,
        e.gvt_type_of_appt as appt_type_cd,
        
        null as auo_pay_pct,
        null as auo_pay_prior_pct,
        e.event_submitted_dt as authentication_dte,
        null as availability_pay_pct,
        null as awop_wgi_start_dte,
        {{ nullif_zero('e.he_awop_sep') }} as awop_ytd_hrs,
        
        e.barg_unit as bargaining_unit_cd,
        {{ calculate_base_hours('e.gvt_work_sched', 'e.std_hours') }} as base_hrs,
        e.birthdate as birth_dte,
        
        e.acct_cd as can_cd,
        null as can_new_cd,
        e.citizenship_status as citizenship_cd,
        e.city as city_nm,
        null as comp_time_bal_hrs,
        null as comp_time_ytd_earned_hrs,
        null as comp_time_ytd_used_hrs,
        null as credit_hrs_bal_hrs,
        null as credit_hrs_ytd_earned_hrs,
        null as credit_hrs_ytd_used_hrs,
        
        e.gvt_csrs_frozn_svc as csrs_frozen_svc_cd,
        e.gvt_curr_apt_auth1 as curr_appt_auth1_cd,
        e.gvt_curr_apt_auth2 as curr_appt_auth2_cd,
        
        e.effdt as eff_dte,
        e.emplid,
        e.empl_rcd,
        e.empl_status as empl_status_cd,
        e.empl_type as empl_type_cd,
        
        e.ethnic_group as ethnicity_cd,
        e.gvt_fegli as fegli_cd,
        e.gvt_fegli_living as fegli_living_cd,
        e.gvt_living_amt as fegli_living_amt,
        e.gvt_fers_coverage as fers_coverage_cd,
        e.first_name as first_nm,
        e.flsa_status as flsa_status_cd,
        e.full_part_time as full_part_time_cd,
        
        e.geo_code as geo_cd,
        e.grade as grade_cd,
        e.grade_entry_dt as grade_entry_dte,
        e.hire_dt as hire_dte,
        e.hourly_rt as hourly_rate_amt,
        
        e.last_name as last_nm,
        e.gvt_leg_auth_1 as leg_auth_cd_1,
        upper(coalesce(e.gvt_par_auth_d1, '') || coalesce(rtrim(e.gvt_par_auth_d1_2), '')) as leg_auth_txt_1,
        e.gvt_leg_auth_2 as leg_auth_cd_2,
        upper(coalesce(e.gvt_par_auth_d2, '') || coalesce(rtrim(e.gvt_par_auth_d2_2), '')) as leg_auth_txt_2,
        e.location as loc_cd,
        e.gvt_locality_adj as locality_adj_pct,
        
        e.middle_name as middle_nm,
        e.gvt_noa_code as noa_cd,
        e.gvt_noa_2nd_code as noa_2nd_cd,
        null as noa_suffix_cd,
        
        e.gvt_occ_series as occ_series_cd,
        e.gvt_pay_basis as pay_basis_cd,
        e.gvt_pay_plan as pay_plan_cd,
        e.gvt_personnel_office as personnel_office_id,
        e.gvt_poi as poi_cd,
        e.position_nbr as position_nbr,
        e.postal as postal_cd,
        e.probation_dt as probation_dte,
        
        e.reg_temp as reg_temp_cd,
        e.rehire_dt as rehire_dte,
        e.gvt_rtnd_grade_beg as retained_grade_begin_dte,
        e.gvt_rtnd_grade_exp as retained_grade_exp_dte,
        e.gvt_rtnd_grade as retained_grade_cd,
        e.gvt_rtnd_pay_plan as retained_pay_plan_cd,
        e.gvt_rtnd_step as retained_step_cd,
        
        e.service_dt as scd_dte,
        e.gvt_scd_leo as scd_leo_dte,
        e.gvt_scd_retire as scd_retire_dte,
        e.gvt_scd_sevpay as scd_sevpay_dte,
        e.gvt_scd_tsp as scd_tsp_dte,
        e.sex as sex_cd,
        {{ nullif_zero('e.he_sl_balance') }} as sick_lv_cur_bal_hrs,
        {{ nullif_zero('e.he_sl_carryover') }} as sick_lv_prior_year_bal_hrs,
        {{ nullif_zero('e.he_sl_accrual') }} as sick_lv_ytd_accrd_hrs,
        {{ nullif_zero('e.he_sl_total') }} as sick_lv_ytd_used_hrs,
        e.ssn,
        e.state as state_cd,
        e.step as step_cd,
        e.step_entry_dt as step_entry_dte,
        e.gvt_sub_agency as sub_agency_cd,
        e.gvt_super_status as supervisory_status_cd,
        
        e.gvt_tenure as tenure_cd,
        e.termination_dt as termination_dte,
        
        e.veteran_status as veterans_pref_cd,
        null as veterans_status_cd,
        
        e.gvt_wgi_due_date as wgi_due_dte,
        e.gvt_wgi_status as wgi_status_cd,
        e.gvt_wip_status as wip_status_cd,
        e.gvt_work_sched as work_schedule_cd,
        
        e.address1 as address_line_1,
        e.annl_benef_base_rt as annl_benef_base_rt,
        e.company,
        e.deptid,
        e.effseq,
        e.jobcode,
        
        current_date() as load_date
        
    from enriched_data e
    inner join sequence_data seq
        on e.emplid = seq.emplid
        and e.empl_rcd = seq.empl_rcd
        and e.effdt = seq.effdt
        and e.effseq = seq.effseq
)

select * from transformed
