{{
    config(
        materialized='incremental',
        unique_key='event_id',
        tags=['marts', 'ehrp2biis']
    )
}}

with event_data as (
    select * from {{ ref('int_biis_event_generation') }}
),

action_primary as (
    select
        biis_event_id as event_id,
        
        gvt_agency_code as agcy_assign_cd,
        gvt_sub_agency as agcy_subelement_cd,
        gvt_sub_agency_prior as agcy_subelement_prior_cd,
        
        null::varchar(1) as ann_lv_45day_ceil_cd,
        null::number(1,0) as ann_lv_catgry_cd,
        null::number(5,2) as ann_lv_crdt_reductn_hrs,
        null::number(6,0) as ann_lv_cur_bal_hrs,
        null::number(4,0) as ann_lv_lump_sum_paid_hrs,
        null::number(6,2) as ann_lv_prior_year_bal_hrs,
        null::number(6,2) as ann_lv_restored_bal_lv_hrs,
        null::number(6,2) as ann_lv_restored_bal1_hrs,
        null::number(6,2) as ann_lv_restored_bal2_hrs,
        null::number(6,2) as ann_lv_restored_bal3_hrs,
        null::number(4,0) as ann_lv_tranfr_in_bal_hrs,
        null::number(5,2) as ann_lv_ytd_accrd_hrs,
        null::number(5,2) as ann_lv_ytd_used_hrs,
        
        annual_rt as ann_salary_rate_amt,
        null::varchar(1) as annuitant_ind_cd,
        null::varchar(3) as appointing_office_change_cd,
        null::varchar(1) as appt_lmt_nte_90day_cd,
        null::date as appt_lmt_nte_90day_dte,
        null::number(10,0) as appt_lmt_nte_hrs,
        gvt_appt_limit_dt as appt_nte_dte,
        null::varchar(1) as appt_status_cd,
        gvt_appt_type as appt_type_cd,
        
        null::number(15,4) as auo_pay_pct,
        null::number(15,4) as auo_pay_prior_pct,
        null::date as authentication_dte,
        null::number(15,4) as availability_pay_pct,
        null::date as awop_wgi_start_dte,
        null::number(6,2) as awop_ytd_hrs,
        
        gvt_bargain_unit as bargaining_unit_cd,
        std_hours as base_hrs,
        null::date as birth_dte,
        
        acct_cd as can_cd,
        null::varchar(15) as can_new_cd,
        null::varchar(1) as citizenship_cd,
        null::varchar(1) as citizenship_prior_cd,
        
        deptid as cost_center_cd,
        null::varchar(10) as cost_center_prior_cd,
        
        gvt_duty_station as duty_station_cd,
        gvt_duty_station_prior as duty_station_prior_cd,
        
        effdt as eff_dte,
        effseq,
        emplid as emp_id,
        empl_rcd as empl_rec_no,
        
        gvt_fegli as fegli_cd,
        gvt_fegli_prior as fegli_prior_cd,
        gvt_fehb as fehb_cd,
        gvt_fehb_prior as fehb_prior_cd,
        gvt_fers_coverage as fers_coverage_cd,
        gvt_fers_coverage_prior as fers_coverage_prior_cd,
        
        gvt_flsa_status as flsa_cd,
        gvt_flsa_status_prior as flsa_prior_cd,
        
        gvt_grade as grade_cd,
        gvt_grade_prior as grade_prior_cd,
        grade_entry_dt as grade_entry_dte,
        
        hourly_rt as hourly_rate_amt,
        
        location as location_cd,
        null::varchar(10) as location_prior_cd,
        
        gvt_locality_adj as locality_adj_amt,
        gvt_locality_adj_prior as locality_adj_prior_amt,
        
        gvt_noa_code as noa_cd,
        gvt_noa_code2 as noa2_cd,
        gvt_noac_suffix as noa_suffix_cd,
        gvt_noac_suffix2 as noa2_suffix_cd,
        
        gvt_occ_series as occ_series_cd,
        gvt_occ_series_prior as occ_series_prior_cd,
        
        gvt_pay_basis as pay_basis_cd,
        gvt_pay_basis_prior as pay_basis_prior_cd,
        gvt_pay_plan as pay_plan_cd,
        gvt_pay_plan_prior as pay_plan_prior_cd,
        paygroup,
        
        gvt_personnel_office as personnel_office_cd,
        gvt_personnel_off_prior as personnel_office_prior_cd,
        gvt_poi as poi_cd,
        gvt_poi_prior as poi_prior_cd,
        
        position_nbr as position_cd,
        null::varchar(8) as position_prior_cd,
        position_entry_dt as position_entry_dte,
        gvt_posn_title_cd as position_title_cd,
        gvt_posn_title_cd_prior as position_title_prior_cd,
        gvt_posn_title_off as position_title_off,
        gvt_posn_title_off_pr as position_title_off_prior,
        
        gvt_retirement_plan as retirement_plan_cd,
        gvt_retire_plan_prior as retirement_plan_prior_cd,
        
        gvt_rtnd_grade as retnd1_grade_cd,
        gvt_rtnd_pay_plan as retnd1_pay_plan_cd,
        gvt_rtnd_step as retnd1_step_cd,
        gvt_rtnd_comprate as retnd1_basic_pay_amt,
        gvt_rtnd_loc_adj as retnd1_locality_adj_amt,
        gvt_rtnd_adj_base as retnd1_adj_basic_pay_amt,
        gvt_rtnd_tot_salary as retnd1_total_pay_amt,
        gvt_rtnd_pay_basis as retnd1_pay_basis_cd,
        gvt_rtnd_sched as retnd1_work_sched_cd,
        gvt_rtnd_pt_hrs as retnd1_part_time_hrs,
        gvt_rtnd_occ_series as retnd1_occ_series_cd,
        gvt_rtnd_posn_title as retnd1_position_title_cd,
        gvt_rtnd_posn_off as retnd1_position_title_off,
        gvt_rtnd_exp_dt as retnd1_expiration_dte,
        gvt_rtnd_wgi_dt as retnd1_wgi_due_dte,
        gvt_rtnd_wgi_status as retnd1_wgi_status_cd,
        gvt_rtnd_legal_auth as retnd1_legal_auth_cd,
        
        gvt_scd_leave as scd_leave_dte,
        gvt_scd_retire as scd_retire_dte,
        gvt_scd_rif as scd_rif_dte,
        gvt_scd_ses as scd_ses_dte,
        gvt_scd_spcl_retire as scd_spcl_retire_dte,
        gvt_scd_tsp as scd_tsp_dte,
        gvt_scd_wgi as scd_wgi_dte,
        
        gvt_step as step_cd,
        gvt_step_prior as step_prior_cd,
        step_entry_dt as step_entry_dte,
        
        gvt_supv_status as supv_status_cd,
        gvt_supv_status_prior as supv_status_prior_cd,
        
        gvt_tenure as tenure_cd,
        gvt_tenure_prior as tenure_prior_cd,
        
        gvt_adj_base_pay as total_pay_amt,
        gvt_adj_base_pay_prior as total_pay_prior_amt,
        gvt_total_salary as total_salary_amt,
        gvt_total_salary_prior as total_salary_prior_amt,
        
        gvt_veterans_pref as veterans_pref_cd,
        gvt_veterans_pref_rif as veterans_pref_rif_cd,
        
        gvt_wgi_due_dt as wgi_due_dte,
        gvt_wgi_status as wgi_status_cd,
        
        gvt_work_sched as work_sched_cd,
        gvt_work_sched_prior as work_sched_prior_cd,
        gvt_part_time_hrs as part_time_hrs,
        gvt_part_time_hrs_prior as part_time_hrs_prior,
        
        action as ehrp_type_action,
        action_reason as ehrp_action_reason,
        gvt_wip_status,
        gvt_status_type,
        position_nbr as ehrp_position_number,
        null::varchar(8) as ehrp_position_prior_number,
        
        null::number(3,0) as corr_cancl_effseq,
        null::varchar(1) as title42_ind,
        null::varchar(1) as title38_ind,
        
        business_unit as opdiv,
        null::varchar(8) as opdiv_prior,
        setid_dept as setid,
        
        gvt_leg_auth_1 as curr_appt_auth1_cd,
        gvt_leg_auth_2 as curr_appt_auth2_cd,
        null::varchar(1) as leo_position_cd,
        
        gvt_effdt as temp_gvt_effdt,
        null::varchar(8) as reports_to,
        position_entry_dt as position_entry_dt,
        gvt_wgi_due_dt as wgi_due_dt,
        
        null::date as ehrp_changed_wip_status_dt,
        null::date as biis_changed_wip_status_dt,
        
        'EHRP2BIIS' as load_id,
        current_date() as load_date,
        emplid as new_ssn,
        null::varchar(9) as old_ssn,
        
        effdt_year as as_of_day,
        
        current_timestamp() as _loaded_at
        
    from event_data
)

select * from action_primary

{% if is_incremental() %}
where load_date > (select max(load_date) from {{ this }})
{% endif %}
