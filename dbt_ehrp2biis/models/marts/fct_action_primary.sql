{{
    config(
        materialized='table',
        schema='marts'
    )
}}

{#
    This model replicates the NWK_ACTION_PRIMARY_TBL target from the Informatica pipeline.
    It implements the field transformations from exp_MAIN2BIIS:
    
    Key transformations:
    1. AGCY_ASSIGN_CD = COMPANY || GVT_SUB_AGENCY (concatenate company and sub-agency)
    2. Leave balance fields: Convert zero values to NULL
    3. BASE_HRS = IIF(GVT_WORK_SCHED='I', STD_HOURS, STD_HOURS*2)
    4. AGCY_SUBELEMENT_PRIOR_CD = IIF(IS_SPACES(GVT_XFER_FROM_AGCY) or ISNULL(GVT_XFER_FROM_AGCY), NULL, GVT_XFER_FROM_AGCY || '00')
    5. LEG_AUTH_TXT concatenations
#}

with enriched_data as (
    select * from {{ ref('int_employee_enrichment') }}
),

transformed as (
    select
        -- Primary key
        event_id,
        
        -- Agency codes (Informatica: o_AGCY_ASSIGN_CD = COMPANY || GVT_SUB_AGENCY)
        company || gvt_sub_agency as agcy_assign_cd,
        deptid as agcy_subelement_cd,
        case 
            when gvt_xfer_from_agcy is null or trim(gvt_xfer_from_agcy) = '' 
            then null 
            else gvt_xfer_from_agcy || '00' 
        end as agcy_subelement_prior_cd,
        
        -- Annual leave fields (Informatica: IIF(field=0, NULL, field))
        he_al_45day_ceil as ann_lv_45day_ceil_cd,
        he_al_category as ann_lv_catgry_cd,
        case when he_al_red_cred = 0 then null else he_al_red_cred end as ann_lv_crdt_reductn_hrs,
        case when he_al_balance = 0 then null else he_al_balance end as ann_lv_cur_bal_hrs,
        case when he_lump_hrs = 0 then null else he_lump_hrs end as ann_lv_lump_sum_paid_hrs,
        case when he_al_carryover = 0 then null else he_al_carryover end as ann_lv_prior_year_bal_hrs,
        case when he_res_balance = 0 then null else he_res_balance end as ann_lv_restored_bal_lv_hrs,
        case when he_res_lastyr = 0 then null else he_res_lastyr end as ann_lv_restored_bal1_hrs,
        case when he_res_twoyrs = 0 then null else he_res_twoyrs end as ann_lv_restored_bal2_hrs,
        case when he_res_threeyrs = 0 then null else he_res_threeyrs end as ann_lv_restored_bal3_hrs,
        he_al_xfer_in as ann_lv_tranfr_in_bal_hrs,
        case when he_al_accrual = 0 then null else he_al_accrual end as ann_lv_ytd_accrd_hrs,
        case when he_al_total = 0 then null else he_al_total end as ann_lv_ytd_used_hrs,
        
        -- Salary and compensation
        annual_rt as ann_salary_rate_amt,
        gvt_ann_ind as annuitant_ind_cd,
        null as appointing_office_change_cd,
        appt_lmt_nte_90day_cd,
        null as appt_lmt_nte_90day_dte,
        null as appt_lmt_nte_hrs,
        appt_nte_dte,
        empl_status as appt_status_cd,
        gvt_type_of_appt as appt_type_cd,
        gvt_auo_pct as auo_pay_pct,
        gvt_auo_pct_prior as auo_pay_prior_pct,
        null as authentication_dte,
        gvt_avail_pay_pct as availability_pay_pct,
        awop_wgi_start_dte,
        case when he_awop_sep = 0 then null else he_awop_sep end as awop_ytd_hrs,
        
        -- Bargaining unit and base hours
        barg_unit as bargaining_unit_cd,
        -- Informatica: IIF(GVT_WORK_SCHED='I', STD_HOURS, STD_HOURS*2)
        case 
            when gvt_work_sched = 'I' then std_hours 
            else std_hours * 2 
        end as base_hrs,
        
        -- Personal data
        birth_dte,
        acct_cd as can_cd,
        null as can_new_cd,
        career_start_dte,
        cash_award_amt,
        null as cash_award_bnft_amt,
        null as ceil_reporting_cd,
        null as chrty_area_cd,
        null as chrty_ded_amt,
        null as city_tax_cd,
        null as city_tax_ded_amt,
        null as city_tax_ded_pct,
        null as city_tax_martl_status_cd,
        null as city_tax_resid_cd,
        null as city_tax_tot_exempt_count,
        null as cnty_tax_cd,
        null as cnty_tax_ded_pct,
        null as cnty_tax_martl_status_cd,
        null as cnty_tax_resid_cd,
        null as cnty_tax_tot_exempt_count,
        competitive_level_cd,
        null as computer_position_cd,
        gvt_corr_effdt as corr_cancl_eff_dte,
        gvt_corr_leg_auth as corr_cancl_legal_auth_cd,
        gvt_corr_auth_desc as corr_cancl_legal_auth_txt,
        gvt_corr_noa_code as corr_cancl_noa_cd,
        creditable_military_service_cd,
        null as csrs_frozen_service_cd,
        null as cybersecurity_cd,
        null as dept_id,
        null as disability_cd,
        null as drug_test_cd,
        education_level_cd,
        emp_addr_line1_txt,
        emp_city_nm,
        emp_eod_dte,
        emplid as emp_id,
        last_name as emp_last_nm,
        first_name as emp_first_nm,
        middle_name as emp_middle_nm,
        emp_state_cd,
        emp_zip_cd,
        null as ethnicity_cd,
        effdt as eff_dte,
        null as fegli_cd,
        gvt_fegli as fegli_code_cd,
        gvt_fegli_living as fegli_living_benefit_cd,
        gvt_living_amt as fegli_living_benefit_amt,
        gvt_fers_coverage as fers_coverage_cd,
        filling_position_cd,
        null as financial_disclosure_cd,
        flsa_status as flsa_status_cd,
        gvt_csrs_frozn_svc as frozen_service_cd,
        gender_cd,
        geo_code,
        grade,
        grade_entry_dt as grade_entry_dte,
        null as handicap_cd,
        hourly_rt as hourly_rate_amt,
        instructional_program_cd,
        gvt_leg_auth_1 as leg_auth_cd_1,
        gvt_leg_auth_2 as leg_auth_cd_2,
        -- Informatica: UPPER(GVT_PAR_AUTH_D1 || RTRIM(GVT_PAR_AUTH_D1_2))
        upper(coalesce(gvt_par_auth_d1, '') || rtrim(coalesce(gvt_par_auth_d1_2, ''))) as leg_auth_txt_1,
        upper(coalesce(gvt_par_auth_d2, '') || rtrim(coalesce(gvt_par_auth_d2_2, ''))) as leg_auth_txt_2,
        gvt_locality_adj as locality_adj_pct,
        location as location_cd,
        lv_scd_dte,
        military_status_cd,
        gvt_noa_code as noa_cd,
        gvt_noa_suffix as noa_suffix_cd,
        gvt_occ_series as occ_series_cd,
        gvt_pay_basis as pay_basis_cd,
        gvt_pay_plan as pay_plan_cd,
        gvt_poi as poi_cd,
        position_nbr as position_cd,
        position_change_end_dte,
        position_entry_dt as position_entry_dte,
        gvt_posn_sens_cd as position_sensitivity_cd,
        probation_dt as probation_dte,
        null as race_cd,
        gvt_rtnd_grade as retained_grade_cd,
        gvt_rtnd_loc_adj as retained_locality_adj_pct,
        gvt_rtnd_occ_sers as retained_occ_series_cd,
        gvt_rtnd_pay_basis as retained_pay_basis_cd,
        gvt_rtnd_pay_plan as retained_pay_plan_cd,
        gvt_rtnd_sal_rate as retained_salary_rate_amt,
        gvt_rtnd_step as retained_step_cd,
        gvt_rtnd_tot_sal as retained_total_salary_amt,
        null as retirement_plan_cd,
        null as service_comp_dte,
        
        -- Sick leave fields
        case when he_sl_balance = 0 then null else he_sl_balance end as sick_lv_cur_bal_hrs,
        null as sick_lv_fers_elect_bal_hrs,
        case when he_sl_carryover = 0 then null else he_sl_carryover end as sick_lv_prior_year_bal_hrs,
        he_sl_xfer_in as sick_lv_tranfr_in_bal_hrs,
        case when he_sl_accrual = 0 then null else he_sl_accrual end as sick_lv_ytd_accrd_hrs,
        case when he_sl_total = 0 then null else he_sl_total end as sick_lv_ytd_used_hrs,
        
        special_program_cd,
        ssn,
        null as st_tax_cd,
        null as st_tax_martl_status_cd,
        null as st_tax_non_resid_cd,
        null as st_tax_opt_ded_amt,
        null as st_tax_tot_exempt_count,
        step as step_cd,
        null as step_prior_cd,
        supervsry_mgrl_prob_start_dte,
        gvt_super_status as supervsry_status_cd,
        suspension_end_dte,
        temp_promtn_exp_dte,
        tenure_cd,
        null as terminal_site_cd,
        null as terminal_site_diffrnt_cd,
        null as timekeeper_num,
        null as tstg_designtd_position_cd,
        null as tstg_designtd_position_eff_dte,
        null as union_cd,
        null as union_ded_amt,
        null as union_ded_type_cd,
        us_citizenship_cd,
        null as veterans_preference_cd,
        military_status_cd as veterans_status_cd,
        null as vlntry_sep_incentive_pymt_cd,
        null as wage_board_shift2_amt,
        null as wage_board_shift3_amt,
        null as wgi_start_dte,
        wgi_status_cd,
        gvt_work_sched as work_schedule_cd,
        null as work_schedule_change_cd,
        null as work_schedule_prior_cd,
        null as year_degree_attained_dte,
        null as as_of_day,
        'EHRP' as load_id,
        current_date() as load_date
        
    from enriched_data
)

select * from transformed
