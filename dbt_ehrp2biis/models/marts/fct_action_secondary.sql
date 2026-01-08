{{
    config(
        materialized='table',
        schema='marts'
    )
}}

{#
    This model replicates the NWK_ACTION_SECONDARY_TBL target from the Informatica pipeline.
    It contains secondary action data including TSP, leave, and bonus details.
    
    The secondary table is linked to the primary table via EVENT_ID.
#}

with enriched_data as (
    select * from {{ ref('int_employee_enrichment') }}
),

transformed as (
    select
        -- Primary key (links to fct_action_primary)
        event_id,
        
        -- TSP (Thrift Savings Plan) fields
        he_tsp_elig_dt as tsp_elig_dte,
        he_tsp_contrib_pct as tsp_contrib_pct,
        he_tsp_contrib_amt as tsp_contrib_amt,
        he_tsp_catchup_amt as tsp_catchup_amt,
        he_tsp_catchup_pct as tsp_catchup_pct,
        he_tsp_roth_pct as tsp_roth_pct,
        he_tsp_roth_amt as tsp_roth_amt,
        he_tsp_roth_catchup_pct as tsp_roth_catchup_pct,
        he_tsp_roth_catchup_amt as tsp_roth_catchup_amt,
        null as tsp_status_cd,
        null as tsp_election_pct,
        null as tsp_catchup_election_pct,
        
        -- Retained grade/step fields
        gvt_rtnd_grade as retnd1_grade_cd,
        gvt_rtnd_step as retnd1_step_cd,
        gvt_rtnd_pay_plan as retnd1_pay_plan_cd,
        gvt_rtnd_occ_sers as retnd1_occ_series_cd,
        gvt_rtnd_pay_basis as retnd1_pay_basis_cd,
        gvt_rtnd_sal_rate as retnd1_salary_rate_amt,
        gvt_rtnd_loc_adj as retnd1_locality_adj_pct,
        gvt_rtnd_tot_sal as retnd1_total_salary_amt,
        null as retnd2_grade_cd,
        null as retnd2_step_cd,
        null as retnd2_pay_plan_cd,
        null as retnd2_occ_series_cd,
        null as retnd2_pay_basis_cd,
        null as retnd2_salary_rate_amt,
        null as retnd2_locality_adj_pct,
        null as retnd2_total_salary_amt,
        
        -- Award/bonus fields
        award_type_cd,
        cash_award_amt as award_amt,
        award_hours,
        award_pct,
        null as award_justification_txt,
        null as award_eff_dte,
        
        -- Leave balance fields (with zero-to-null conversion)
        case when he_al_balance = 0 then null else he_al_balance end as ann_lv_balance_hrs,
        case when he_al_carryover = 0 then null else he_al_carryover end as ann_lv_carryover_hrs,
        case when he_al_accrual = 0 then null else he_al_accrual end as ann_lv_accrual_hrs,
        case when he_al_total = 0 then null else he_al_total end as ann_lv_used_hrs,
        case when he_res_balance = 0 then null else he_res_balance end as ann_lv_restored_hrs,
        case when he_sl_balance = 0 then null else he_sl_balance end as sick_lv_balance_hrs,
        case when he_sl_carryover = 0 then null else he_sl_carryover end as sick_lv_carryover_hrs,
        case when he_sl_accrual = 0 then null else he_sl_accrual end as sick_lv_accrual_hrs,
        case when he_sl_total = 0 then null else he_sl_total end as sick_lv_used_hrs,
        
        -- AWOP (Absence Without Pay) fields
        case when he_awop_sep = 0 then null else he_awop_sep end as awop_sep_hrs,
        case when he_awop_wigi = 0 then null else he_awop_wigi end as awop_wigi_hrs,
        
        -- Position and job fields
        position_nbr as position_cd,
        null as position_prior_cd,
        jobcode as job_cd,
        null as job_prior_cd,
        deptid as dept_cd,
        null as dept_prior_cd,
        location as location_cd,
        null as location_prior_cd,
        
        -- Pay fields
        annual_rt as annual_salary_amt,
        hourly_rt as hourly_rate_amt,
        comprate as comp_rate_amt,
        gvt_locality_adj as locality_adj_pct,
        gvt_auo_pct as auo_pct,
        gvt_avail_pay_pct as availability_pay_pct,
        
        -- Grade and step
        grade as grade_cd,
        null as grade_prior_cd,
        step as step_cd,
        null as step_prior_cd,
        gvt_pay_plan as pay_plan_cd,
        null as pay_plan_prior_cd,
        gvt_occ_series as occ_series_cd,
        null as occ_series_prior_cd,
        
        -- Work schedule
        gvt_work_sched as work_schedule_cd,
        null as work_schedule_prior_cd,
        std_hours,
        std_hrs_frequency,
        
        -- Employment info
        emplid as emp_id,
        empl_rcd as empl_rec_no,
        effdt as eff_dte,
        effseq,
        action as ehrp_type_action,
        action_reason as ehrp_action_reason,
        gvt_wip_status,
        gvt_status_type,
        position_nbr as ehrp_position_number,
        null as ehrp_position_prior_number,
        gvt_corr_effseq as corr_cancl_effseq,
        
        -- Indicators
        null as title42_ind,
        null as title38_ind,
        company as opdiv,
        null as opdiv_prior,
        setid_dept as setid,
        null as curr_appt_auth1_cd,
        null as curr_appt_auth2_cd,
        gvt_leo_position as leo_position_cd,
        effdt as temp_gvt_effdt,
        null as reports_to,
        position_entry_dt,
        null as wgi_due_dt,
        null as ehrp_changed_wip_status_dt,
        
        -- Metadata
        'EHRP' as load_id,
        current_date() as load_date
        
    from enriched_data
)

select * from transformed
