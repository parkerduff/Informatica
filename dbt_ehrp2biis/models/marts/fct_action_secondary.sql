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

action_secondary as (
    select
        biis_event_id as event_id,
        
        null::varchar(1) as adtnl_opt_life_ins_cd,
        null::varchar(2) as allot_cd,
        null::number(8,2) as allot_ded_amt,
        null::date as allot_eff_dte,
        null::varchar(17) as allot_emp_acct_num,
        null::varchar(2) as allot_emp_acct_type_cd,
        null::varchar(60) as allot_fincl_instn_city_st_name,
        null::varchar(43) as allot_fincl_instn_name,
        null::varchar(10) as allot_fincl_instn_num,
        null::varchar(12) as allot_fincl_instn_postal_cd,
        null::varchar(40) as allot_fincl_instn_street_name,
        
        null::varchar(2) as bond_coben_cd,
        null::varchar(120) as bond_coben_name,
        null::varchar(20) as bond_coben_ssn,
        null::varchar(15) as bond_denom_cd,
        null::varchar(15) as bond_inscription_num,
        null::number(7,2) as bond_note_tot_ded_amt,
        null::varchar(30) as bond_owner_city_name,
        null::varchar(120) as bond_owner_name,
        null::varchar(12) as bond_owner_postal_cd,
        null::varchar(20) as bond_owner_ssn,
        null::varchar(30) as bond_owner_st_name,
        null::varchar(40) as bond_owner_street_name,
        null::varchar(1) as bond_refund_ind,
        
        null::number(7,2) as buyout_amt,
        null::date as buyout_eff_dte,
        null::number(1,0) as claim_type_cd,
        null::number(8,2) as cola_pct,
        null::varchar(1) as court_order_applctn_cd,
        
        null::varchar(2) as csa_cd,
        null::number(8,2) as csa_ded_amt,
        null::number(8,2) as csa_ded_pct,
        null::varchar(120) as csa_recipient_name,
        null::varchar(1) as csrs_admin_fee_cd,
        
        null::number(8,2) as envirn_dif_pct,
        null::number(1,0) as family_life_ins_cd,
        null::number(5,0) as forgn_lang_award_amt,
        
        null::varchar(1) as garnishment_cd,
        null::number(8,2) as garnishment_ded_amt,
        null::number(8,2) as garnishment_ded_pct,
        null::varchar(120) as garnishment_recipient_name,
        
        null::number(8,2) as hazard_pay_pct,
        null::varchar(1) as health_benefits_cd,
        null::varchar(1) as health_benefits_prior_cd,
        
        null::number(8,2) as incentive_award_amt,
        null::date as incentive_award_dte,
        null::varchar(3) as incentive_award_noa_cd,
        
        gvt_leg_auth_1 as legal_auth1_cd,
        gvt_par_auth_d1 as legal_auth1_desc,
        gvt_par_auth_d1_2 as legal_auth1_desc2,
        gvt_leg_auth_2 as legal_auth2_cd,
        gvt_par_auth_d2 as legal_auth2_desc,
        gvt_par_auth_d2_2 as legal_auth2_desc2,
        
        null::varchar(1) as life_ins_cd,
        null::varchar(1) as life_ins_prior_cd,
        
        null::number(6,2) as mil_lv_cur_fy_hrs,
        null::number(6,2) as mil_lv_prior_fy_hrs,
        
        null::varchar(1) as opt_life_ins_cd,
        null::varchar(1) as opt_life_ins_prior_cd,
        
        null::number(8,2) as perf_award_amt,
        null::date as perf_award_dte,
        null::varchar(3) as perf_award_noa_cd,
        
        null::number(8,2) as recruitment_bonus_amt,
        null::date as recruitment_bonus_dte,
        null::number(8,2) as relocation_bonus_amt,
        null::date as relocation_bonus_dte,
        null::number(8,2) as retention_bonus_amt,
        null::date as retention_bonus_dte,
        
        gvt_rtnd_grade as retnd1_grade_cd,
        gvt_rtnd_pay_plan as retnd1_pay_plan_cd,
        case 
            when gvt_rtnd_step = '0.0000000000000' then null
            else gvt_rtnd_step
        end as retnd1_step_cd,
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
        
        null::number(6,2) as sick_lv_cur_bal_hrs,
        null::number(6,2) as sick_lv_prior_year_bal_hrs,
        null::number(6,2) as sick_lv_ytd_accrd_hrs,
        null::number(6,2) as sick_lv_ytd_used_hrs,
        
        null::number(6,2) as time_off_award_amt,
        null::number(6,2) as time_off_granted_hrs,
        
        null::varchar(1) as tsp_cd,
        null::number(8,2) as tsp_contrib_pct,
        null::number(8,2) as tsp_ded_amt,
        null::varchar(1) as tsp_prior_cd,
        
        gvt_unif_svc_mon as unif_svc_mon,
        gvt_mil_status as mil_status_cd,
        gvt_veterans_pref as veterans_pref_cd,
        gvt_veterans_pref_rif as veterans_pref_rif_cd,
        
        gvt_award_type as award_type_cd,
        gvt_award_amount as award_amt,
        gvt_award_pct as award_pct,
        gvt_award_hours as award_hrs,
        gvt_award_date as award_dte,
        gvt_award_noa as award_noa_cd,
        
        paygroup,
        null::date as leo_scd_dt,
        null::date as retention_bonus_end_date,
        
        current_timestamp() as _loaded_at
        
    from event_data
)

select * from action_secondary

{% if is_incremental() %}
where event_id not in (select event_id from {{ this }})
{% endif %}
