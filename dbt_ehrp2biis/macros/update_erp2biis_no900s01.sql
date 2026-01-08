{% macro update_erp2biis_no900s01() %}
{#
    Converts Oracle stored procedure HISTDBA.UPDATE_ERP2BIIS_NO900S01_p to dbt SQL.
    
    This procedure processes non-900 series actions and creates additional
    remarks based on specific business rules.
    
    Original Oracle procedure logic:
    - Selects records where event_id < 9000000000
    - Applies specific formatting rules for non-900 series actions
    - Updates remarks with additional information
#}

with action_data as (
    select
        p.event_id,
        p.noa_cd,
        p.noa2_cd,
        p.legal_auth1_cd,
        p.legal_auth1_desc,
        p.legal_auth2_cd,
        p.legal_auth2_desc,
        p.appt_type_cd,
        p.pay_plan_cd,
        p.pay_plan_prior_cd,
        p.grade_cd,
        p.grade_prior_cd,
        p.step_cd,
        p.step_prior_cd,
        p.total_pay_amt,
        p.total_pay_prior_amt,
        p.position_title_off,
        p.position_title_off_prior,
        p.duty_station_cd,
        p.duty_station_prior_cd,
        p.eff_dte,
        s.retnd1_grade_cd,
        s.retnd1_pay_plan_cd,
        s.retnd1_step_cd
    from {{ ref('fct_action_primary') }} p
    left join {{ ref('fct_action_secondary') }} s
        on p.event_id = s.event_id
    where p.load_date = current_date()
    and p.event_id < 9000000000
),

additional_remarks as (
    select
        event_id,
        2 as remark_seq,
        'M02' as remark_cd,
        case
            when noa_cd like '7%' and pay_plan_prior_cd is not null then
                'FROM: ' || pay_plan_prior_cd || '-' || grade_prior_cd || ' Step ' || step_prior_cd || ' $' || total_pay_prior_amt::varchar
            when noa_cd like '5%' then
                'CONVERTED FROM: ' || coalesce(position_title_off_prior, 'Previous Position')
            when noa_cd like '3%' then
                'SEPARATED FROM: ' || coalesce(position_title_off, '') || ' at ' || coalesce(duty_station_cd, '')
            when retnd1_grade_cd is not null then
                'RETAINED GRADE: ' || retnd1_pay_plan_cd || '-' || retnd1_grade_cd || ' Step ' || retnd1_step_cd
            else
                null
        end as remark_text
    from action_data
    where noa_cd is not null
)

select * from additional_remarks
where remark_text is not null

{% endmacro %}
