{% macro update_erp2biis_900sonly01() %}
{#
    Converts Oracle stored procedure HISTDBA.UPDATE_ERP2BIIS_900SONLY01_P to dbt SQL.
    
    This procedure processes 900-series only actions and applies specific
    formatting and business rules for these special action types.
    
    Original Oracle procedure logic:
    - Selects records where event_id >= 9000000000
    - Applies specific formatting rules for 900-series actions
    - Updates action records with corrected/cancelled information
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
        p.corr_cancl_effseq
    from {{ ref('fct_action_primary') }} p
    where p.load_date = current_date()
    and p.event_id >= 9000000000
),

formatted_900s as (
    select
        event_id,
        2 as remark_seq,
        'M91' as remark_cd,
        case
            when noa_cd in ('900', '910', '920') then
                'ORIGINAL ACTION: ' || coalesce(noa2_cd, 'Unknown') || 
                ' EFFECTIVE: ' || to_char(eff_dte, 'MM/DD/YYYY') ||
                ' EFFSEQ: ' || coalesce(corr_cancl_effseq::varchar, 'N/A')
            when noa_cd like '91%' then
                'CORRECTED TO: ' || pay_plan_cd || '-' || grade_cd || 
                ' Step ' || step_cd || ' $' || total_pay_amt::varchar
            when noa_cd like '92%' then
                'CANCELLED ACTION DETAILS: ' || 
                coalesce(position_title_off, '') || ' at ' || coalesce(duty_station_cd, '')
            when noa_cd like '93%' then
                'RETROACTIVE FROM: ' || to_char(eff_dte, 'MM/DD/YYYY') ||
                ' - ' || coalesce(legal_auth1_desc, '')
            else
                '900-SERIES DETAILS: ' || coalesce(legal_auth1_desc, 'No additional details')
        end as remark_text
    from action_data
    where noa_cd is not null
    and noa_cd like '9%'
)

select * from formatted_900s
where remark_text is not null

{% endmacro %}
