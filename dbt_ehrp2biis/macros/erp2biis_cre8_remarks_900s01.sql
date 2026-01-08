{% macro erp2biis_cre8_remarks_900s01() %}
{#
    Converts Oracle stored procedure HISTDBA.ERP2BIIS_CRE8_REMARKS_900s01 to dbt SQL.
    
    This procedure creates remarks specifically for 900-series actions.
    900-series actions are special personnel actions that require different
    processing logic than standard actions.
    
    Original Oracle procedure logic:
    - Selects records where event_id >= 9000000000
    - Creates remarks based on 900-series NOA code patterns
    - Inserts into nwk_action_remarks_tbl
#}

with action_data as (
    select
        event_id,
        noa_cd,
        noa2_cd,
        noa_suffix_cd,
        legal_auth1_cd,
        legal_auth1_desc,
        legal_auth2_cd,
        legal_auth2_desc,
        appt_type_cd,
        pay_plan_cd,
        grade_cd,
        step_cd,
        total_pay_amt,
        position_title_off,
        duty_station_cd,
        eff_dte
    from {{ ref('fct_action_primary') }}
    where load_date = current_date()
    and event_id >= 9000000000
),

remarks_900s as (
    select
        event_id,
        1 as remark_seq,
        'M90' as remark_cd,
        case
            when noa_cd in ('900', '901', '902', '903', '904', '905', '906', '907', '908', '909') then
                '900-SERIES ACTION: ' || noa_cd || ' - ' || coalesce(legal_auth1_desc, 'Correction/Cancellation')
            when noa_cd in ('910', '911', '912', '913', '914', '915', '916', '917', '918', '919') then
                'CORRECTION: ' || coalesce(legal_auth1_desc, 'Corrected Action')
            when noa_cd in ('920', '921', '922', '923', '924', '925', '926', '927', '928', '929') then
                'CANCELLATION: ' || coalesce(legal_auth1_desc, 'Cancelled Action')
            when noa_cd in ('930', '931', '932', '933', '934', '935', '936', '937', '938', '939') then
                'RETROACTIVE ACTION: ' || coalesce(legal_auth1_desc, 'Retroactive Change')
            when noa_cd in ('940', '941', '942', '943', '944', '945', '946', '947', '948', '949') then
                'DATA CORRECTION: ' || coalesce(legal_auth1_desc, 'Data Correction')
            when noa_cd in ('950', '951', '952', '953', '954', '955', '956', '957', '958', '959') then
                'ADMINISTRATIVE CORRECTION: ' || coalesce(legal_auth1_desc, 'Administrative Change')
            when noa_cd in ('960', '961', '962', '963', '964', '965', '966', '967', '968', '969') then
                'SYSTEM CORRECTION: ' || coalesce(legal_auth1_desc, 'System Generated')
            when noa_cd in ('970', '971', '972', '973', '974', '975', '976', '977', '978', '979') then
                'MASS CHANGE: ' || coalesce(legal_auth1_desc, 'Mass Update')
            when noa_cd in ('980', '981', '982', '983', '984', '985', '986', '987', '988', '989') then
                'SPECIAL ACTION: ' || coalesce(legal_auth1_desc, 'Special Processing')
            when noa_cd in ('990', '991', '992', '993', '994', '995', '996', '997', '998', '999') then
                'MISCELLANEOUS: ' || coalesce(legal_auth1_desc, 'Other Action')
            else
                '900-SERIES: ' || noa_cd || ' - ' || coalesce(legal_auth1_desc, 'No description')
        end as remark_text
    from action_data
    where noa_cd is not null
    and noa_cd like '9%'
)

select * from remarks_900s

{% endmacro %}
