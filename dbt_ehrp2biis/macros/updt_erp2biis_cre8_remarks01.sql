{% macro updt_erp2biis_cre8_remarks01() %}
{#
    Converts Oracle stored procedure HISTDBA.UPDT_ERP2BIIS_CRE8_REMARKS01_P to dbt SQL.
    
    This procedure creates remarks for standard personnel actions (non-900 series).
    It generates remarks based on the nature of action (NOA) code and other
    action attributes.
    
    Original Oracle procedure logic:
    - Selects records from nwk_action_primary_tbl where load_date = trunc(sysdate)
    - Creates remarks based on NOA code patterns
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
    and event_id < 9000000000
),

remarks_generated as (
    select
        event_id,
        1 as remark_seq,
        'M01' as remark_cd,
        case
            when noa_cd in ('100', '101', '108', '115', '117', '120', '122', '124', '130', '140', '141', '142', '143', '145', '146', '147', '148', '149', '150', '151', '153', '154', '155', '156', '157', '159', '170', '171', '190', '198', '199') then
                'APPOINTMENT: ' || coalesce(position_title_off, '') || ' at ' || coalesce(duty_station_cd, '')
            when noa_cd in ('300', '301', '302', '303', '304', '312', '317', '330', '350', '351', '352', '353', '355', '356', '357', '385') then
                'SEPARATION: ' || coalesce(legal_auth1_desc, '')
            when noa_cd in ('500', '501', '502', '503', '504', '508', '510', '511', '514', '515', '516', '517', '518', '519', '520', '521', '522', '523', '524', '525', '527', '528', '529', '530', '531', '535', '536', '537', '538', '539', '540', '541', '542', '543', '544', '545', '546', '548', '549', '550', '551', '552', '553', '555', '556', '557', '558', '559', '560', '561', '562', '563', '564', '565', '566', '567', '568', '569', '570', '571', '572', '573', '574', '575', '576', '577', '578', '579', '580', '581', '582', '583', '584', '585', '590', '591', '592', '593', '594', '595', '596', '597', '598', '599') then
                'CONVERSION: ' || coalesce(legal_auth1_desc, '')
            when noa_cd in ('700', '701', '702', '703', '713', '714', '715', '717', '718', '719', '720', '721', '722', '723', '724', '730', '731', '740', '741', '742', '743', '744', '745', '746', '747', '748', '749', '750', '751', '752', '753', '754', '755', '756', '757', '758', '759', '760', '761', '762', '763', '764', '765', '766', '767', '768', '769', '770', '771', '772', '773', '774', '775', '776', '777', '778', '779', '780', '781', '782', '783', '784', '790', '791', '792', '793', '794', '795', '796', '797', '798', '799') then
                'CHANGE: ' || pay_plan_cd || '-' || grade_cd || ' Step ' || step_cd || ' $' || total_pay_amt::varchar
            when noa_cd in ('800', '801', '802', '803', '804', '805', '806', '807', '808', '809', '810', '811', '812', '813', '814', '815', '816', '817', '818', '819', '820', '821', '822', '823', '824', '825', '826', '827', '828', '829', '830', '831', '832', '833', '834', '835', '836', '837', '838', '839', '840', '841', '842', '843', '844', '845', '846', '847', '848', '849', '850', '851', '852', '853', '854', '855', '856', '857', '858', '859', '860', '861', '862', '863', '864', '865', '866', '867', '868', '869', '870', '871', '872', '873', '874', '875', '876', '877', '878', '879', '880', '881', '882', '883', '884', '885', '886', '887', '888', '889', '890', '891', '892', '893', '894', '895', '896', '897', '898', '899') then
                'AWARD/BONUS: ' || coalesce(legal_auth1_desc, '')
            else
                'ACTION: ' || noa_cd || ' - ' || coalesce(legal_auth1_desc, 'No description')
        end as remark_text
    from action_data
    where noa_cd is not null
)

select * from remarks_generated

{% endmacro %}
