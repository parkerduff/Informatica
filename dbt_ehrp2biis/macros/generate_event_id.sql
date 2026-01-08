{% macro generate_event_id(year_column, base_seq_column) %}
{#
    Generates a unique event ID based on the year and sequence number.
    
    This macro replicates the Informatica exp_MAIN2BIIS transformation logic:
    - Extracts year from effective date
    - Looks up the last sequence number for that year from SEQUENCE_NUM_TBL
    - Generates unique event ID by incrementing the sequence
    
    The Informatica logic was:
    v_CURRENT_YEAR = GET_DATE_PART(EFFDT, 'YYYY')
    v_EVENT_ID = IIF(v_CURRENT_YEAR = v_PREVIOUS_YEAR, v_EVENT_ID + 1, EHRP_SEQ_NUMBER + 1)
    
    In Snowflake, we use ROW_NUMBER() to generate sequential IDs within each year.
#}
(
    {{ year_column }} * 10000000 + 
    ({{ base_seq_column }} + row_number() over (
        partition by {{ year_column }} 
        order by effdt, effseq, emplid, empl_rcd
    ))
)
{% endmacro %}


{% macro nullif_zero(column_name) %}
{#
    Converts zero values to NULL for leave balance fields.
    
    This macro replicates the Informatica exp_MAIN2BIIS transformation logic:
    IIF(HE_AL_BALANCE=0, NULL, HE_AL_BALANCE)
    
    Used for fields like:
    - HE_AL_BALANCE (annual leave balance)
    - HE_SL_BALANCE (sick leave balance)
    - HE_RES_BALANCE (restored leave balance)
    - And other leave-related fields
#}
nullif({{ column_name }}, 0)
{% endmacro %}


{% macro calculate_base_hours(work_schedule_column, std_hours_column) %}
{#
    Calculates base hours based on work schedule.
    
    This macro replicates the Informatica exp_MAIN2BIIS transformation logic:
    IIF(GVT_WORK_SCHED='I', STD_HOURS, STD_HOURS*2)
    
    For intermittent schedules ('I'), use standard hours as-is.
    For all other schedules, double the standard hours.
#}
case 
    when {{ work_schedule_column }} = 'I' then {{ std_hours_column }}
    else {{ std_hours_column }} * 2
end
{% endmacro %}


{% macro create_agency_assignment_code(company_column, sub_agency_column) %}
{#
    Creates agency assignment code by concatenating company and sub-agency.
    
    This macro replicates the Informatica exp_MAIN2BIIS transformation logic:
    COMPANY || GVT_SUB_AGENCY
#}
{{ company_column }} || {{ sub_agency_column }}
{% endmacro %}


{% macro create_prior_agency_code(xfer_from_agcy_column) %}
{#
    Creates prior agency subelement code.
    
    This macro replicates the Informatica exp_MAIN2BIIS transformation logic:
    IIF(IS_SPACES(GVT_XFER_FROM_AGCY) or ISNULL(GVT_XFER_FROM_AGCY), NULL, GVT_XFER_FROM_AGCY || '00')
#}
case 
    when {{ xfer_from_agcy_column }} is null or trim({{ xfer_from_agcy_column }}) = '' then null
    else {{ xfer_from_agcy_column }} || '00'
end
{% endmacro %}
