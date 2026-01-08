{% macro generate_event_id(year_column, base_sequence_column) %}
{#
    Generates a unique event ID based on the year and sequence number.
    
    This macro replicates the Informatica logic:
    - If the current year equals the previous year, increment the event ID
    - Otherwise, start from the base sequence number + 1
    
    In Snowflake, we use ROW_NUMBER() to generate sequential IDs within each year partition.
    
    Parameters:
    - year_column: The column containing the year extracted from EFFDT
    - base_sequence_column: The column containing the base sequence number from SEQUENCE_NUM_TBL
    
    Returns:
    - A unique event ID as an integer
#}
(
    {{ base_sequence_column }} + 
    row_number() over (
        partition by {{ year_column }}
        order by effdt, emplid, empl_rcd, effseq
    )
)
{% endmacro %}


{% macro zero_to_null(column_name) %}
{#
    Converts zero values to NULL for leave balance fields.
    
    This macro replicates the Informatica expression:
    IIF(column = 0, NULL, column)
    
    Parameters:
    - column_name: The column to transform
    
    Returns:
    - NULL if the value is 0, otherwise the original value
#}
case when {{ column_name }} = 0 then null else {{ column_name }} end
{% endmacro %}


{% macro calculate_base_hours(work_schedule_column, std_hours_column) %}
{#
    Calculates base hours based on work schedule.
    
    This macro replicates the Informatica expression:
    IIF(GVT_WORK_SCHED='I', STD_HOURS, STD_HOURS*2)
    
    For intermittent schedules ('I'), use standard hours as-is.
    For all other schedules, double the standard hours.
    
    Parameters:
    - work_schedule_column: The column containing the work schedule code
    - std_hours_column: The column containing standard hours
    
    Returns:
    - Base hours as a decimal
#}
case 
    when {{ work_schedule_column }} = 'I' then {{ std_hours_column }}
    else {{ std_hours_column }} * 2
end
{% endmacro %}


{% macro build_agency_assignment_code(company_column, sub_agency_column) %}
{#
    Builds the agency assignment code by concatenating company and sub-agency.
    
    This macro replicates the Informatica expression:
    COMPANY || GVT_SUB_AGENCY
    
    Parameters:
    - company_column: The column containing the company code
    - sub_agency_column: The column containing the sub-agency code
    
    Returns:
    - Concatenated agency assignment code
#}
{{ company_column }} || {{ sub_agency_column }}
{% endmacro %}


{% macro build_prior_agency_subelement(xfer_from_agcy_column) %}
{#
    Builds the prior agency subelement code.
    
    This macro replicates the Informatica expression:
    IIF(IS_SPACES(GVT_XFER_FROM_AGCY) or ISNULL(GVT_XFER_FROM_AGCY), NULL, GVT_XFER_FROM_AGCY || '00')
    
    Parameters:
    - xfer_from_agcy_column: The column containing the transfer from agency code
    
    Returns:
    - Prior agency subelement code or NULL
#}
case 
    when {{ xfer_from_agcy_column }} is null or trim({{ xfer_from_agcy_column }}) = '' 
    then null 
    else {{ xfer_from_agcy_column }} || '00'
end
{% endmacro %}


{% macro build_legal_authority_text(auth_d1_column, auth_d1_2_column) %}
{#
    Builds the legal authority text by concatenating and uppercasing.
    
    This macro replicates the Informatica expression:
    UPPER(GVT_PAR_AUTH_D1 || RTRIM(GVT_PAR_AUTH_D1_2))
    
    Parameters:
    - auth_d1_column: The first part of the authority description
    - auth_d1_2_column: The second part of the authority description
    
    Returns:
    - Concatenated and uppercased legal authority text
#}
upper({{ auth_d1_column }} || rtrim({{ auth_d1_2_column }}))
{% endmacro %}
