{% macro generate_biis_event_id(year_column, sequence_column) %}
{#
    Generates a unique BIIS_EVENT_ID using the pattern: year * 1000000 + sequence_number
    
    This macro replicates the logic from the Informatica exp_MAIN2BIIS transformation:
    - v_CURRENT_YEAR = GET_DATE_PART(EFFDT, 'YYYY')
    - v_EVENT_ID = IIF(v_CURRENT_YEAR = v_PREVIOUS_YEAR, v_EVENT_ID + 1, EHRP_SEQ_NUMBER + 1)
    - o_EVENT_ID = v_EVENT_ID
    
    In Snowflake, we use ROW_NUMBER() to generate sequential IDs within each year,
    then combine with the year to create the final event ID.
    
    Args:
        year_column: The column containing the year (e.g., 'effdt_year')
        sequence_column: The column containing the base sequence number from SEQUENCE_NUM_TBL
    
    Returns:
        A SQL expression that generates the BIIS_EVENT_ID
#}

(
    {{ year_column }} * 1000000 + 
    {{ sequence_column }} + 
    row_number() over (
        partition by {{ year_column }} 
        order by effdt, emplid, empl_rcd, effseq
    )
)

{% endmacro %}


{% macro get_next_sequence_number(year_value) %}
{#
    Gets the next available sequence number for a given year.
    Used to update the SEQUENCE_NUM_TBL after processing.
    
    Args:
        year_value: The year to get the sequence for
    
    Returns:
        The next sequence number to use
#}

select 
    coalesce(max(biis_event_id) % 1000000, 0) + 1 as next_sequence
from {{ ref('fct_action_primary') }}
where date_part('year', eff_dte) = {{ year_value }}

{% endmacro %}
