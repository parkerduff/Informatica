{% macro generate_event_id(effdt_column) %}
{#
    This macro generates a unique event ID based on the effective date year.
    It replicates the Informatica logic that:
    1. Extracts the year from the effective date
    2. Looks up the last sequence number for that year from SEQUENCE_NUM_TBL
    3. Increments the sequence to generate a unique event ID
    
    In Snowflake, we use a sequence object combined with year-based logic.
    The event ID format is: YYYY followed by a sequential number within that year.
    
    Usage: {{ generate_event_id('effdt') }}
#}

(
    EXTRACT(YEAR FROM {{ effdt_column }}) * 1000000 + 
    ehrp_event_seq.NEXTVAL
)

{% endmacro %}


{% macro create_ehrp_sequence() %}
{#
    Creates the Snowflake sequence object for generating event IDs.
    This should be run once during initial setup.
    
    The sequence starts at 1 and increments by 1 for each new event.
    The actual event ID is computed by combining the year with the sequence value.
#}

CREATE SEQUENCE IF NOT EXISTS {{ target.schema }}.ehrp_event_seq
    START = 1
    INCREMENT = 1;

{% endmacro %}


{% macro get_next_event_id(effdt_column, seq_num_ref) %}
{#
    Alternative macro that more closely replicates the Informatica logic
    by using a lookup to the sequence number table and incrementing within
    each year.
    
    This uses window functions to generate sequential IDs within each year,
    starting from the last known sequence number for that year.
    
    Parameters:
    - effdt_column: The effective date column to extract the year from
    - seq_num_ref: Reference to the sequence_num_tbl seed or table
    
    Usage: {{ get_next_event_id('effdt', ref('sequence_num_tbl')) }}
#}

(
    EXTRACT(YEAR FROM {{ effdt_column }}) * 1000000 +
    COALESCE(
        (SELECT ehrp_seq_number FROM {{ seq_num_ref }} 
         WHERE ehrp_year = EXTRACT(YEAR FROM {{ effdt_column }})),
        0
    ) +
    ROW_NUMBER() OVER (
        PARTITION BY EXTRACT(YEAR FROM {{ effdt_column }})
        ORDER BY {{ effdt_column }}, emplid, empl_rcd, effseq
    )
)

{% endmacro %}
