{% macro format_remarks(event_id, remark_code, remark_text) %}
{#
    Formats remarks for personnel actions.
    
    This macro provides utility functions for creating and formatting
    remarks that are attached to personnel action records.
    
    Args:
        event_id: The BIIS event ID
        remark_code: The remark code (e.g., 'M01', 'M02', etc.)
        remark_text: The text of the remark
    
    Returns:
        A formatted remark record
#}

select
    {{ event_id }} as event_id,
    row_number() over (partition by {{ event_id }} order by {{ remark_code }}) as remark_seq,
    {{ remark_code }} as remark_cd,
    {{ remark_text }} as remark_text

{% endmacro %}


{% macro truncate_remark_text(text_column, max_length=500) %}
{#
    Truncates remark text to a maximum length.
    
    Args:
        text_column: The column containing the remark text
        max_length: Maximum length (default 500)
    
    Returns:
        Truncated text
#}

case 
    when length({{ text_column }}) > {{ max_length }} 
    then left({{ text_column }}, {{ max_length }} - 3) || '...'
    else {{ text_column }}
end

{% endmacro %}


{% macro generate_remark_sequence(event_id_column) %}
{#
    Generates sequential remark numbers for each event.
    
    Args:
        event_id_column: The column containing the event ID
    
    Returns:
        A row number partitioned by event ID
#}

row_number() over (
    partition by {{ event_id_column }} 
    order by remark_cd
)

{% endmacro %}
