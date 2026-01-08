{{
    config(
        materialized='incremental',
        schema='marts',
        unique_key='event_id',
        incremental_strategy='append'
    )
}}

{#
    This model replicates the action_primary_all historical table from the Informatica pipeline.
    It appends daily records to maintain a complete history of all action primary records.
    
    From ehrp2biis_afterload.sql:
    INSERT INTO action_primary_all
    SELECT * FROM nknight.nwk_action_primary_tbl
    WHERE load_date = TRUNC(sysdate);
    
    This incremental model appends new records based on load_date.
#}

with source as (
    select * from {{ ref('fct_action_primary') }}
)

select *
from source

{% if is_incremental() %}
where load_date > (select max(load_date) from {{ this }})
{% endif %}
