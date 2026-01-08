{{
    config(
        materialized='incremental',
        schema='marts',
        unique_key='event_id',
        incremental_strategy='append'
    )
}}

{#
    This model replicates the action_secondary_all historical table from the Informatica pipeline.
    It appends daily records to maintain a complete history of all action secondary records.
    
    From ehrp2biis_afterload.sql:
    INSERT INTO action_secondary_all
    SELECT * FROM nknight.nwk_action_secondary_tbl
    WHERE event_id IN (SELECT event_id FROM nknight.nwk_action_primary_tbl WHERE load_date = TRUNC(sysdate));
    
    This incremental model appends new records based on load_date.
#}

with source as (
    select * from {{ ref('fct_action_secondary') }}
)

select *
from source

{% if is_incremental() %}
where load_date > (select max(load_date) from {{ this }})
{% endif %}
