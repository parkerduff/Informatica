{% snapshot snp_action_remarks_all %}
{#
    Snapshot for historical tracking of action_remarks records.
    
    This replaces the Oracle pattern of inserting into action_remarks_all
    from nwk_action_remarks_tbl where event_id in (select event_id from 
    nwk_action_primary_tbl where load_date = trunc(sysdate)).
    
    dbt snapshots use SCD Type 2 to track historical changes automatically.
#}

{{
    config(
        target_schema='snapshots',
        unique_key="event_id || '-' || remark_seq",
        strategy='timestamp',
        updated_at='_loaded_at',
        invalidate_hard_deletes=True
    )
}}

select
    event_id,
    remark_seq,
    remark_cd,
    remark_text,
    _loaded_at
from {{ ref('fct_action_remarks') }}

{% endsnapshot %}
