{{
    config(
        materialized='incremental',
        unique_key=['event_id', 'remark_seq'],
        tags=['marts', 'ehrp2biis']
    )
}}

with event_data as (
    select * from {{ ref('int_biis_event_generation') }}
),

standard_remarks as (
    {{ updt_erp2biis_cre8_remarks01() }}
),

no_900s_remarks as (
    {{ update_erp2biis_no900s01() }}
),

remarks_900s as (
    {{ erp2biis_cre8_remarks_900s01() }}
),

all_remarks as (
    select * from standard_remarks
    union all
    select * from no_900s_remarks
    union all
    select * from remarks_900s
)

select
    event_id,
    remark_seq,
    remark_cd,
    remark_text,
    current_timestamp() as _loaded_at
from all_remarks

{% if is_incremental() %}
where event_id not in (select distinct event_id from {{ this }})
{% endif %}
