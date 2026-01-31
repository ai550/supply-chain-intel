{{ config(materialized='table') }}

with codes as (
  select distinct event_code
  from {{ ref('stg_gdelt_events_daily') }}
)
select
  row_number() over (order by event_code) as event_type_key,
  'gdelt' as source_system,
  event_code,
  'event_' || event_code as event_label,
  case
    when event_code in ('18','19','20') then 2.0
    else 1.0
  end as severity_weight
from codes
