{{ config(materialized='table') }}

select
  s.date_key,
  c.country_key,
  et.event_type_key,
  sum(s.event_count) as event_count,
  avg(s.avg_tone) as avg_tone,
  sum(s.event_count * et.severity_weight) as disruption_score
from {{ ref('stg_gdelt_events_daily') }} s
left join {{ ref('dim_country') }} c on c.iso3 = s.country_iso3
join {{ ref('dim_event_type') }} et on et.event_code = s.event_code
group by 1, 2, 3
