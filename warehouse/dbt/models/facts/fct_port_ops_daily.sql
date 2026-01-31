{{ config(materialized='table') }}

select
  s.date_key,
  p.port_key,
  avg(s.teu_total) as teu_total,
  avg(s.vessel_calls) as vessel_calls,
  avg(s.avg_vessel_wait_hours) as avg_vessel_wait_hours,
  avg(s.terminal_dwell_days) as terminal_dwell_days,
  avg(s.rail_dwell_days) as rail_dwell_days,
  avg(s.truck_turn_time_min) as truck_turn_time_min,
  max(s.source_record_count) as source_record_count,
  min(s.source_min_ts) as source_min_ts,
  max(s.source_max_ts) as source_max_ts
from {{ ref('stg_port_ops_daily') }} s
join {{ ref('dim_port') }} p on p.port_name = s.port_name
group by 1, 2
