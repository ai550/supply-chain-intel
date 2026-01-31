{{ config(materialized='table') }}

with port as (
  select *
  from {{ ref('fct_port_ops_daily') }}
),
rollups as (
  select
    port.date_key,
    port.port_key,
    (select sum(disruption_score)
     from {{ ref('fct_disruption_daily') }} d
     where d.date_key between port.date_key - 14 and port.date_key
    ) as disruption_score_14d,
    (select avg(weather_anomaly_score)
     from {{ ref('fct_weather_daily') }} w
     where w.date_key between port.date_key - 7 and port.date_key
    ) as weather_anomaly_7d
  from port
),
scored as (
  select
    port.date_key,
    port.port_key,

    port.teu_total,
    port.terminal_dwell_days,
    port.avg_vessel_wait_hours,
    port.truck_turn_time_min,

    r.disruption_score_14d,
    r.weather_anomaly_7d,

    (
      coalesce(ln(1 + port.teu_total), 0) * 0.25 +
      coalesce(port.terminal_dwell_days, 0) * 0.30 +
      coalesce(port.avg_vessel_wait_hours, 0) * 0.25 +
      coalesce(port.truck_turn_time_min, 0) * 0.10 +
      coalesce(r.disruption_score_14d, 0) * 0.07 +
      coalesce(r.weather_anomaly_7d, 0) * 0.03
    ) as strain_index
  from port
  join rollups r using (date_key, port_key)
),
bucketed as (
  select
    *,
    case
      when strain_index < 2 then 'low'
      when strain_index < 4 then 'medium'
      when strain_index < 6 then 'high'
      else 'critical'
    end as strain_bucket
  from scored
)
select * from bucketed
