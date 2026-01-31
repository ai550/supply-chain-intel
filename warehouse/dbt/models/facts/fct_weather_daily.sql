{{ config(materialized='table') }}

select
  s.date_key,
  ws.station_key,
  s.temp_avg_c,
  s.precipitation_mm,
  s.wind_avg_mps,
  s.storm_flag,
  -- Weather anomaly score: simple heuristic combining extreme temp, precip, wind
  (
    case when abs(coalesce(s.temp_avg_c, 0)) > 35 then 1.0 else 0.0 end +
    case when coalesce(s.precipitation_mm, 0) > 25 then 1.0 else 0.0 end +
    case when coalesce(s.wind_avg_mps, 0) > 10 then 1.0 else 0.0 end +
    case when s.storm_flag then 2.0 else 0.0 end
  ) as weather_anomaly_score
from {{ ref('stg_noaa_weather_daily') }} s
join {{ ref('dim_weather_station') }} ws on ws.station_id = s.station_id
