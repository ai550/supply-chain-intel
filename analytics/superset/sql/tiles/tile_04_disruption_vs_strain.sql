-- Tile 4: Disruption Score vs Strain (lag view: 0-14d)
-- Chart type: Dual-axis line chart
-- Create as a Superset virtual dataset

with base as (
  select d.date, m.port_key, m.strain_index
  from mart_port_strain_daily m
  join dim_date d on d.date_key = m.date_key
),
lags as (
  select b.date, b.port_key, b.strain_index,
         m.disruption_score_14d as disruption_roll
  from base b
  join mart_port_strain_daily m
    on m.port_key = b.port_key
    and m.date_key = cast(strftime(b.date, '%Y%m%d') as int)
)
select * from lags order by date;
