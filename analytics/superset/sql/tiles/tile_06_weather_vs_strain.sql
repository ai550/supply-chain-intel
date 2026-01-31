-- Tile 6: Weather Anomaly vs Strain (7d rolling)
-- Chart type: Dual-axis line chart

select d.date, p.port_name, m.weather_anomaly_7d, m.strain_index
from mart_port_strain_daily m
join dim_date d on d.date_key = m.date_key
join dim_port p on p.port_key = m.port_key
where p.port_name in ('Port of Los Angeles', 'Port of Long Beach')
order by d.date;
