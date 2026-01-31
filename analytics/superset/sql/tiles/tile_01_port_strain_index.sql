-- Tile 1: Port Strain Index (time series)
-- Chart type: Line chart
-- Dataset: mart_port_strain_daily

select d.date, p.port_name, m.strain_index
from mart_port_strain_daily m
join dim_date d on d.date_key = m.date_key
join dim_port p on p.port_key = m.port_key
where p.port_name in ('Port of Los Angeles', 'Port of Long Beach')
order by d.date;
