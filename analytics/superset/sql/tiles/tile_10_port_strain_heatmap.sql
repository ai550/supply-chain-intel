-- Tile 10: Port Strain Heatmap (month x port)
-- Chart type: Heatmap

select cast(substr(cast(m.date_key as varchar), 1, 6) as int) as month_key,
       p.port_name,
       avg(m.strain_index) as avg_strain
from mart_port_strain_daily m
join dim_port p on p.port_key = m.port_key
group by 1, 2
order by month_key, p.port_name;
