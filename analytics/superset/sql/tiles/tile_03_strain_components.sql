-- Tile 3: Components of Strain (multi-series line)
-- Chart type: Multi-series line chart

select d.date, p.port_name,
       m.terminal_dwell_days,
       m.avg_vessel_wait_hours,
       m.truck_turn_time_min
from mart_port_strain_daily m
join dim_date d on d.date_key = m.date_key
join dim_port p on p.port_key = m.port_key
where p.port_name in ('Port of Los Angeles', 'Port of Long Beach')
order by d.date;
