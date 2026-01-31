-- Tile 9: Impact Windows Leaderboard (largest strain deltas)
-- Chart type: Table
-- Dataset: mart_event_impact_windows

select d.date as event_date, et.event_label,
       w.strain_delta, w.trade_delta_pct
from mart_event_impact_windows w
join dim_date d on d.date_key = w.event_date_key
join dim_event_type et on et.event_type_key = w.event_type_key
where w.window_days = 14
order by w.strain_delta desc
limit 20;
