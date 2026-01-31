-- Tile 5: Top Disruption Event Types (last 90 days)
-- Chart type: Horizontal bar chart

select et.event_label, sum(f.event_count) as events, sum(f.disruption_score) as score
from fct_disruption_daily f
join dim_event_type et on et.event_type_key = f.event_type_key
join dim_date d on d.date_key = f.date_key
where d.date >= current_date - interval 90 day
group by 1
order by score desc
limit 10;
