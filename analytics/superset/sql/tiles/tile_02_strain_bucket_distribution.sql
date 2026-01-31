-- Tile 2: Strain Bucket Distribution (last 30 days)
-- Chart type: Pie or bar chart

select m.strain_bucket, count(*) as days
from mart_port_strain_daily m
join dim_date d on d.date_key = m.date_key
where d.date >= current_date - interval 30 day
group by 1
order by days desc;
