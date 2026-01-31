-- Tile 7: Trade Value Trend (by commodity group)
-- Chart type: Stacked area chart
-- Dataset: mart_trade_impact_monthly

select t.month_key, c.commodity_name, t.trade_value_usd
from mart_trade_impact_monthly t
join dim_commodity c on c.commodity_key = t.commodity_key
where t.reporter_country_key = (select country_key from dim_country where iso3 = 'USA')
order by t.month_key;
