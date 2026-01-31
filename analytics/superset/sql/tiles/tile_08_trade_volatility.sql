-- Tile 8: Trade MoM and YoY Volatility (bar + line combo)
-- Dataset: mart_trade_impact_monthly

select t.month_key, c.commodity_name,
       t.trade_value_mom_pct, t.trade_value_yoy_pct
from mart_trade_impact_monthly t
join dim_commodity c on c.commodity_key = t.commodity_key
where t.reporter_country_key = (select country_key from dim_country where iso3 = 'USA')
order by t.month_key;
