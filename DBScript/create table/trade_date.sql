drop view if exists trade_date;

CREATE VIEW trade_date AS
SELECT trade_date, dayofweek(trade_date) -1 as day4week, week(trade_date) week4year
FROM wind_trade_date 
order by trade_date desc;
