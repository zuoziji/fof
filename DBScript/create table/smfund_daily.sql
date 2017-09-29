drop table if exists smfund_daily;

create table smfund_daily 
ENGINE = MyISAM
as 
select *, (select count(*) from wind_trade_date as td where sm.trade_date<td.trade_date and td.trade_date<=sm.next_pcvdate) as ndays_to_pcvdate 
FROM wind_smfund_daily as sm 
where next_pcvdate is not null
and close_a is not null
;