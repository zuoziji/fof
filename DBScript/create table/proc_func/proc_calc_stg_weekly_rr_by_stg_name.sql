CREATE PROCEDURE `proc_calc_stg_weekly_rr_by_stg_name`(
i_strategy_type varchar(20), 
i_date_from date
)
BEGIN
	declare kdr_type int default 3; -- name_date_rr type = 3
	replace into name_date_rr(name, date, type, rr)
    select i_strategy_type, date, kdr_type, avg(avg_rr)
	from
	(
	select mgrcomp_id, date, avg(rr) avg_rr from name_date_rr ndr
	inner join fund_info fi
	on date>=i_date_from
	and ndr.type=2
	and fi.strategy_type=i_strategy_type
	and ndr.name=fi.wind_code
	group by mgrcomp_id, date
	) tt
	group by date;
END