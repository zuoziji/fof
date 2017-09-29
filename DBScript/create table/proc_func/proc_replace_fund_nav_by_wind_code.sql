DROP procedure IF EXISTS proc_replace_fund_nav_by_wind_code;

CREATE PROCEDURE proc_replace_fund_nav_by_wind_code(
i_wind_code varchar(20),
i_nav_date Date
)
BEGIN

-- 根据基金代码 将相应批次的净值进行归一化，并更新到fund_nav对应字段
replace fund_nav(wind_code, nav_date, nav, nav_acc, source_mark) 
select ffm.wind_code_s, fn_curr.nav_date, fn_curr.nav_acc/fn_start.nav_acc, fn_curr.nav_acc/fn_start.nav_acc, 2 
from fund_nav fn_curr, 
	(select * from fund_essential_info where wind_code != wind_code_s) ffm 
    left outer join 
    fund_nav fn_start 
		on fn_start.wind_code=ffm.wind_code and fn_start.nav_date=ffm.date_start
where ffm.wind_code = i_wind_code
 and ffm.date_start<=i_nav_date and if(ffm.date_end is null, true, i_nav_date<=ffm.date_end)
 and fn_curr.wind_code=ffm.wind_code 
 and fn_curr.nav_date=i_nav_date
;

-- 更新 fund_nav_calc 表
-- 份额字段，如果当日没有对应记录则使用上一日的份额
replace fund_nav_calc(wind_code, nav_date, share, market_value, nav)
select wind_code_s, fn.nav_date, ifnull(fnc_latest.share, share_confirmed) share, round(ifnull(fnc_latest.share, share_confirmed)*fn.nav, 2) market_value, fn.nav
from fund_essential_info ffm 
	left outer join 
		(select fnc_sub.wind_code, nav_date, share, market_value, cash_amount, manage_fee, custodian_fee, admin_fee, storage_fee, other_fee, nav
		from fund_nav_calc fnc_sub, (select wind_code, max(nav_date) nav_date_max from fund_nav_calc where nav_date<=i_nav_date group by wind_code) fnc_max
		where fnc_sub.wind_code = fnc_max.wind_code and fnc_sub.nav_date=fnc_max.nav_date_max) fnc_latest
	on fnc_latest.wind_code=ffm.wind_code_s
    ,fund_nav fn
where ffm.wind_code = i_wind_code
 and ffm.wind_code = fn.wind_code and fn.nav_date = i_nav_date and ffm.date_start<=i_nav_date and if(ffm.date_end is null, true, i_nav_date<=ffm.date_end)
;
END