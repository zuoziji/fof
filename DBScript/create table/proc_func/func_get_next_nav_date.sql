CREATE DEFINER=`mg`@`%` FUNCTION `func_get_next_nav_date`(i_wind_code  varchar(20)) RETURNS date
BEGIN
	# 最大的净值日期
    DECLARE nav_date_max DATE;
    # 下一个净值日期
    DECLARE nav_date_next DATE;
    select max(nav_date) into nav_date_max from fund_nav_calc where wind_code=i_wind_code;
    if nav_date_max is null then
		select fund_setupdate into nav_date_next from fund_info where wind_code=i_wind_code;
	else
		-- select min(trade_date) into nav_date_next from wind_trade_date where trade_date>nav_date_max;
        select adddate(nav_date_max, 1) into nav_date_next;
    end if;
	RETURN nav_date_next;
END