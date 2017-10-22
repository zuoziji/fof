CREATE FUNCTION `func_calc_fund_nav_rr_all`() RETURNS date
BEGIN
	declare wind_code_t varchar(20);
	declare nav_date_min_t, nav_date_max_t, date_max_t date;
    declare date_min_ret date default '2030-12-31';
	declare done bool default false;

    /* 声明游标 */
    DECLARE cur CURSOR FOR 
		select wind_code, nav_date_min, nav_date_max, date_max
        from
		(
			select wind_code, min(nav_date) nav_date_min, max(nav_date) nav_date_max
			from fund_nav
			group by wind_code
		) fn left outer join
		(
			select name, type, max(date) date_max
			from name_date_rr kdr
			where type=1
			group by name
		) ndr
		on fn.wind_code = ndr.name;
    DECLARE CONTINUE HANDLER FOR SQLSTATE '02000' SET done = true;

	-- 打开游标
	OPEN cur;
	-- 开始循环
	read_loop: LOOP
		-- 提取游标里的数据
		FETCH cur INTO wind_code_t, nav_date_min_t, nav_date_max_t, date_max_t;		
		if done then
			leave read_loop;
		end if;
	
		if date_max_t is null then
			call proc_calc_fund_nav_rr(wind_code_t, nav_date_min_t);
            call proc_calc_fund_nav_weekly_rr(wind_code_t, nav_date_min_t);
            if date_min_ret > nav_date_min_t then
				set date_min_ret = nav_date_min_t;
			end if;
        elseif date_max_t < nav_date_max_t then
			call proc_calc_fund_nav_rr(wind_code_t, date_max_t);
            call proc_calc_fund_nav_weekly_rr(wind_code_t, date_max_t);
            if date_min_ret > date_max_t then
				set date_min_ret = date_max_t;
			end if;
        end if;
        
	END LOOP read_loop;
	CLOSE cur;
    
    # 更新策略指数
    call proc_calc_stg_weekly_rr(date_min_ret);
    
RETURN date_min_ret;
END