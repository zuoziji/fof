CREATE PROCEDURE `proc_update_fund_info`(force_update bool)
BEGIN
	# 基金代码
    DECLARE wind_code_curr VARCHAR(200);
    # 第一个交易日
    DECLARE nav_date_first DATE;
    # 最新一个净值日
    DECLARE nav_date_curr DATE;
	/* 声明游标 */
	DECLARE cur CURSOR FOR 
		select fv.wind_code, nav_date_first, nav_date_max 
		from (
			SELECT wind_code, min(nav_date) nav_date_first, max(nav_date) nav_date_max
			FROM fund_nav
			group by wind_code
		) fv, 
		fund_info fi 
		where fv.wind_code=fi.wind_code;
    DECLARE CONTINUE HANDLER FOR SQLSTATE '02000' SET wind_code_curr = NULL; 
    -- 打开游标
    OPEN cur;
    -- 开始循环
	read_loop: LOOP
		-- 提取游标里的数据，这里只有一个，多个的话也一样； 
		FETCH cur INTO wind_code_curr, nav_date_first, nav_date_curr;
        
        if wind_code_curr is null then
			leave read_loop;
        end if;
        call proc_update_fund_info_by_wind_code(wind_code_curr, nav_date_first, nav_date_curr, force_update);
    END LOOP read_loop;
    CLOSE cur;
END