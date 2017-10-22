CREATE PROCEDURE `proc_calc_fund_nav_by_mgrcomp`(
i_mgrcomp varchar(20), 
i_date_from date, 
i_date_to date,
i_check_before_save bool
)
BEGIN
	declare wind_code_t varchar(20);
	declare kdr_type int default 3; -- name_date_rr type = 3
	declare nav_date_t date;
    declare nav_acc_t double;
    declare nav_acc_last_t double default 0;
    declare nav_date_from_fn, nav_date_from_kdr, nav_date_to_fn, nav_date_to_kdr date;
    declare need_calc bool default true;
    DECLARE done INT DEFAULT FALSE;
    /* 声明游标 */
    DECLARE cur_4_check CURSOR FOR 
		select wind_code from fund_info where fund_mgrcomp=i_mgrcomp;
    DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;
    
    -- 检查基金是否ok
	-- 打开游标
	OPEN cur_4_check;
	-- 开始循环
	read_loop: LOOP
		-- 提取游标里的数据，这里只有一个，多个的话也一样； 
		FETCH cur_4_check INTO wind_code_t;
		if done is null then
			leave read_loop;
		end if;
		call proc_calc_fund_nav_weekly_rr(wind_code_t, i_date_from, i_date_to, i_check_before_save);
	END LOOP read_loop;
	CLOSE cur_4_check;

	-- SET done = FALSE; -- 两个游标的情况下，注意在遍历第二个游标之前将done标志设为FALSE
    
	replace into name_date_rr(name, date, type, rr) 
    select i_mgrcomp, date_rr.date, kdr_type, date_rr.avgrr from
	(
		select date, avg(rr) avgrr
		from name_date_rr kdr2 
		where kdr2.key in (select wind_code from fund_info where fund_mgrcomp = i_mgrcomp) 
        and date between i_date_from and i_date_to
        and type = 3
		group by date
	) date_rr
	left outer join
	(
		select date from name_date_rr kdr_sub 
        where kdr_sub.key = i_mgrcomp 
        and kdr_sub.type=kdr_type
        and date between i_date_from and i_date_to
	) mark_val
	on date_rr.date = mark_val.date
	where mark_val.date is null;
END