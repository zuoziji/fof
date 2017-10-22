CREATE PROCEDURE `proc_calc_fund_nav_rr`(
i_wind_code varchar(20), 
i_date_from date
)
BEGIN
	declare kdr_type int default 1; -- name_date_rr type = 1
	declare nav_date_t date;
    declare nav_acc_t double;
    declare nav_acc_last_t double default 0;
    declare done bool default false;

    /* 声明游标 */
    DECLARE cur CURSOR FOR 
		select nav_date, nav_acc
		from fund_nav
		where wind_code=i_wind_code
        and nav_date >= i_date_from;
    DECLARE CONTINUE HANDLER FOR SQLSTATE '02000' SET done = true;

	-- 打开游标
	OPEN cur;
	-- 开始循环前先执行一次，设置第一条记录
	FETCH cur INTO nav_date_t, nav_acc_last_t;
	if not done then
		-- 如果第一条记录不存在则插入第一条记录 rr=1
        if not exists(select rr from name_date_rr where name=i_wind_code and date=nav_date_t and type=kdr_type) then
			insert into name_date_rr(name, date, type, rr) values(i_wind_code, nav_date_t, kdr_type, 1);
		end if;
            
		-- 开始循环
		read_loop: LOOP
			-- 提取游标里的数据
			FETCH cur INTO nav_date_t, nav_acc_t;			
			if done then
				leave read_loop;
			end if;
		
			replace into name_date_rr(name, date, type, rr) values(i_wind_code, nav_date_t, kdr_type, nav_acc_t / nav_acc_last_t);
            
			set nav_acc_last_t = nav_acc_t;
		END LOOP read_loop;
	end if;
	CLOSE cur;
END