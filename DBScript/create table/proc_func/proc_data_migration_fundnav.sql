CREATE DEFINER=`root`@`localhost` PROCEDURE `proc_data_migration_fundnav`()
BEGIN
    -- 已经废弃
    -- 基金代码
    DECLARE wind_code_curr VARCHAR(200);
    /* 声明游标 */
    DECLARE cur CURSOR FOR SELECT wind_code FROM fundnav group by wind_code;
    DECLARE CONTINUE HANDLER FOR SQLSTATE '02000' SET wind_code_curr = NULL; 
    -- 打开游标
    OPEN cur;
    -- 开始循环
    read_loop: LOOP
        -- 提取游标里的数据，这里只有一个，多个的话也一样； 
        
        FETCH cur INTO wind_code_curr;
        if wind_code_curr is null then
            leave read_loop;
        end if;
        insert into fund_nav select * from fundnav where wind_code = wind_code_curr;
    END LOOP read_loop;
    CLOSE cur;
END