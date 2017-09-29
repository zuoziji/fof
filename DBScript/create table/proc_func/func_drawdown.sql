CREATE FUNCTION `func_drawdown`(fund_code VARCHAR(200)) RETURNS decimal(8,4)
BEGIN

DECLARE mdd real;  # 定义最大回撤
DECLARE high real; # 前期最高点
DECLARE temp_mdd real;
DECLARE nav_curr DOUBLE;
DECLARE date_curr date;
-- 定义游标
DECLARE cur CURSOR FOR SELECT nav_acc, nav_date FROM fund_nav WHERE wind_code = fund_code;
DECLARE CONTINUE HANDLER FOR SQLSTATE '02000' SET date_curr = NULL; 

SET mdd = 0;
OPEN cur;
    -- 开始循环
read_loop: LOOP
	
    FETCH cur into nav_curr, date_curr;
	IF date_curr is null then
			leave read_loop;
        end IF;
	SELECT MAX(nav_acc) INTO high 
    FROM fund_nav 
    WHERE wind_code = fund_code 
    AND nav_date < date_curr;
    IF high is null or high < nav_curr then
		ITERATE read_loop;
	END IF;
	SET temp_mdd = 1- nav_curr / high;
    IF mdd < temp_mdd then
		SET mdd = temp_mdd;
	END IF;
	END LOOP;
    
RETURN mdd;
END