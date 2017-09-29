CREATE PROCEDURE `proc_update_fund_info_by_wind_code`(
	wind_code_curr varchar(200),
    nav_date_first Date,
    nav_date_curr Date,
    force_update bool
    )
BEGIN
	# 指定交易日净值
	DECLARE nav_acc_curr DOUBLE;
    # 年华收益率
    DECLARE annual_return_curr FLOAT;
    # 最大回撤
    DECLARE nav_mdd FLOAT;
    # 夏普比率
    DECLARE sharpe_ratio FLOAT;
    # 最新净值日期
    DECLARE nav_date_ltest DATE;
    select nav_date_latest into nav_date_ltest from fund_info where wind_code = wind_code_curr;
    if force_update or nav_date_ltest is null or nav_date_ltest < nav_date_curr then
		# 统计最新净值, max?
		select nav_acc into nav_acc_curr from fund_nav where wind_code = wind_code_curr and nav_date = nav_date_curr;
        # 计算年华收益率
        IF nav_date_curr is null or nav_date_curr = nav_date_first or nav_acc_curr<=0 then
			set annual_return_curr = 0;
            set sharpe_ratio = 0;
            set nav_mdd = 0;
		ELSE
			if nav_date_curr > nav_date_first then
				set annual_return_curr = POW(nav_acc_curr,365/DATEDIFF(nav_date_curr, nav_date_first));
			else
				set annual_return_curr = 0;
			end if;
            # 计算sharpe比率
            select func_SharpeRatio(wind_code_curr, 0.03) into sharpe_ratio;
            # 统计最大回撤
            IF nav_date_ltest is NULL THEN
				select func_drawdown(wind_code_curr) into nav_mdd;
			ELSE
				select func_drawdown_update(wind_code_curr, nav_date_ltest) into nav_mdd;
			END IF;
		END IF;
		
        
		# 更新相关统计数据
		update fund_info
        set nav_acc_mdd = nav_mdd, 
			sharpe = sharpe_ratio,
            annual_return = annual_return_curr, 
            nav_acc_latest = nav_acc_curr,
            nav_date_latest = nav_date_curr
		where wind_code = wind_code_curr;
    END if;
END