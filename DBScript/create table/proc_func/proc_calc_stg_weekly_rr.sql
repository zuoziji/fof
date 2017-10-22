CREATE PROCEDURE `proc_calc_stg_weekly_rr`(
i_date_from date
)
BEGIN
	call proc_calc_stg_weekly_rr_by_stg_name('事件驱动策略', i_date_from);
	call proc_calc_stg_weekly_rr_by_stg_name('债券策略', i_date_from);
	call proc_calc_stg_weekly_rr_by_stg_name('其他市场中性策略', i_date_from);
	call proc_calc_stg_weekly_rr_by_stg_name('其他策略', i_date_from);
	call proc_calc_stg_weekly_rr_by_stg_name('其他股票策略', i_date_from);
	call proc_calc_stg_weekly_rr_by_stg_name('套利策略', i_date_from);
	call proc_calc_stg_weekly_rr_by_stg_name('宏观策略', i_date_from);
	call proc_calc_stg_weekly_rr_by_stg_name('管理期货策略', i_date_from);
	call proc_calc_stg_weekly_rr_by_stg_name('组合基金策略', i_date_from);
	call proc_calc_stg_weekly_rr_by_stg_name('股票多头策略', i_date_from);
	call proc_calc_stg_weekly_rr_by_stg_name('股票多空策略', i_date_from);
	call proc_calc_stg_weekly_rr_by_stg_name('货币市场策略', i_date_from);
	call proc_calc_stg_weekly_rr_by_stg_name('阿尔法策略', i_date_from);
END