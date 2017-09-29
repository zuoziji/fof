DROP procedure IF EXISTS proc_delete_fund_nav_by_wind_code;

CREATE PROCEDURE proc_delete_fund_nav_by_wind_code(
i_wind_code varchar(20),
i_nav_date Date
)
BEGIN

# 删除该基金相关批次，给定日期的全部净值数据
delete from fund_nav
where nav_date = i_nav_date and wind_code in (
select wind_code_s from fund_essential_info 
where wind_code = i_wind_code
);

# 删除该基金相关批次，给定日期的全部净值数据
delete from fund_nav_calc
where nav_date = i_nav_date and wind_code in (
select wind_code_s from fund_essential_info 
where wind_code = i_wind_code
);
END