# -*- coding: utf-8 -*-
"""
Created on 2017/8/24
@author: MG
"""
import logging
from config_fh import get_db_session, get_db_engine
from fh_tools.fh_utils import str_2_date
import pandas as pd
logger = logging.getLogger()


def get_fund_calc_info(wind_code_s):
    """获取 fund_essential_info 相关基金要素信息，费率、费率计算模式、初始规模、初始金额等数据"""
    sql_str = """SELECT wind_code_s,wind_code,sec_name_s,date_start,date_end,warning_line,winding_line,closed_period,
investment_scope,investment_restrictions,open_frequency,bonus_mode,subscribe_threshold,redemption_threshold,
day_count_4_calc_fee,manage_fee_rate,manage_fee_calc_mode,custodian_fee_rate,custodian_fee_calc_mode,
admin_fee_rate,admin_fee_calc_mode,storage_fee_rate,storage_fee_calc_mode,subscribe_fee_rate,redemption_fee_rate,
subscribe_fee_mode,incentive_raito,incentive_mode,incentive_period,other_contract,remark,
ifnull(invest_amount,0),ifnull(share_confirmed,0)
FROM fund_essential_info where wind_code_s = :wind_code_s"""
    with get_db_session() as session:
        table = session.execute(sql_str, {"wind_code_s": wind_code_s})
        content = table.first()
    if content is None:
        fund_calc_info = None
    else:
        fund_calc_info = {
            'wind_code_s': content[0],
            'wind_code': content[1],
            'sec_name_s': content[2],
            'date_start': str_2_date(content[3]),
            'date_end': str_2_date(content[4]),
            'warning_line': content[5],
            'winding_line': content[6],
            'closed_period': content[7],
            'investment_scope': content[8],
            'investment_restrictions': content[9],
            'open_frequency': content[10],
            'bonus_mode': content[11],
            'subscribe_threshold': content[12],
            'redemption_threshold': content[13],
            'day_count_4_calc_fee': content[14],
            'manage_fee_rate': content[15],
            'manage_fee_calc_mode': content[16],
            'custodian_fee_rate': content[17],
            'custodian_fee_calc_mode': content[18],
            'admin_fee_rate': content[19],
            'admin_fee_calc_mode': content[20],
            'storage_fee_rate': content[21],
            'storage_fee_calc_mode': content[22],
            'subscribe_fee_rate': content[23],
            'redemption_fee_rate': content[24],
            'subscribe_fee_mode': content[25],
            'incentive_raito': content[26],
            'incentive_mode': content[27],
            'incentive_period': content[28],
            'other_contract': content[29],
            'remark': content[30],
            'invest_amount': content[31],
            'share_confirmed': content[32],
        }
    return fund_calc_info


def get_fund_nav_calc_data_last(wind_code, nav_date, fund_calc_info=None,
                                share_default=None, cash_amount_default=None, other_fee_default=None):
    """
    获取最近一个净值日期的 fund_nav_calc 数据作为参考计算数据
    :param wind_code: 
    :param nav_date: 
    :return: fund_nav_calc_data_last_dic 返回dict对象，其中 has_data 为 true 标明没有数据
    """
    # 获取净值计算需要的基本信息
    if fund_calc_info is None:
        fund_calc_info = get_fund_calc_info(wind_code)
    # 获取最近一个净值日期的 fund_nav_calc 数据作为参考计算数据
    sql_str = """select nav_date, share, market_value, cash_amount, 
    manage_fee, custodian_fee, admin_fee, storage_fee, other_fee, 
    market_value + cash_amount cap_tot, 
    market_value + cash_amount - (manage_fee + custodian_fee + admin_fee + storage_fee + other_fee) cap_net, nav 
    from fund_nav_calc where wind_code = :wind_code and nav_date = 
    (select max(nav_date) from fund_nav_calc where wind_code = :wind_code1 and nav_date < :nav_date)"""
    with get_db_session() as session:
        table = session.execute(sql_str, {'wind_code': wind_code, 'wind_code1': wind_code, "nav_date": nav_date})
        content = table.first()
        # logger.debug("%s", content)
    if content is None:
        # 没有历史记录，从0开始计算
        logger.warning("fund %s 没有可用的上一净值日期数据，使用默认数据：\nfund_calc_info：%s",
                       wind_code, fund_calc_info)
        has_data = False
        nav_date = fund_calc_info["date_start"]
        share = fund_calc_info["share_confirmed"]
        if share is None:
            share = 0
        market_value = 0
        cash_amount = fund_calc_info["invest_amount"]
        if cash_amount is None:
            cash_amount = 0
        manage_fee, custodian_fee, admin_fee, storage_fee, other_fee = 0, 0, 0, 0, 0
    else:
        # 存在历史记录，继承上一净值日期数据进行计算
        has_data = True
        nav_date, share, market_value, cash_amount, manage_fee, custodian_fee, \
        admin_fee, storage_fee, other_fee, cap_tot, cap_net, nav = content

    if share_default is not None:
        share = share_default
    if other_fee_default is not None:
        other_fee = other_fee_default
    if cash_amount_default is not None:
        cash_amount = cash_amount_default

    cap_tot = market_value + cash_amount
    cap_net = cap_tot - (manage_fee + custodian_fee + admin_fee + storage_fee + other_fee)
    if share == 0:
        nav = 0
        logger.warning("fund %s 没有可用的份额数据，无法计算净值，可以通过fund_essential_info表添加初始份额，或通过fund_nav_calc表添加上一净值日期的份额",
                       wind_code)
    else:
        nav = cap_net / share

    # 查询记录进行汇总
    fund_nav_calc_data_last_dic = {
        "has_data": has_data,
        "nav_date": nav_date,
        "share": share,
        "market_value": market_value,
        "cash_amount": cash_amount,
        "manage_fee": manage_fee,
        "custodian_fee": custodian_fee,
        "admin_fee": admin_fee,
        "storage_fee": storage_fee,
        "other_fee": other_fee,
        "cap_tot": cap_tot,
        "cap_net": cap_net,
        "nav": nav,
    }
    return fund_nav_calc_data_last_dic


def calc_fof_nav(wind_code, nav_date, fund_calc_info=None,
                 share_default=None, cash_amount_default=None, other_fee_default=None):
    """
    计算FOF净值
    同时，返回各个子基金【规模、净值、市值】、母基金各项费用、银行余额，规模、总资产、净资产、净值数据
    :param wind_code: 
    :param nav_date: 
    :param fund_calc_info: 
    :return: 
    """
    nav_date = str_2_date(nav_date)
    # 获取子基金【规模、净值、市值】


    engine = get_db_engine()
    with get_db_session(engine) as session:
        # 获取子基金产品、及总份额对应关系
        sql_str_tot_share = """select wind_code, sum(w_tot_share.total_share) total_share
        from fund_essential_info fei
        inner join
        (    
        	select wind_code_s, total_share 
        	from fund_transaction ft 
        	inner join (
        	  select max(id) ft_id from fund_transaction ft 
        	  inner join(
        		select wind_code_s, max(confirm_date) confirm_date_min 
        		from fund_transaction ft 
        		where wind_code= :wind_code and confirm_date<= :confirm_date
        		group by wind_code_s) fg 
        	  on ft.wind_code_s = fg.wind_code_s and ft.confirm_date = fg.confirm_date_min 
        	group by ft.wind_code_s ) fg 
        	on ft.id = fg.ft_id
        	where ft.total_share > 0
        ) w_tot_share
        on fei.wind_code_s = w_tot_share.wind_code_s
        group by fei.wind_code"""
        table = session.execute(sql_str_tot_share, params={'wind_code': wind_code, 'confirm_date': nav_date})
        wind_code_tot_share_dic = dict(table.fetchall())
        # 获取子基金指定日期的最新净值
        if len(wind_code_tot_share_dic) > 0:
            in_str = "'" + "','".join(wind_code_tot_share_dic.keys()) + "'"

            sql_str_nav = """select fund_nav.wind_code, nav from fund_nav
inner join
(
	select wind_code, max(nav_date) nav_date_max
	from fund_nav fn
	where wind_code in (%s)
	and nav_date <= :nav_date
	group by wind_code
) fn_latest
on fund_nav.wind_code = fn_latest.wind_code
and fund_nav.nav_date = fn_latest.nav_date_max""" % in_str
            table = session.execute(sql_str_nav, params={'nav_date': nav_date})
            wind_code_nav_dic = dict(table.fetchall())
            wind_cap_df = pd.DataFrame({"tot_share":wind_code_tot_share_dic, "nav": wind_code_nav_dic})
            wind_cap_df["market_value"] = wind_cap_df['tot_share'] * wind_cap_df['nav']
            market_value = wind_cap_df["market_value"].sum()
        else:
            market_value = 0

    # fund_nav_calc_s_df = pd.read_sql(sql_str, engine, params=[wind_code, nav_date])
    # 获取净值计算需要的基本信息
    if fund_calc_info is None:
        fund_calc_info = get_fund_calc_info(wind_code)
    # 获取最近一个净值日期的数据作为参考计算数据
    fund_nav_calc_data_last_dic = get_fund_nav_calc_data_last(wind_code, nav_date, fund_calc_info)

    # 计算母基金各项费用
    manage_fee = calc_fee(fund_calc_info, nav_date, 'manage_fee', fund_nav_calc_data_last_dic)
    custodian_fee = calc_fee(fund_calc_info, nav_date, 'custodian_fee', fund_nav_calc_data_last_dic)
    admin_fee = calc_fee(fund_calc_info, nav_date, 'admin_fee', fund_nav_calc_data_last_dic)
    storage_fee = calc_fee(fund_calc_info, nav_date, 'storage_fee', fund_nav_calc_data_last_dic)

    # 计算母基金净值

    if share_default is not None:
        share = share_default
    else:
        share = fund_nav_calc_data_last_dic['share']
    if cash_amount_default is not None:
        cash_amount = cash_amount_default
    else:
        cash_amount = cash_amount = fund_nav_calc_data_last_dic['cash_amount']
    if other_fee_default is not None:
        other_fee = other_fee_default
    else:
        other_fee = 0
    # 计算母基金总资产、净资产
    cap_tot = market_value + cash_amount
    cap_net = cap_tot - (manage_fee + custodian_fee + admin_fee + storage_fee + other_fee)

    if share == 0:
        nav = 0
        logger.warning("fund %s 没有可用的份额数据，无法计算净值，可以通过fund_essential_info表添加初始份额，或通过fund_nav_calc表添加上一净值日期的份额", wind_code)
    else:
        nav = cap_net / share
    fund_nav_calc_dic = {
        "nav_date": nav_date,
        "share": share,
        "market_value": market_value,
        "cash_amount": cash_amount,
        "manage_fee": manage_fee,
        "custodian_fee": custodian_fee,
        "admin_fee": admin_fee,
        "storage_fee": storage_fee,
        "other_fee": other_fee,
        "cap_tot": cap_tot,
        "cap_net": cap_net,
        "nav": nav,
    }
    return fund_nav_calc_dic


def calc_fee(fund_calc_info, nav_date, fee_name, fund_nav_calc_data_last_dic, share_default=None, cap_net_default=None):
    """
    根据母基金代码及净值日期计算管理费：
    calc_mode 0：费率 * 上一期FOF份额 * （前期日期 - 上期FOF日期 + 1 ） / 365
    calc_mode 1：费率 * 上一日资产净值 * （前期日期 - 上期FOF日期 + 1 ） / 365
    fee_rate、fee_calc_mode、360|365 取决于数据库中相关基金要素
    上一日资产净值 取自 基金净值计算表中最近一条有日期记录的相应字段
    :param fund_calc_info: 
    :param nav_date: 
    :param fee_name: 
    :param fund_nav_calc_data_last_dic: 
    :param share_default:
    :param cap_net_default:
    :return: 
    """
    nav_date = str_2_date(nav_date)
    fee_mode = fund_calc_info[fee_name + '_calc_mode']
    fee_rate = fund_calc_info[fee_name + '_rate']
    nav_date_first = fund_calc_info['date_start']
    date_count_calc = (nav_date - nav_date_first).days + 1
    day_count_4_calc_fee = fund_calc_info['day_count_4_calc_fee']
    if fee_mode is None or fee_mode == 0:
        if share_default is not None:
            share = share_default
        else:
            share = fund_nav_calc_data_last_dic['share']
        fee = fee_rate * share * date_count_calc / day_count_4_calc_fee
    else:
        if cap_net_default is not None:
            cap_net = cap_net_default
        else:
            cap_net = fund_nav_calc_data_last_dic['cap_net']
        fee = fee_rate * cap_net * date_count_calc / day_count_4_calc_fee
    return fee


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s: %(levelname)s [%(name)s] %(message)s')

    wind_code = 'FHF-101601'
    nav_date = '2017-11-22'
    fund_nav_calc_dic = calc_fof_nav(wind_code, nav_date)
    print('fund_nav_calc_dic', fund_nav_calc_dic)
    # 获取最近一个净值日期的 fund_nav_calc 数据作为参考计算数据
    # fund_nav_calc_data_last_dic = get_fund_nav_calc_data_last(wind_code, nav_date)
    # print("fund_nav_calc_data_last_dic", fund_nav_calc_data_last_dic)
    # 计算FOF净值
    # 同时，返回各个子基金【规模、净值、市值】、母基金各项费用、银行余额，规模、总资产、净资产、净值数据
    # fund_nav_calc_dic1 = calc_fof_nav(wind_code, nav_date)
    # print("fund_nav_calc_dic1", fund_nav_calc_dic1)
    # fund_nav_calc_dic2 = calc_fof_nav(wind_code, nav_date, share_default=10000000)
    # print("fund_nav_calc_dic2", fund_nav_calc_dic2)
    # assert fund_nav_calc_dic2['share'] == 10000000
    # assert fund_nav_calc_dic2['nav'] / fund_nav_calc_dic1['nav'] == fund_nav_calc_dic1['share'] / fund_nav_calc_dic2['share']

    # 插入数据，将某批次的历史数据插入到fund_nav_calc中
    # -- 更新 fund_nav_calc 表
    # -- 份额字段，如果当日没有对应记录则使用上一日的份额
    #     sql_str = """
    # replace fund_nav_calc(wind_code, nav_date, share, market_value, nav)
    # select wind_code_s, fn.nav_date, ifnull(fnc.share, share_confirmed) share, ifnull(fnc.share, share_confirmed)*fn.nav market_value, fn.nav
    # from fund_essential_info ffm
    # 	left outer join
    # 		(select fnc_sub.wind_code, nav_date, share, market_value, cash_amount, manage_fee, custodian_fee, admin_fee, storage_fee, other_fee, nav
    # 		from fund_nav_calc fnc_sub, (select wind_code, max(nav_date) nav_date_max from fund_nav_calc where nav_date<=i_nav_date group by wind_code) fnc_max
    # 		where fnc_sub.wind_code = fnc_max.wind_code and fnc_sub.nav_date=fnc_max.nav_date_max) fnc
    # 	on fnc.wind_code=ffm.wind_code_s
    #     ,fund_nav fn
    # where ffm.wind_code = :wind_code
    #  and ffm.wind_code = fn.wind_code and fn.nav_date = :nav_date"""
    #     with get_db_session() as session:
    #         session.execute(sql_str, params={"wind_code": wind_code, "nav_date": nav_date})