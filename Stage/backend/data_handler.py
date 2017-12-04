# -*- coding: utf-8 -*-
"""
Created on 2017/4/7
@author: MG
"""
from datetime import datetime
import pandas as pd
import numpy as np
from config_fh import STR_FORMAT_DATE, get_db_engine, get_db_session
import matplotlib.pyplot as plt  # pycharm 需要通过现实调用 plt.show 才能显示plot
from fh_tools.fh_utils import str_2_date, DataFrame, return_risk_analysis
import logging

logger = logging.getLogger()


def get_fund_nav(fund_code_list, from_date, to_date):
    """
    获取基金净值增长率，所有基金代码横向数据对其，补插值方案为”上一条数据净值”
    :param fund_code_list: 基金代码列表
    :param from_date: 起始日期
    :param to_date: 截止日期
    :return: 
    """
    fund_code_count = len(fund_code_list)
    from_date = from_date if type(from_date) is str else datetime.strptime(from_date, STR_FORMAT_DATE)
    to_date = to_date if type(to_date) is str else datetime.strptime(to_date, STR_FORMAT_DATE)
    param_str = ", ".join(['%s' for n in range(fund_code_count)])
    sql_str = 'select wind_code, nav_date, nav_acc from fund_nav where wind_code in (%s)' % param_str
    engine = get_db_engine()
    df = pd.read_sql(sql_str, engine, params=fund_code_list, parse_dates=['nav_date'])
    if df.shape[0] == 0:
        logger.info('No data found')
        return None
    fund_df_all = df.pivot(index='nav_date', columns='wind_code', values='nav_acc') \
        .ffill().bfill()
    fund_df = fund_df_all.iloc[(from_date <= fund_df_all.index) & (fund_df_all.index <= to_date)]
    pct_df = (fund_df.pct_change().fillna(0) + 1).cumprod()
    return pct_df


def get_fof_fund_pct_each_nav_date(wind_code_p):
    """
    根据母基金代码获取每个净值日的子基金配比
    :param wind_code_p: 
    :return: 
    """
    # 获取基金净值信息
    engine = get_db_engine()
    sql_str = 'select nav_date, nav_acc from fund_nav where wind_code = %s'
    nav_df = pd.read_sql(sql_str, engine, params=[wind_code_p], parse_dates=['nav_date'], index_col='nav_date')
    if nav_df.shape[0] == 0:
        logger.info('No data found')
        return None, None
    nav_date_list = list(nav_df.index)
    nav_date_list.sort()
    # 获取子基金比例信息
    sql_str = """select fei.wind_code wind_code, ffp.wind_code_s, ifnull(fi.sec_name, fei.sec_name_s) sec_name, date_adj, invest_scale 
from fof_fund_pct ffp 
left join fund_essential_info fei
on wind_code_p = %s
and ffp.wind_code_s = fei.wind_code_s
left join fund_info fi
on fei.wind_code = fi.wind_code
"""
    data_df = pd.read_sql(sql_str, engine, params=[wind_code_p], parse_dates=['date_adj'])
    sec_name_date_scale_df = data_df.groupby(['wind_code', 'sec_name', 'date_adj']).sum()
    fof_fund_pct_df = sec_name_date_scale_df.reset_index().pivot(index='sec_name', columns='date_adj', values='invest_scale').fillna(0)
    date_fund_scale_dic = fof_fund_pct_df.to_dict()  # 'records'
    date_4_scale_list = list(date_fund_scale_dic.keys())
    date_4_scale_list.sort()
    if len(date_4_scale_list) == 0:
        return nav_df, None
    # 设置全0记录
    zero_scale_dic = {sec_name: 0 for sec_name in date_fund_scale_dic[date_4_scale_list[0]].keys()}
    # 循环查找每一个日期，先前寻找 当期日期 在 fof_fund_pct_df 中记录对应的最近的日期
    # 将相应的配比记录进行复制作为当期子基金持仓纪录
    nav_date_fund_scale_dic = {}
    date_index_count = len(date_4_scale_list)
    nav_date_count = len(nav_date_list)
    for rownum, copy_date_cur in enumerate(date_4_scale_list):
        for n_date in range(len(nav_date_fund_scale_dic), nav_date_count):
            nav_date = nav_date_list[n_date]
            if nav_date < copy_date_cur:
                if rownum == 0:
                    nav_date_fund_scale_dic[nav_date] = zero_scale_dic
                else:
                    nav_date_fund_scale_dic[nav_date] = date_fund_scale_dic[date_4_scale_list[rownum - 1]]
            elif nav_date == copy_date_cur:
                nav_date_fund_scale_dic[nav_date] = date_fund_scale_dic[copy_date_cur]
                break
            else:
                break

    # 没有找到，则说明当期最新的配比为最后一次配比状态
    last_scale_dic = date_fund_scale_dic[date_4_scale_list[-1]]
    for nav_date in nav_date_list[len(nav_date_fund_scale_dic):]:
        nav_date_fund_scale_dic[nav_date] = last_scale_dic
    # 将配比信息合并成DataFrame
    nav_date_fund_scale_df = pd.DataFrame(nav_date_fund_scale_dic).T
    return nav_df, nav_date_fund_scale_df


def get_fund_nav_between(wind_code, from_date, to_date, index_code='000300.SH'):
    """
    输入 wind_code 基金代码，返回基金代码以及同期沪深300指数的对比走势
    :param wind_code: 基金代码
    :param from_date: 起始日期
    :param to_date: 截止日期
    :return: 拟合后的对比走势，最新净值日的基金增长率、指数增长率、最新净值日期
    """
    engine = get_db_engine()
    sql_str = r"select nav_date, nav_acc from fund_nav where wind_code =%s order by nav_date"
    fund_df = pd.read_sql(sql_str, engine, params=[wind_code], parse_dates=['nav_date'])  #
    if fund_df.shape[0] == 0:
        logger.info('No data found')
        return None
    fund_df.set_index('nav_date', inplace=True)
    fund_df.rename(columns={'nav_acc': wind_code}, inplace=True)
    sql_str = "select trade_date, close from wind_index_daily where wind_code=%s order by trade_date"
    hs300_df = pd.read_sql(sql_str, engine, params=[index_code], parse_dates=['trade_date'])  #
    # hs300_df.set_index('trade_date', inplace=True)
    hs300_df.rename(columns={'close': index_code}, inplace=True)
    # df = pd.concat([fund_df, hs300_df], axis=1, join='inner')
    fund_tmp_df = fund_df.reset_index()
    fund_tmp_df = fund_tmp_df.rename(columns={'nav_date': 'trade_date'})
    df = pd.merge(fund_tmp_df, hs300_df, on=['trade_date'])
    df.set_index('trade_date', inplace=True)
    fund_df = df.iloc[(from_date <= df.index) & (df.index <= to_date)].copy()
    if fund_df.shape[0] == 0:
        logger.info('No data between %s - %s', from_date, to_date)
        return None
    fund_df[index_code] = (fund_df[index_code].pct_change().fillna(0) + 1).cumprod()  # * fund_df[wind_code][0]
    # pct_df = (fund_df.pct_change().fillna(0) + 1).cumprod()
    date_latest = fund_df.index[-1].date()
    # fund_rr = pct_df.iloc[-1, 0]
    # index_rr = pct_df.iloc[-1, 1]
    # return fund_df, fund_rr, index_rr, date_latest
    return {"fund_df": fund_df, "date_latest": date_latest}


def get_fof_fund_date_range(wind_code):
    """获取FOF基金下面每一只子基金的日期范围"""
    fund_date_df = None
    sql_str = """select wind_code_s, date_adj, invest_scale from fof_fund_pct f1 where f1.wind_code_p = %s"""
    engine = get_db_engine()
    data_df = pd.read_sql(sql_str, engine, params=[wind_code])
    if data_df.shape[0] == 0:
        return None
    fund_date_df = data_df.pivot(columns='wind_code_s', index='date_adj', values='invest_scale')
    date_list = list(fund_date_df.index)
    date_list.sort()
    fund_date_range_dic = {}
    for wind_code_s in fund_date_df.columns:
        fund_date_s = fund_date_df[wind_code_s]
        date_range_list = []
        date_range = []
        for fund_date in date_list:
            invest_scale = fund_date_s[fund_date]
            if not np.isnan(invest_scale):
                date_range.append(fund_date)
            if np.isnan(invest_scale) and len(date_range) > 0:
                date_range_list.append((min(date_range), fund_date))
                date_range = []
        else:
            if len(date_range) > 0:
                date_range_list.append((min(date_range), datetime.max.date()))
                date_range = []
            fund_date_range_dic[wind_code_s] = date_range_list
    return fund_date_range_dic


def get_fof_nav_between(wind_code, from_date, to_date, index_code='000300.SH'):
    """
    获取母基金及全部子基金以及指数对比走势
    时间序列以母基金为准
    指数做归一处理，母基金及子基金显示原始净值
    :param wind_code: 
    :param from_date: 
    :param to_date: 
    :param index_code: 
    :return: 
    """
    from_date = str_2_date(from_date)
    to_date = str_2_date(to_date)
    engine = get_db_engine()
    # 获取母基金净值
    fund_p_sql_str = 'select nav_date, nav_acc from fund_nav where wind_code = %s'
    fund_p_data_df = pd.read_sql(fund_p_sql_str, engine, params=[wind_code])
    fund_p_nav_df = fund_p_data_df.rename(columns={'nav_acc': wind_code}).set_index('nav_date')  #
    # sql_str = """select  fn.wind_code, nav_date_friday as nav_date, nav_acc
    # from fund_nav fn,
    # (
    # select fund_nav.wind_code, adddate(nav_date, 4 - weekday(nav_date)) as nav_date_friday, max(nav_date) as nav_date_max
    # from fund_nav,
    # 	(select wind_code_s
    # 	from fof_fund_pct f1, (select wind_code_p, max(date_adj) date_max from fof_fund_pct group by wind_code_p) f2
    # 	where f1.wind_code_p = %s and f2.date_max = f1.date_adj and f1.wind_code_p = f2.wind_code_p) wind_t
    # where fund_nav.wind_code = wind_t.wind_code_s
    # group by wind_code, nav_date_friday
    # ) fnf
    # where fn.wind_code = fnf.wind_code
    # and fn.nav_date = fnf.nav_date_max
    # order by nav_date_friday"""
    sql_str = """select  fn.wind_code, nav_date_max as nav_date, nav_acc
from fund_nav fn,
(
select fund_nav.wind_code, adddate(nav_date, 4 - weekday(nav_date)) as nav_date_friday, max(nav_date) as nav_date_max 
from fund_nav, 
	(select wind_code_s 
	from fof_fund_pct f1
	where f1.wind_code_p = %s) wind_t
where fund_nav.wind_code = wind_t.wind_code_s
group by wind_code, nav_date_friday
) fnf
where fn.wind_code = fnf.wind_code
and fn.nav_date = fnf.nav_date_max
order by nav_date_friday"""
    fund_data_df = pd.read_sql(sql_str, engine, params=[wind_code])
    if fund_p_nav_df.shape[0] > 0 and fund_data_df.shape[0] > 0:
        logger.info('get fund nav data %s', fund_data_df.shape)
        fund_nav_df = fund_data_df.pivot(index='nav_date', columns='wind_code', values='nav_acc')
        fof_nav_df = fund_p_nav_df.merge(fund_nav_df, how='outer', left_index=True, right_index=True)
        # fof_nav_df.ffill(inplace=True)
        nav_date_min, nav_date_max = min(fund_p_nav_df.index), max(fund_p_nav_df.index)
        fof_nav_df = fof_nav_df[(nav_date_min <= fof_nav_df.index) & (fof_nav_df.index <= nav_date_max)]
        logger.info('merge fund nav data %s', fof_nav_df.shape)
    else:
        fof_nav_df = fund_p_nav_df
        logger.info('get fof nav data %s', fof_nav_df.shape)
    # logger.info(fof_nav_df.shape)
    # 获取指数数据
    sql_str = "select trade_date, close from wind_index_daily where wind_code=%s order by trade_date"
    hs300_df = pd.read_sql(sql_str, engine, params=[index_code])  # , parse_dates=['trade_date']
    # hs300_df.set_index('trade_date', inplace=True)
    hs300_df.rename(columns={'close': index_code}, inplace=True)
    hs300_df.set_index('trade_date', inplace=True)
    # df = pd.concat([fund_df, hs300_df], axis=1, join='inner')
    # fund_tmp_df = fof_nav_df.reset_index()
    # fund_tmp_df = fund_tmp_df.rename(columns={'nav_date': 'trade_date'})
    # df = pd.merge(fof_nav_df, hs300_df, how='left')
    # df.set_index('trade_date', inplace=True)
    df = pd.merge(fof_nav_df, hs300_df, how='left', left_index=True, right_index=True)
    if df.shape[0] == 0:
        return None
    # 内插值处理，按时间范围进行截取
    df = DataFrame.interpolate_inner(df)
    fund_df = df.iloc[(from_date <= df.index) & (df.index <= to_date)]
    # 删除全Nan的列 截取有效日期范围内数据
    fund_date_range_dic = get_fof_fund_date_range(wind_code)
    col_names = list(fund_df.columns)
    date_list = list(fund_df.index)
    for wind_code_s in col_names:
        if fund_df[wind_code_s].isnull().all():
            fund_df = fund_df.drop(wind_code_s, axis=1)
            continue

    # 对 fund_df 进行有效日期 筛选，只选取有效日期范围内的数据
    def map_func(col_vol, index_vol, data_vol):
        if col_vol in fund_date_range_dic:
            date_range_list = fund_date_range_dic[col_vol]
            for date_range in date_range_list:
                if date_range[0] <= index_vol < date_range[1]:
                    return data_vol
            return np.nan
        else:
            return data_vol
    if fund_date_range_dic is not None:
        fund_df = DataFrame.map(fund_df.copy(), map_func)

    # 计算 指数净值增长率
    if index_code in fund_df.columns:
        fund_df[index_code] = (fund_df[index_code].pct_change().fillna(0) + 1).cumprod()  # * fund_df[wind_code][0]
    if fund_df.shape[0] == 0:
        date_latest = None
        rr_latest_s = None
    else:
        pct_df = (fund_df.pct_change().fillna(0) + 1).cumprod()
        date_latest = fund_df.index[-1]
        rr_latest_s = pct_df.iloc[-1, :]
    if fund_df.empty:
        return None
    return {"fund_df": fund_df, "date_latest": date_latest}


def get_fof_nav_rr_between(wind_code, from_date, to_date):
    """
    获取母基金及全部子基金以及指数对比走势
    时间序列以母基金为准
    指数做归一处理，母基金及子基金显示原始净值
    :param wind_code: 
    :param from_date: 
    :param to_date: 
    :return: 
    """
    from_date = str_2_date(from_date)
    to_date = str_2_date(to_date)
    engine = get_db_engine()
    # 获取母基金净值
    fund_p_sql_str = 'select nav_date, nav_acc from fund_nav where wind_code = %s '
    fund_p_data_df = pd.read_sql(fund_p_sql_str, engine, params=[wind_code])
    fund_p_nav_df = fund_p_data_df.rename(columns={'nav_acc': wind_code}).set_index('nav_date')  #
    # 获取子基金净值
    sql_str = """select  fn.wind_code, nav_date_friday as nav_date, nav_acc
from fund_nav fn,
(
select fund_nav.wind_code, adddate(nav_date, 4 - weekday(nav_date)) as nav_date_friday, max(nav_date) as nav_date_max 
from fund_nav, 
	(select wind_code_s 
	from fof_fund_pct f1, (select wind_code_p, max(date_adj) date_max from fof_fund_pct group by wind_code_p) f2 
	where f1.wind_code_p = %s and f2.date_max = f1.date_adj and f1.wind_code_p = f2.wind_code_p) wind_t
where fund_nav.wind_code = wind_t.wind_code_s
group by wind_code, nav_date_friday
) fnf
where fn.wind_code = fnf.wind_code
and fn.nav_date = fnf.nav_date_max
order by nav_date_friday"""
    fund_data_df = pd.read_sql(sql_str, engine, params=[wind_code])
    if fund_data_df.shape[0] > 0:
        logger.info('get fund nav data %s', fund_data_df.shape)
        fund_nav_df = fund_data_df.pivot(index='nav_date', columns='wind_code', values='nav_acc')
        # fund_nav_pct_df = fund_nav_df.pct_change()
        # fund_nav_rr_df = (fund_nav_pct_df + 1).cumprod()
        fof_nav_df = fund_p_nav_df.merge(fund_nav_df, how='left', left_index=True, right_index=True)
        logger.info('merge fund nav data %s', fof_nav_df.shape)
    else:
        fof_nav_df = fund_p_nav_df
        logger.info('get fof nav data %s', fof_nav_df.shape)
    # logger.info(fof_nav_df.shape)
    # 获取指数数据
    index_code = '000300.SH'
    sql_str = "select trade_date, close from wind_index_daily where wind_code=%s order by trade_date"
    hs300_df = pd.read_sql(sql_str, engine, params=[index_code])  # , parse_dates=['trade_date']
    # hs300_df.set_index('trade_date', inplace=True)
    hs300_df.rename(columns={'close': index_code}, inplace=True)
    hs300_df.set_index('trade_date', inplace=True)
    # df = pd.concat([fund_df, hs300_df], axis=1, join='inner')
    # fund_tmp_df = fof_nav_df.reset_index()
    # fund_tmp_df = fund_tmp_df.rename(columns={'nav_date': 'trade_date'})
    # df = pd.merge(fof_nav_df, hs300_df, how='left')
    # df.set_index('trade_date', inplace=True)
    # 向左合并对齐
    df = pd.merge(fof_nav_df, hs300_df, how='left', left_index=True, right_index=True)
    # df.ffill(inplace=True)
    # 取日期区间数据
    fund_rr_df = df.iloc[(from_date <= df.index) & (df.index <= to_date)].copy()
    if fund_rr_df.shape[0] == 0:
        return {"fund_df": None, "rr_latest_s": None, "date_latest": None}
    fund_rr_dic = {}
    fund_rr_latest_dic = {}
    wind_code_list = list(fund_rr_df.columns)
    for code in wind_code_list:
        if code == wind_code:
            fund_nav_s = fund_rr_df[code]
            fund_rr_latest_dic[code] = fund_nav_s.pct_change().iloc[-1]
        else:
            fund_nav_s = fund_rr_df.pop(code)
            # fund_pct_s = fund_rr_df[code]
            if fund_nav_s.shape[0] == 0:
                continue
            is_not_nan = fund_nav_s.index[~np.isnan(fund_nav_s)]
            date_range = (min(is_not_nan), max(is_not_nan))
            fund_nav_s = fund_nav_s[date_range[0]:date_range[1]].interpolate()
            fund_pct_s = fund_nav_s.pct_change().fillna(0)
            fund_rr_s = (fund_pct_s + 1).cumprod()
            fund_rr_df = fund_rr_df.merge(pd.DataFrame(fund_rr_s), how='left', left_index=True, right_index=True)
            fund_rr_latest_dic[code] = fund_pct_s.iloc[-1]
    rr_latest_s = pd.Series(fund_rr_latest_dic)
    if fund_rr_df.shape[0] == 0:
        date_latest = None
    else:
        date_latest = fund_rr_df.index[-1]
    return {"fund_df": fund_rr_df, "rr_latest_s": rr_latest_s, "date_latest": date_latest}


def get_fund_nav_by_wind_code(wind_code, limit=5):
    """
    输入 wind_code 基金代码，返回基金代码以及同期沪深300指数的对比走势
    :param wind_code: 基金代码
    :param limit: 限制返回数量
    :return: 拟合后的对比走势，最新净值日的基金增长率、指数增长率、最新净值日期
    """
    engine = get_db_engine()
    if limit  == 0:
        sql_str = r"select nav_date, nav, nav_acc from fund_nav where wind_code =%s order by nav_date desc "
        fund_df = pd.read_sql(sql_str, engine, params=[wind_code], parse_dates=['nav_date'])
    else :
        sql_str = r"select nav_date, nav, nav_acc from fund_nav where wind_code =%s order by nav_date desc limit %s"
        fund_df = pd.read_sql(sql_str, engine, params=[wind_code, limit + 1], parse_dates=['nav_date'])  #
    if fund_df.shape[0] == 0:
        logger.info('No data found')
        return None
    fund_df.sort_values('nav_date', inplace=True)
    fund_df.set_index('nav_date', inplace=True)
    fund_df['pct'] = fund_df['nav_acc'].pct_change().fillna(0)
    if fund_df.shape[0] > limit:
        fund_df = fund_df.iloc[-limit:]
    return fund_df


def get_stg_indexes():
    """获取全部策略指数走势"""
    sql_str = """select iv.index_name, nav_date_friday, value
from strategy_index_val iv,
(
select index_name, adddate(nav_date, 4 - weekday(nav_date)) as nav_date_friday, max(nav_date) nav_date_max from strategy_index_val group by index_name, nav_date_friday
) ivf
where iv.index_name=ivf.index_name and iv.nav_date = ivf.nav_date_max"""
    engine = get_db_engine()
    all_index_df = pd.read_sql(sql_str, engine)
    stg_index_df = all_index_df.pivot(index='nav_date_friday', columns='index_name', values='value')
    stg_index_df.ffill(inplace=True)
    return stg_index_df


def get_alpha_index():
    """获取alpha策略指数及沪深300指数对比走势"""
    return get_stg_index('alpha', '000300.SH')


def get_fixed_income_index():
    """获取固定收益及中债总财富指数对比走势"""
    return get_stg_index('fixed_income', '037.CS')


def get_stg_index(index_name, wind_code):
    """
    获取策略指数及对比指数的的对比走势
    :param index_name: 策略指数名称
    :param wind_code: 对比的指数wind_code
    :return:
    """
    sql_str = """select nav_date, value, close from
(SELECT * FROM strategy_index_val where index_name=%s) stg
left outer join
(select * from wind_index_daily where wind_code = %s) idx
on stg.nav_date = idx.trade_date
order by nav_date"""
    engine = get_db_engine()
    alpha_index_df = pd.read_sql(sql_str, engine, params=[index_name, wind_code])
    alpha_index_df[wind_code] = alpha_index_df['CLOSE'] / alpha_index_df['CLOSE'][0]
    alpha_index_df.ffill(inplace=True)
    alpha_index_df.rename(columns={'value': index_name}, inplace=True)
    return alpha_index_df[['nav_date', index_name, wind_code]]


def update_fof_stg_pct(wind_code_p):
    """
    根据子基金投资额及子基金策略比例调整fof基金总体策略比例
    :param wind_code_p: fof基金代码
    :return: 
    """
    # 获取子基金投资额
    # sql_str = "select wind_code_s, date_adj, invest_scale from fof_fund_pct where wind_code_p = %s"
    sql_str = """select ffm.wind_code, date_adj, sum(invest_scale) invest_scale
from fof_fund_pct ffp,
fund_essential_info ffm
where ffp.wind_code_p = %s
and ffp.wind_code_s = ffm.wind_code_s
GROUP BY ffm.wind_code, date_adj"""
    engine = get_db_engine()
    data_df = pd.read_sql(sql_str, engine, params=[wind_code_p])
    if data_df.shape[0] == 0:
        logger.warning('%s 没有找到子基金策略信息')
        return
    fof_fund_df = data_df.pivot(columns='wind_code', index='date_adj', values='invest_scale')
    fof_fund_pct_df = fof_fund_df.fillna(0) / fof_fund_df.sum(axis=1).values.repeat(fof_fund_df.shape[1]).reshape(fof_fund_df.shape)
    # 获取子基金策略比例
    sql_str = """select DISTINCT ffm.wind_code, stg_code, trade_date, stg_pct 
from fund_stg_pct sp,
fund_essential_info ffm,
(select wind_code_s from fof_fund_pct where wind_code_p = %s group by wind_code_s) fp
 where sp.wind_code = ffm.wind_code
 and ffm.wind_code_s = fp.wind_code_s;"""
    data_df = pd.read_sql(sql_str, engine, params=[wind_code_p])
    if data_df.shape[0] == 0:
        logger.warning('获取%s子基金策略比例失败', wind_code_p)
        return
    fund_stg_df = data_df.set_index(['wind_code', 'stg_code', 'trade_date']).unstack().T.copy()
    # fund_stg_df.set_index(fund_stg_df.index.levels[1], inplace=True)
    # fund_stg_df.index = [dt.date() for dt in fund_stg_df.index]
    # 策略比例df日期轴格式转换
    fund_stg_df.index = [dt.date() for dt in fund_stg_df.index.levels[1]]
    # 与 子基金比例 df 进行比对，补充日期轴
    add_date_df = pd.DataFrame(columns=fund_stg_df.columns, index=list(set(fof_fund_pct_df.index) - set(fund_stg_df.index)))
    fund_stg_df = fund_stg_df.append(add_date_df).sort_index()
    # 按列逐级寻找为空列 插 0 # 2017-11-14 忘记当初为什么写这个功能，引起了比例为0 的错误，现在注释掉
    # for code in fund_stg_df.columns.levels[0]:
    #     is_fill_rows = fund_stg_df[code].sum(axis=1) != 0
    #     for stg_code in fund_stg_df[code].columns:
    #         is_fill_rows_col = np.isnan(fund_stg_df[code][stg_code])
    #         fill_df = fund_stg_df.loc[is_fill_rows & is_fill_rows_col, code]
    #         fill_df[stg_code] = 0
    #         fund_stg_df.loc[is_fill_rows & is_fill_rows_col, code] = fill_df.values
    fund_stg_df.ffill(inplace=True)
    fund_stg_df.fillna(0, inplace=True)

    # 按日循环、调整各基金、各策略比例
    adj_date_last = None
    fund_stg_df_index = fund_stg_df.index
    wind_code_list = list(fund_stg_df.columns.levels[0])
    fof_fund_df_index = fof_fund_pct_df.index
    for trade_date in fund_stg_df_index:
        if trade_date in fof_fund_df_index:
            adj_date_last = trade_date
        if adj_date_last is None:
            fund_stg_df.loc[trade_date, :] = 0
            continue
        for code in wind_code_list:
            fund_stg_df.loc[trade_date, code] = (fund_stg_df.loc[trade_date, code] * fof_fund_pct_df[code][adj_date_last]).values
    fund_stg_pct_df = fund_stg_df.sum(level=1, axis=1)
    fund_stg_pct_df = fund_stg_pct_df.drop(fund_stg_pct_df.index[fund_stg_pct_df.sum(axis=1) == 0])
    fund_stg_pct_df = fund_stg_pct_df.unstack().reset_index()
    fund_stg_pct_df['wind_code'] = wind_code_p
    fund_stg_pct_df.rename(columns={'level_1': 'trade_date', 0: 'stg_pct'}, inplace=True)
    # fund_stg_pct_df.set_index('wind_code', inplace=True)
    # 清除原有记录
    logger.info('调整基金策略比例信息：')
    for trade_date, df in fund_stg_pct_df.groupby('trade_date'):
        logger.info('交易日：%s 策略比例如下：\n%s', trade_date, df)
    with get_db_session(engine) as session:
        session.execute('delete from fund_stg_pct where wind_code = :wind_code', params={'wind_code': wind_code_p})
    fund_stg_pct_df_available = fund_stg_pct_df[fund_stg_pct_df['stg_pct'] > 0]
    if fund_stg_pct_df_available.shape[0] == 0:
        logger.warning('%s 没有子基金策略比例数据', wind_code_p)
        return
    # 插入最新fof基金策略比例
    fund_stg_pct_df_available.to_sql('fund_stg_pct', engine, if_exists='append', index=False)


def stat_fund(wind_code):
    """
    统计基金的绩效指标
    :param wind_code: 
    :return: 
    """
    engine = get_db_engine()
    nav_df = pd.read_sql("select nav_date, nav_acc from fund_nav where wind_code=%s", engine, params=[wind_code], index_col='nav_date')
    stat_df = return_risk_analysis(nav_df)  # , freq='daily'
    return stat_df


def stat_period_fof_fund(wind_code, date_from, date_to):
    """
    统计FOF及子基金日期区间内的净值变化及收益贡献度
    sql 查询语句
    set @wind_code='FHF-101701', @date_from='2017-9-1', @date_to='2017-10-31';
    
SELECT fn_range.wind_code, fi.sec_name, ffp.invest_scale, fn_range.nav_date_from,fn_range.nav_date_to, 
nav_from.nav_acc nav_from, nav_to.nav_acc nav_to, 
(nav_to.nav_acc - nav_from.nav_acc) / nav_from.nav_acc * 100 pct_change
from 
(
	select fn_from.wind_code, fn_from.nav_date nav_date_from, fn_to.nav_date nav_date_to from 
	(
		select wind_code, max(nav_date) nav_date
		from fund_nav 
		where wind_code in (
			select wind_code_s FROM
			fof_fund_pct ffp
			where ffp.wind_code_p = @wind_code
			and ffp.date_adj = (select max(date_adj) from fof_fund_pct ffp_sub where ffp_sub.wind_code_p = @wind_code and ffp_sub.date_adj < @date_to)
			)
		and nav_date <= @date_to
		group by wind_code
	) fn_to
	right JOIN
	(
		select wind_code, max(nav_date) nav_date
		from fund_nav 
		where wind_code in (
			select wind_code_s FROM
			fof_fund_pct ffp
			where ffp.wind_code_p = @wind_code
			and ffp.date_adj = (select max(date_adj) from fof_fund_pct ffp_sub where ffp_sub.wind_code_p = @wind_code and ffp_sub.date_adj < @date_to)
			)
		and nav_date < @date_from
		group by wind_code
	) fn_from
	on fn_from.wind_code = fn_to.wind_code
) fn_range
LEFT JOIN fund_nav nav_from
on fn_range.wind_code = nav_from.wind_code
and fn_range.nav_date_from = nav_from.nav_date
left JOIN fund_nav nav_to
on fn_range.wind_code = nav_to.wind_code
and fn_range.nav_date_to = nav_to.nav_date
LEFT JOIN fund_essential_info fei
on fn_range.wind_code = fei.wind_code_s
LEFT JOIN fund_info fi
on fei.wind_code = fi.wind_code
left JOIN 
(
	select ffp_sub.wind_code_s, ffp_sub.invest_scale 
	from fof_fund_pct ffp_sub 
	where ffp_sub.wind_code_p = @wind_code
	and ffp_sub.date_adj = (select max(date_adj) from fof_fund_pct ffp_sub where ffp_sub.wind_code_p = @wind_code and ffp_sub.date_adj < @date_to)
) ffp
on fn_range.wind_code = ffp.wind_code_s
    :param wind_code: 
    :param date_from: 
    :param date_to: 
    :return: 
    """
    sql_fund_str = """SELECT fn_range.wind_code, fi.sec_name, ffp.invest_scale, fn_range.nav_date_from,fn_range.nav_date_to, 
nav_from.nav_acc nav_from, nav_to.nav_acc nav_to, 
(nav_to.nav_acc - nav_from.nav_acc) / nav_from.nav_acc pct_change
from 
(
	select fn_from.wind_code, fn_from.nav_date nav_date_from, fn_to.nav_date nav_date_to from 
	(
		select wind_code, max(nav_date) nav_date
		from fund_nav 
		where wind_code = %s
		and nav_date <= %s
		group by wind_code
	) fn_to
	right JOIN
	(
		select wind_code, max(nav_date) nav_date
		from fund_nav 
		where wind_code = %s
		and nav_date < %s
		group by wind_code
	) fn_from
	on fn_from.wind_code = fn_to.wind_code
) fn_range
LEFT JOIN fund_nav nav_from
on nav_from.wind_code = fn_range.wind_code
and nav_from.nav_date = fn_range.nav_date_from
left JOIN fund_nav nav_to
on nav_to.wind_code = fn_range.wind_code
and nav_to.nav_date = fn_range.nav_date_to
LEFT JOIN fund_info fi
on fn_range.wind_code = fi.wind_code
left JOIN 
(
	select ffp_sub.wind_code_p, sum(ffp_sub.invest_scale) invest_scale
	from fof_fund_pct ffp_sub 
	where ffp_sub.wind_code_p = %s
	and ffp_sub.date_adj = (select max(date_adj) from fof_fund_pct ffp_sub where ffp_sub.wind_code_p = %s and ffp_sub.date_adj < %s)
	group by ffp_sub.wind_code_p
) ffp
on fn_range.wind_code = ffp.wind_code_p
"""
    engine = get_db_engine()
    nav_fund_df = pd.read_sql(sql_fund_str, engine, params=[wind_code, date_to, wind_code, date_from, wind_code, wind_code, date_from])
    nav_fund_df['contribue_pct'] = '100%'
    sql_fof_str = """SELECT fn_range.wind_code, fi.sec_name, ffp.invest_scale, fn_range.nav_date_from,fn_range.nav_date_to, 
nav_from.nav_acc nav_from, nav_to.nav_acc nav_to, 
(nav_to.nav_acc - nav_from.nav_acc) / nav_from.nav_acc pct_change
from 
(
	select fn_from.wind_code, fn_from.nav_date nav_date_from, fn_to.nav_date nav_date_to from 
	(
		select wind_code, max(nav_date) nav_date
		from fund_nav 
		where wind_code in (
			select wind_code_s FROM
			fof_fund_pct ffp
			where ffp.wind_code_p = %s
			and ffp.date_adj = (select max(date_adj) from fof_fund_pct ffp_sub where ffp_sub.wind_code_p = %s and ffp_sub.date_adj < %s)
			)
		and nav_date <= %s
		group by wind_code
	) fn_to
	right JOIN
	(
		select wind_code, max(nav_date) nav_date
		from fund_nav 
		where wind_code in (
			select wind_code_s FROM
			fof_fund_pct ffp
			where ffp.wind_code_p = %s
			and ffp.date_adj = (select max(date_adj) from fof_fund_pct ffp_sub where ffp_sub.wind_code_p = %s and ffp_sub.date_adj < %s)
			)
		and nav_date < %s
		group by wind_code
	) fn_from
	on fn_from.wind_code = fn_to.wind_code
) fn_range
LEFT JOIN fund_nav nav_from
on fn_range.wind_code = nav_from.wind_code
and fn_range.nav_date_from = nav_from.nav_date
left JOIN fund_nav nav_to
on fn_range.wind_code = nav_to.wind_code
and fn_range.nav_date_to = nav_to.nav_date
LEFT JOIN fund_essential_info fei
on fn_range.wind_code = fei.wind_code_s
LEFT JOIN fund_info fi
on fei.wind_code = fi.wind_code
left JOIN 
(
	select ffp_sub.wind_code_s, ffp_sub.invest_scale 
	from fof_fund_pct ffp_sub 
	where ffp_sub.wind_code_p = %s
	and ffp_sub.date_adj = (select max(date_adj) from fof_fund_pct ffp_sub where ffp_sub.wind_code_p = %s and ffp_sub.date_adj < %s)
) ffp
on fn_range.wind_code = ffp.wind_code_s
"""
    engine = get_db_engine()
    nav_fof_df = pd.read_sql(sql_fof_str, engine, params=[wind_code, wind_code, date_to, date_to, wind_code, wind_code, date_to, date_from, wind_code, wind_code, date_to])
    profit_s = nav_fof_df['invest_scale'] * nav_fof_df['nav_to'] - nav_fof_df['invest_scale'] * nav_fof_df['nav_from']
    nav_fof_df['contribue_pct'] = (profit_s / abs(profit_s.sum()) * 100).apply(lambda x: '%.2f%%' % x)
    nav_df = pd.concat([nav_fund_df, nav_fof_df]).set_index('wind_code')
    nav_df['pct_change'] = nav_df['pct_change'].apply(lambda x: '%.2f%%' % (x * 100))
    return nav_df


def get_fof_fund_pct_df(wind_code):
    """
    获取指定FOF各个确认日截面持仓情况
    :param wind_code: 
    :return: 
    """
    engine = get_db_engine()
    sql_str = """select ffp.wind_code_s, ffp.date_adj, ffp.invest_scale, fei.sec_name_s
from
	(select wind_code_s, date_adj, invest_scale from fof_fund_pct where wind_code_p=%s) ffp
	LEFT JOIN
	fund_essential_info fei
	on ffp.wind_code_s = fei.wind_code_s"""
    data_df = pd.read_sql(sql_str, engine, params=[wind_code])
    data_df['wind_code_sec_name_s'] = data_df['wind_code_s'].fillna('[NAN]') + data_df['sec_name_s'].fillna('[NAN]')
    date_fund_pct_df = data_df.pivot(columns='wind_code_sec_name_s', index='date_adj', values='invest_scale')
    # print(date_fund_pct_df)
    return date_fund_pct_df

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s: %(levelname)s [%(name)s] %(message)s')
    # wind_code_list = ['XT090739.XT', 'XT090970.XT', 'XT091012.XT']
    # pct_df = get_fund_nav(wind_code_list, '2016-12-1', '2017-3-31')
    # logger.info('pct_df\n', pct_df)
    # wind_code = 'QHZG004083.OF'
    # wind_code = 'FHF-101601'
    # from_date, to_date = '2016-1-1', '2017-5-18'
    # ret_dic = get_fund_nav_between(wind_code, from_date, to_date)
    # ret_dic = get_fund_nav_between('FHF-101701', '2017-3-27', '2017-7-28', index_code='000905.SH')
    # logger.info('ret_dic\n %r', ret_dic)
    # ret_dic['fund_df'].to_csv('fund_df.csv')
    # logger.info('pct_df\n', ret_dic['pct_df'])
    # logger.info('index_rr\n', index_rr)
    # logger.info('date_latest\n', date_latest)
    # pct_df.plot()
    # plt.show()

    # 计算基金的绩效指标
    # wind_code = 'FHF-101701'
    # stat_df = stat_fund(wind_code)
    # logger.debug('%s return_risk_analysis:\n%s', wind_code, stat_df)
    # stat_df.to_csv('%s return_risk_analysis.csv' % wind_code)

    # 统计日期区间段内FOF及子基金净值变化
    # wind_code, date_from, date_to = 'FHF-101701', '2017-9-1', '2017-10-20'
    # nav_df = stat_period_fof_fund(wind_code, date_from, date_to)
    # nav_df.to_csv('%s stat_period_fof_fund.csv' % wind_code)
    # print(nav_df)

    # 获取母基金及子基金走势
    # wind_code = 'XT1619361.XT'  # 'FHF-101601' 'XT1605537.XT' 'FHF-101602' 'FHF-101701B'
    # from_date, to_date = '2016-10-8', '2017-9-8'
    # ret_dic = get_fof_nav_rr_between(wind_code, from_date, to_date)
    # ret_dic = get_fof_nav_between(wind_code, from_date, to_date)
    # pct_df, date_latest = ret_dic['fund_df'], ret_dic['date_latest']
    # logger.info(pct_df)
    # pct_df.to_csv('%s.csv' % wind_code)
    # pct_df.plot(legend=False)
    # plt.show()

    #
    # fund_df = get_fund_nav_by_wind_code(wind_code, limit=0)
    # logger.info(fund_df)

    # 获取策略指数及对比指数的的对比走势
    # stg_index_df = get_stg_indexes()
    # logger.info(stg_index_df)
    # index_df = get_alpha_index()
    # logger.info(index_df)

    # 根据子基金投资额及子基金策略比例调整fof基金总体策略比例
    # wind_code = 'FHF-101701'  # 'FHF-101601'  'FHF-101701'
    # update_fof_stg_pct(wind_code)

    # 获取指定FOF各个确认日截面持仓情况
    # wind_code = 'FHF-101601'  # 'FHF-101601'  'FHF-101701'
    # date_fund_pct_df = get_fof_fund_pct_df(wind_code)
    # print(date_fund_pct_df)
    # date_fund_pct_df.to_csv('%s date_fund_pct_df.csv' % wind_code)

    # 获取 每一个净值日期的子基金配比记录
    wind_code_p = 'FHF-101601'
    nav_df, nav_date_fund_scale_df = get_fof_fund_pct_each_nav_date(wind_code_p)
    print(nav_date_fund_scale_df)