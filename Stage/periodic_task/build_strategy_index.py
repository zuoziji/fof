# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np
from config_fh import get_db_engine, get_db_session, get_cache_file_path, STR_FORMAT_DATE
from fh_tools.fh_utils import return_risk_analysis, str_2_date
from fh_tools import fh_utils
import matplotlib.pyplot as plt  # pycharm 需要通过现实调用 plt.show 才能显示plot
from datetime import date, datetime, timedelta
from sqlalchemy.types import String, Date, FLOAT
import datetime as dt
import logging

logger = logging.getLogger()
STRATEGY_TYPE_CN_EN_DIC = {'债券策略': 'fixed_income',
                           '套利策略': 'arbitrage',
                           '管理期货策略': 'cta',
                           '股票多头策略': 'long_only',
                           '阿尔法策略': 'alpha',
                           '宏观策略': 'macro',
                           '组合基金策略': 'fof'}
STRATEGY_TYPE_EN_CN_DIC = {en: cn for cn, en in STRATEGY_TYPE_CN_EN_DIC.items()}


def calc_wind_code_list_index(wind_code_list, date_since, file_name=None):
    """
    计算 wind_code_list 组成的指数
    :param wind_code_list:
    :param date_since:
    :param file_name: 默认为None，不生成文件
    :return: 合成后的指数每日收益率列表
    """
    # 获取样本子基金行情数据
    wind_code_list_str = ', '.join(["'" + wind_code + "'" for wind_code in wind_code_list])
    query_base_str = r'''select fv.wind_code, nav_date_week, fv.nav_acc
from (
        select wind_code, adddate(nav_date, 4 - weekday(nav_date)) as nav_date_week, max(nav_date) as nav_date_max
        from fund_nav
        where wind_code in (%s)
        group by wind_code, nav_date_week
        ) as ffv,
        fund_nav fv
    where ffv.nav_date_week >= %s
    and fv.wind_code = ffv.wind_code
    and fv.nav_date = ffv.nav_date_max
    group by wind_code, nav_date_week
    order by nav_date_week desc'''
    query_str = query_base_str % (wind_code_list_str, date_since)
    # logger.info(query_str)
    engine = get_db_engine()
    fund_nav_df = pd.read_sql_query(query_str, engine)
    # 获取样本子基金名称
    sql_str = """select wind_code, sec_name 
from fund_info 
where wind_code in (%s)"""
    query_str = sql_str % wind_code_list_str
    with get_db_session(engine) as session:
        table = session.execute(query_str)
        fund_code_name_dic = dict(table.fetchall())
    # logger.info(df_fund_nav)
    df_fund = fund_nav_df.pivot(index='nav_date_week', columns='wind_code', values='nav_acc')
    df_fund.rename(columns=fund_code_name_dic, inplace=True)
    # df_fund.to_csv('%s-%s【%d】 %s_%s.csv' % (strategy_name, sample_name, len(wind_code_list), date_from, date_to))
    df_fund.interpolate(inplace=True)
    df_fund.dropna(inplace=True)
    wind_code_list = list(df_fund.columns)
    wind_code_count = len(wind_code_list)
    if wind_code_count == 0:
        logger.info('wind_code_list_str has no data')

    # df_fund.to_csv('%s_df_fund.csv' % sample_name)
    weight = 1 / wind_code_count
    # logger.info(df_fund)
    fund_pct_df = df_fund.pct_change().fillna(0)
    if file_name is not None:
        file_path = get_cache_file_path(file_name)
        fund_index_df = (1 + fund_pct_df).cumprod()
        fund_index_df.to_csv(file_path)
    fund_pct_df *= weight
    # logger.info(df_fund_pct)
    nav_index_pct_s = None
    for wind_code in wind_code_list:

        if nav_index_pct_s is None:
            nav_index_pct_s = fund_pct_df[wind_code]
        else:
            nav_index_pct_s += fund_pct_df[wind_code]
            # logger.info("df_nav_index_pct_s[%s]:\n" % wind_code, df_nav_index_pct_s)
    date_list = list(fund_pct_df.index)
    if len(date_list) == 0:
        file_path = get_cache_file_path('df_fund_%s_%s.csv' % (file_name, date_since))
        logger.info('子基金净值日期交集为空, 参见 %s文件查看具体数据', file_path)
        df_fund.to_csv(file_path)
    logger.info('between: %s ~ %s', min(date_list), max(date_list))
    return nav_index_pct_s


def calc_strategy_index(strategy_name, date_from, date_to, calc_sample_name=None, create_sub_index_csv=False):
    """
    计算策略指数
    根据策略名称，提取策略样本基金代码，等权重捏合指数
    :param strategy_name:策略名称
    :param date_from:起始日期
    :param date_to:截止日期
    :param calc_sample_name: 需要计算的 sample_name。'main'为主指数，none为全部样本指数。
    :return:返回主指数，及其他样本指数的df
    """
    # logger.info('strategy %s between: %s %s', strategy_name, date_from, date_to)
    with get_db_session() as session:
        # 获取 nav_date 列表
        stg_table = session.execute(
            'SELECT nav_date_week, wind_code_str, sample_name FROM strategy_index_info where strategy_name=:stg_name order by nav_date_week desc',
            {'stg_name': strategy_name})
        date_last = None
        index_pct_s = None
        sample_name_list = []
        sample_val_list = []
        stg_table_data_list = []
        for stg_info in stg_table.fetchall():
            # date_since = stg_info[0]
            # wind_code_str = stg_info[1]
            sample_name = stg_info[2]
            # logger.info('stg_info %s', stg_info)
            if calc_sample_name is not None and sample_name != calc_sample_name:
                continue
            stg_table_data_list.append(
                {'nav_date_week': stg_info[0], 'wind_code_str': stg_info[1], 'sample_name': sample_name})
        stg_table_df = pd.DataFrame(stg_table_data_list)
        logger.debug('stg_table_df.shape %s', stg_table_df.shape)
        stg_table_df_gp = stg_table_df.groupby('sample_name')
        stg_table_df_gp_dic = stg_table_df_gp.groups
        for sample_name, row_num_list in stg_table_df_gp_dic.items():
            index_pct_s = None
            date_last = None
            for row_num in row_num_list:
                wind_code_str = stg_table_df.iloc[row_num]['wind_code_str']
                date_since = stg_table_df.iloc[row_num]['nav_date_week']
                wind_code_list = wind_code_str.split(sep=',')
                if create_sub_index_csv:
                    file_name = '%s_%s_since_%s.csv' % (strategy_name, sample_name, date_since)
                else:
                    file_name = None
                nav_index_pct_s = calc_wind_code_list_index(wind_code_list, date_since, file_name)
                logger.debug('%s\n%s', sample_name, nav_index_pct_s)
                if date_last is None:
                    date_available = [d for d in nav_index_pct_s.index if date_from <= d <= date_to and date_since <= d]
                else:
                    date_available = [d for d in nav_index_pct_s.index if
                                      date_from <= d <= date_to and date_since <= d < date_last]
                date_last = date_since
                if index_pct_s is None:
                    index_pct_s = nav_index_pct_s.ix[date_available]
                else:
                    index_pct_s.append(nav_index_pct_s.ix[date_available])
            # logger.info(sample_name, '\n', index_pct_s)
            sample_val_s = (1 + index_pct_s).cumprod()
            sample_name_list.append(sample_name)
            sample_val_list.append(sample_val_s)
            # sample_val_s.to_csv('%s %s_%s.csv' % (strategy_name, date_from, date_to))
    if len(sample_val_list) == 0:
        index_df = None
    else:
        index_df = pd.DataFrame(sample_val_list, index=sample_name_list).T
        index_df.rename(columns={'main': strategy_name}, inplace=True)
        index_df.interpolate(inplace=True)
    return index_df


def update_strategy_index(date_from_str, date_to_str):
    """
    strategy_index_info 中所有 strategy_name 更新指数净值到数据库 strategy_index_val 中
    :param date_from_str:  起始日期 %Y-%m-%d
    :param date_to_str: 截止日期 %Y-%m-%d
    :return:
    """
    engine = get_db_engine()
    with get_db_session(engine) as session:
        stg_table = session.execute('select strategy_name from strategy_index_info group by strategy_name')
        strategy_name_list = [stg_info[0] for stg_info in stg_table.fetchall()]
    strategy_name_count = len(strategy_name_list)
    if strategy_name_count == 0:
        logger.info('strategy_index_info table is empty')
        return
    # strategy_name_list = ['long_only', 'cta', 'arbitrage', 'alpha', 'macro']
    date_from = datetime.strptime(date_from_str, '%Y-%m-%d').date()
    date_to = datetime.strptime(date_to_str, '%Y-%m-%d').date()
    index_df_list = []
    for strategy_name in strategy_name_list:
        # strategy_name = 'long_only'
        # index_df = calc_strategy_index(strategy_name, date_from, date_to, calc_sample_name='main')
        stg_index_s = get_strategy_index_by_name(strategy_name, date_from, date_to, statistic=False)
        if stg_index_s is not None:
            logger.info('生成%s策略指数【%s ~ %s】', strategy_name, stg_index_s.index[0], stg_index_s.index[-1])
            # index_df.to_csv('%s_sample_%s_%s.csv' % (strategy_name, date_from, date_to))
            index_df = pd.DataFrame({'value': stg_index_s})
            index_df.index.rename('nav_date', inplace=True)
            index_df.reset_index(inplace=True)
            # index_df.rename(columns={'nav_date_week': 'nav_date', strategy_name: 'value'}, inplace=True)
            index_df['index_name'] = strategy_name
            index_df_list.append(index_df)
        else:
            logger.info('No Data for shown on %s', strategy_name)
    index_df_all = pd.concat(index_df_list)
    index_df_all.set_index(['index_name', 'nav_date'], inplace=True)

    # 重置内容
    table_name = 'strategy_index_val'
    with get_db_session(engine) as session:
        # session.execute("delete from %s where nav_date between '%s' and '%s'" % (table_name, date_from_str, date_to_str))
        session.execute("truncate table %s" % table_name)
    index_df_all.to_sql(table_name, engine, if_exists='append',
                        dtype={
                            'index_name': String(20),
                            'nav_date': Date,
                            'value': FLOAT,
                        }
                        )


def stat_fund_by_stg(strategy_type, date_from, date_to):
    """
    统计制定日期段内策略表现情况，包括：样本数、胜率，1%以上、-1%以下占比等
    :param strategy_type: 
    :param date_from: 
    :param date_to: 
    :return: 
    """
    sql_str = """select fv.wind_code, nav_date_week, fv.nav_acc
from (
        select wind_code, adddate(nav_date, 4 - weekday(nav_date)) as nav_date_week, max(nav_date) as nav_date_max
        from fund_nav
        where wind_code in (select wind_code from fund_info where strategy_type = '%s')
        group by wind_code, nav_date_week
        having nav_date_week between '%s' and '%s'
        ) as ffv,
        fund_nav fv
    where ffv.nav_date_week between '%s' and '%s'
    and fv.wind_code = ffv.wind_code
    and fv.nav_date = ffv.nav_date_max
    group by wind_code, nav_date_week
    order by nav_date_week desc"""
    query_str = sql_str % (strategy_type, date_from, date_to, date_from, date_to)
    engine = get_db_engine()
    fund_nav_df = pd.read_sql_query(query_str, engine)
    df_fund = fund_nav_df.pivot(index='nav_date_week', columns='wind_code', values='nav_acc')

    # 获取样本子基金名称
    sql_str = """select wind_code, sec_name 
    from fund_info 
    where wind_code in (%s)"""
    wind_code_list_str = ', '.join(["'" + wind_code + "'" for wind_code in list(df_fund.columns)])
    query_str = sql_str % wind_code_list_str
    with get_db_session(engine) as session:
        table = session.execute(query_str)
        fund_code_name_dic = dict(table.fetchall())
    # logger.info(df_fund_nav)
    df_fund.rename(columns=fund_code_name_dic, inplace=True)
    # df_fund.to_csv('%s-%s【%d】 %s_%s.csv' % (strategy_name, sample_name, len(wind_code_list), date_from, date_to))
    df_fund.interpolate(inplace=True)
    df_fund.dropna(axis=1, inplace=True)
    wind_code_list = list(df_fund.columns)
    wind_code_count = len(wind_code_list)
    if wind_code_count == 0:
        logger.info('wind_code_list_str has no data')

    weight = 1 / wind_code_count
    # logger.info(df_fund)
    fund_pct_df = df_fund.pct_change().fillna(0)
    fund_comprod_df = (1 + fund_pct_df).cumprod()
    fund_comprod_df = fund_comprod_df[fund_comprod_df.columns[fund_comprod_df.max() != fund_comprod_df.min()]]
    date_list = list(fund_pct_df.index)
    win_count = (fund_comprod_df.iloc[-1] > 1).sum()
    win_1_count = (fund_comprod_df.iloc[-1] > 1.01).sum()
    loss_1_count = (fund_comprod_df.iloc[-1] < 0.99).sum()
    logger.info('%s 统计日期: %s ~ %s', strategy_type, min(date_list), max(date_list))
    logger.info('完整公布业绩数据的%d 只基金中', wind_code_count)
    logger.info('获得正收益的产品有%d 只，占比%3.1f%%', win_count, win_count / wind_code_count * 100)
    logger.info('收益超过1%%的产品有%d 只，占比%3.1f%%', win_1_count, win_1_count / wind_code_count * 100)
    logger.info('亏损超过-1%%的有%d 只，占比%3.1f%%', loss_1_count, loss_1_count / wind_code_count * 100)

    fund_index_df = fund_comprod_df.mean(axis=1)
    fund_comprod_df[strategy_type] = fund_index_df

    file_path = get_cache_file_path('%s %s.csv' % (strategy_type, date_to))
    fund_comprod_df.to_csv(file_path)

    file_path = get_cache_file_path('%s index %s.csv' % (strategy_type, date_to))
    fund_index_df.to_csv(file_path)


def filter_wind_code(fund_nav_df, strategy_type_en):
    """债券策略指数中存在很多不符合标准的基金，因此需要以 strategy_index_info 中保持的列表为基准"""
    query_str = "select wind_code_str from strategy_index_info where strategy_name = :strategy_type"
    with get_db_session() as session:
        row_data = session.execute(query_str, {'strategy_type': strategy_type_en}).fetchone()
        if row_data is not None and len(row_data) > 0:
            wind_code_str = row_data[0]
            if wind_code_str is not None and len(wind_code_str) > 0:
                wind_code_list = wind_code_str.split(',')
                wind_code_list = list(set(list(fund_nav_df.columns)) & set(wind_code_list))
                fund_nav_df = fund_nav_df[wind_code_list]
    return fund_nav_df


def get_fund_nav_weekly_by_strategy(strategy_type_en, date_from, date_to,
                                    show_fund_name=False, do_filter_wind_code=False):
    """
    输入策略代码，起止日期，返回该策略所有基金周净值
    :param strategy_type_en: 
    :param date_from: 
    :param date_to: 
    :param show_fund_name: 
    :return: 
    """
    global STRATEGY_TYPE_EN_CN_DIC
    sql_str = """select fv.wind_code, nav_date_week, fv.nav_acc
    from (
        select wind_code, adddate(nav_date, 4 - weekday(nav_date)) as nav_date_week, max(nav_date) as nav_date_max
        from fund_nav
        where wind_code in (select wind_code from fund_info where strategy_type = '%s')
        group by wind_code, nav_date_week
        having nav_date_week between '%s' and '%s'
        ) as ffv,
        fund_nav fv
    where ffv.nav_date_week between '%s' and '%s'
    and fv.wind_code = ffv.wind_code
    and fv.nav_date = ffv.nav_date_max
    group by wind_code, nav_date_week
    order by nav_date_week desc"""
    strategy_name_cn = STRATEGY_TYPE_EN_CN_DIC[strategy_type_en]
    query_str = sql_str % (strategy_name_cn, date_from, date_to, date_from, date_to)
    # logger.debug('策略子基金净值查询sql：\n%s', query_str)
    engine = get_db_engine()
    data_df = pd.read_sql_query(query_str, engine)
    fund_nav_df = data_df.pivot(index='nav_date_week', columns='wind_code', values='nav_acc')
    # 筛选子基金列表
    if do_filter_wind_code:
        fund_nav_df = filter_wind_code(fund_nav_df, strategy_type_en)

    if show_fund_name:
        # 获取样本子基金名称
        sql_str = "select wind_code, sec_name from fund_info where wind_code in (%s)"
        wind_code_list_str = ', '.join(["'" + wind_code + "'" for wind_code in list(fund_nav_df.columns)])
        query_str = sql_str % wind_code_list_str
        with get_db_session(engine) as session:
            table = session.execute(query_str)
            fund_code_name_dic = dict(table.fetchall())
        # logger.info(df_fund_nav)
        fund_nav_df.rename(columns=fund_code_name_dic, inplace=True)
    return fund_nav_df


def get_strategy_index_quantile(strategy_type_en, date_from, date_to,
                                q_list=[0.05, 0.10, 0.25, 0.50, 0.75, 0.90, 0.95], do_filter=True):
    """
    返回策略指数 quantile 走势，市场策略统计及市场回顾功能使用
    :param strategy_type_en: 
    :param date_from: 
    :param date_to: 
    :param q_list: 默认 [0.05, 0.10, 0.25, 0.50, 0.75, 0.90, 0.95]
    :return: 
    """
    fund_nav_df = get_fund_nav_weekly_by_strategy(strategy_type_en, date_from, date_to, show_fund_name=True)
    fund_nav_df = fh_utils.DataFrame.interpolate_inner(fund_nav_df)
    fund_pct_df = fund_nav_df.pct_change().fillna(0)

    # 超过 6次净值不变则剔除
    if do_filter:
        column_name_list = []
        for name in fund_pct_df.columns:
            fund_pct_s = fund_pct_df[name]
            if len(fund_pct_s.shape) == 1 and (fund_pct_s == 0).sum() < 6:
                column_name_list.append(name)
        fund_comprod_df = (1 + fund_pct_df[column_name_list]).cumprod()
    else:
        fund_comprod_df = (1 + fund_pct_df).cumprod()
    if fund_comprod_df.shape[0] == 0:
        df_rr_df = None
    else:
        df_rr_df = fund_comprod_df.quantile(q_list, axis=1).T
        df_rr_df.rename(columns={ff: '%3.2f分位' % (ff) for ff in q_list}, inplace=True)
    # fund_comprod_df.to_csv('fund_comprod_df.csv')
    return df_rr_df


def get_strategy_index_hist(strategy_type_en, date_from, date_to):
    """
    返回策略指数区间收益率分布
    :param strategy_type_en: 
    :param date_from: 
    :param date_to: 
    :param q_list: 默认 [0.05, 0.10, 0.25, 0.50, 0.75, 0.90, 0.95]
    :return: 
    """
    fund_nav_df = get_fund_nav_weekly_by_strategy(strategy_type_en, date_from, date_to)
    fund_nav_df = fh_utils.DataFrame.interpolate_inner(fund_nav_df)
    fund_pct_df = fund_nav_df.pct_change().fillna(0)
    # 超过 6次净值不变则剔除
    column_name_list = []
    for name in fund_pct_df.columns:
        fund_pct_s = fund_pct_df[name]
        if len(fund_pct_s.shape) == 1 and (fund_pct_s == 0).sum() < 6:
            column_name_list.append(name)
    fund_comprod_df = (1 + fund_pct_df[column_name_list]).cumprod()
    fund_rr_s = fund_comprod_df.iloc[:, -1]
    y, x, patches = plt.hist(fund_rr_s, bins=10)
    return y, x


def get_strategy_index_by_name(strategy_type_en, date_from, date_to, statistic=True, create_csv=False):
    """
    统计制定日期段内策略表现情况，包括：样本数、胜率，1%以上、-1%以下占比等
    :param strategy_type_en: 
    :param date_from: 
    :param date_to: 
    :return: 
    """
    fund_nav_df = get_fund_nav_weekly_by_strategy(strategy_type_en, date_from, date_to, show_fund_name=True)
    fund_nav_df.interpolate(inplace=True)
    fund_nav_df.dropna(axis=1, inplace=True)
    wind_code_list = list(fund_nav_df.columns)
    wind_code_count = len(wind_code_list)
    if wind_code_count == 0:
        logger.info('wind_code_list_str has no data')
    weight = 1 / wind_code_count
    # logger.info(df_fund)
    fund_pct_df = fund_nav_df.pct_change().fillna(0)
    # 超过 6次净值不变则剔除
    column_name_list = []
    for name in fund_pct_df.columns:
        fund_pct_s = fund_pct_df[name]
        if len(fund_pct_s.shape) == 1 and (fund_pct_s == 0).sum() < 6:
            column_name_list.append(name)
    # column_name_list = [name for name in fund_pct_df.columns if
    #                     (type((fund_pct_df[name] == 0).sum()) is np.int64) and ((fund_pct_df[name] == 0).sum() < 6)]
    fund_comprod_df = (1 + fund_pct_df[column_name_list]).cumprod()
    # fund_comprod_df = fund_comprod_df[fund_comprod_df.columns[fund_comprod_df.max() != fund_comprod_df.min()]]
    stg_statistic_dic = {}
    if statistic:
        date_list = list(fund_pct_df.index)
        win_count = (fund_comprod_df.iloc[-1] > 1).sum()
        win_1_count = (fund_comprod_df.iloc[-1] > 1.01).sum()
        loss_1_count = (fund_comprod_df.iloc[-1] < 0.99).sum()
        strategy_name_cn = STRATEGY_TYPE_EN_CN_DIC[strategy_type_en]
        print('%s 统计区间: %s ~ %s' % (strategy_name_cn, min(date_list), max(date_list)))
        print('完整公布业绩数据的%d 只基金中' % (wind_code_count))
        print('获得正收益的产品有%d 只，占比%3.1f%%' % (win_count, win_count / wind_code_count * 100))
        print('收益超过1%%的产品有%d 只，占比%3.1f%%' % (win_1_count, win_1_count / wind_code_count * 100))
        print('亏损超过-1%%的有%d 只，占比%3.1f%%' % (loss_1_count, loss_1_count / wind_code_count * 100))
        stg_statistic_dic['统计起始日期'] = min(date_list)
        stg_statistic_dic['统计截止日期'] = max(date_list)
        stg_statistic_dic['统计数量'] = wind_code_count
        stg_statistic_dic['正收益产品数量'] = win_count
        stg_statistic_dic['正收益产品占比'] = win_count / wind_code_count * 100
        stg_statistic_dic['超1%收益产品数量'] = win_1_count
        stg_statistic_dic['超1%收益产品占比'] = win_1_count / wind_code_count * 100
        stg_statistic_dic['低于-1%收益产品数量'] = loss_1_count
        stg_statistic_dic['低于-1%收益产品占比'] = loss_1_count / wind_code_count * 100

    stg_index_s = fund_comprod_df.mean(axis=1).rename(strategy_name_cn)
    fund_comprod_df[strategy_name_cn] = stg_index_s
    if create_csv:
        file_path = get_cache_file_path('%s %s %s.csv' % (strategy_name_cn, date_from, date_to))
        fund_comprod_df.to_csv(file_path)
        logger.info('output file path: %s', file_path)
    # file_path = get_cache_file_path('%s index %s %s.csv' % (strategy_type, date_from, date_to))
    # fund_index_s.to_csv(file_path)
    logger.debug('子基金净值走势df.shape %s', fund_comprod_df.shape)
    return stg_index_s, stg_statistic_dic


def get_strategy_index_by_name_bck(strategy_type_en, date_from, date_to, statistic=True, create_csv=False):
    """
    统计制定日期段内策略表现情况，包括：样本数、胜率，1%以上、-1%以下占比等
    :param strategy_type_en: 
    :param date_from: 
    :param date_to: 
    :return: 
    """
    fund_nav_df = get_fund_nav_weekly_by_strategy(strategy_type_en, date_from, date_to, show_fund_name=True)
    fund_nav_df.interpolate(inplace=True)
    fund_nav_df.dropna(axis=1, inplace=True)
    wind_code_list = list(fund_nav_df.columns)
    wind_code_count = len(wind_code_list)
    if wind_code_count == 0:
        logger.info('wind_code_list_str has no data')
    weight = 1 / wind_code_count
    # logger.info(df_fund)
    fund_pct_df = fund_nav_df.pct_change().fillna(0)
    # 超过 6次净值不变则剔除
    column_name_list = []
    for name in fund_pct_df.columns:
        fund_pct_s = fund_pct_df[name]
        if len(fund_pct_s.shape) == 1 and (fund_pct_s == 0).sum() < 6:
            column_name_list.append(name)
    # column_name_list = [name for name in fund_pct_df.columns if
    #                     (type((fund_pct_df[name] == 0).sum()) is np.int64) and ((fund_pct_df[name] == 0).sum() < 6)]
    fund_comprod_df = (1 + fund_pct_df[column_name_list]).cumprod()
    # fund_comprod_df = fund_comprod_df[fund_comprod_df.columns[fund_comprod_df.max() != fund_comprod_df.min()]]
    stg_statistic_dic = {}
    if statistic:
        date_list = list(fund_pct_df.index)
        win_count = (fund_comprod_df.iloc[-1] > 1).sum()
        win_1_count = (fund_comprod_df.iloc[-1] > 1.01).sum()
        loss_1_count = (fund_comprod_df.iloc[-1] < 0.99).sum()
        strategy_name_cn = STRATEGY_TYPE_EN_CN_DIC[strategy_type_en]
        print('%s 统计区间: %s ~ %s' % (strategy_name_cn, min(date_list), max(date_list)))
        print('完整公布业绩数据的%d 只基金中' % (wind_code_count))
        print('获得正收益的产品有%d 只，占比%3.1f%%' % (win_count, win_count / wind_code_count * 100))
        print('收益超过1%%的产品有%d 只，占比%3.1f%%' % (win_1_count, win_1_count / wind_code_count * 100))
        print('亏损超过-1%%的有%d 只，占比%3.1f%%' % (loss_1_count, loss_1_count / wind_code_count * 100))
        stg_statistic_dic['统计起始日期'] = min(date_list)
        stg_statistic_dic['统计截止日期'] = max(date_list)
        stg_statistic_dic['统计数量'] = wind_code_count
        stg_statistic_dic['正收益产品数量'] = win_1_count
        stg_statistic_dic['正收益产品占比'] = win_count / wind_code_count * 100
        stg_statistic_dic['超1%收益产品数量'] = win_1_count
        stg_statistic_dic['超1%收益产品占比'] = win_1_count / wind_code_count * 100
        stg_statistic_dic['低于-1%收益产品数量'] = loss_1_count
        stg_statistic_dic['低于-1%收益产品占比'] = loss_1_count / wind_code_count * 100

    stg_index_s = fund_comprod_df.mean(axis=1).rename(strategy_name_cn)
    fund_comprod_df[strategy_name_cn] = stg_index_s
    if create_csv:
        file_path = get_cache_file_path('%s %s %s.csv' % (strategy_name_cn, date_from, date_to))
        fund_comprod_df.to_csv(file_path)
        logger.info('output file path: %s', file_path)
    # file_path = get_cache_file_path('%s index %s %s.csv' % (strategy_type, date_from, date_to))
    # fund_index_s.to_csv(file_path)
    logger.debug('fund_comprod_df.shape %s', fund_comprod_df.shape)
    return stg_index_s, stg_statistic_dic


def build_index_with_strategy_name_list(strategy_name_list, date_from, date_to):
    index_df_list = []
    index_name_list = []
    create_sub_index_csv = True
    sample_name = 'main'
    date_from = datetime.strptime(date_from, '%Y-%m-%d').date()
    date_to = datetime.strptime(date_to, '%Y-%m-%d').date()
    for strategy_name in strategy_name_list:
        logger.info('生成%s策略指数【%s ~ %s】', strategy_name, date_from, date_to)
        index_df = calc_strategy_index(strategy_name, date_from, date_to, sample_name, create_sub_index_csv)
        if index_df is not None:
            file_path = get_cache_file_path('%s_sample_%s_%s.csv' % (strategy_name, date_from, date_to))
            index_df.to_csv(file_path)
            index_df_list.append(index_df)
            index_name_list.append(strategy_name)
            # index_df.plot()
            # plt.show()
        else:
            logger.info('No Data for shown')
    index_all_df = pd.concat(index_df_list, axis=1)
    index_all_df.interpolate(inplace=True)
    file_path = get_cache_file_path('all_index_%s_%s.csv' % (date_from, date_to))
    index_all_df.to_csv(file_path)


def build_index_with_strategy_name(strategy_name, date_from, date_to):
    date_from = datetime.strptime(date_from, '%Y-%m-%d').date()
    date_to = datetime.strptime(date_to, '%Y-%m-%d').date()
    logger.info('生成%s策略指数【%s ~ %s】', strategy_name, date_from, date_to)
    index_df = calc_strategy_index(strategy_name, date_from, date_to, calc_sample_name=None, create_sub_index_csv=False)
    if index_df is not None:
        file_path = get_cache_file_path('%s_sample_%s_%s.csv' % (strategy_name, date_from, date_to))
        index_df.to_csv(file_path)
    else:
        logger.info('No Data for shown')


def SelectByDate(Data, starttime, endtime):
    newdata = Data[(Data.Date >= starttime) & (Data.Date <= endtime)]
    newdata = newdata.reset_index(drop=True)
    return (newdata)


def do_update_strategy_index():
    date_to_str = date.today().strftime(STR_FORMAT_DATE)
    date_from_str = (date.today() - timedelta(days=365)).strftime(STR_FORMAT_DATE)
    update_strategy_index(date_from_str, date_to_str)


def calc_index_by_wind_code_dic(wind_code_dic, date_from_str, date_to_str):
    """
    根据子基金投资比例获得历史加权净值走势
    :param wind_code_dic: 
    :param date_from_str: 
    :param date_to_str: 
    :return: 
    """
    wind_code_list = list(wind_code_dic.keys())
    wind_code_count = len(wind_code_list)
    if wind_code_count == 0:
        return None
    engine = get_db_engine()
    # sql_str = 'select * from fund_nav where wind_code in (%s)' % ("'" + "', '".join(wind_code_list) + "'")
    sql_str = """select fn.wind_code, nav_acc, nav_date_week
from fund_nav fn,
(select wind_code, max(nav_date) nav_date , adddate(nav_date, 4 - weekday(nav_date)) as nav_date_week 
from fund_nav where wind_code in (%s)
group by wind_code, adddate(nav_date, 4 - weekday(nav_date))
) as fnw
where fn.wind_code = fnw.wind_code
and fn.nav_date = fnw.nav_date"""
    sql_str = sql_str % (", ".join([r'%s' for n in range(wind_code_count)]))

    data_df = pd.read_sql(sql_str, engine, params=wind_code_list)
    data_len = data_df.shape[0]
    logger.debug('data_df.shape[0]: %d', data_len)
    if data_len == 0:
        return None
    date_from = str_2_date(date_from_str)
    date_to = str_2_date(date_to_str)
    fund_nav_df = data_df.pivot(columns='wind_code', index='nav_date_week', values='nav_acc')
    logger.debug('fund_nav_df.shape: %s', fund_nav_df.shape)
    # 对各个基金的数据进行内插值
    fh_utils.DataFrame.interpolate_inner(fund_nav_df, inplace=True)
    # 过滤有效日期列表
    date_available = [d for d in fund_nav_df.index if date_from <= d <= date_to]
    fund_nav_df_filter = fund_nav_df.ix[date_available, :]
    # 归一化处理
    fund_nav_pct_df = fund_nav_df_filter.pct_change().fillna(0)
    index_pct_s = None
    wind_code_list = list(fund_nav_pct_df.columns)
    tot_weight = sum([wind_code_dic[wind_code] for wind_code in wind_code_list])
    for wind_code in  wind_code_list:
        pct_s = fund_nav_pct_df[wind_code] * wind_code_dic[wind_code] / tot_weight
        if index_pct_s is None:
            index_pct_s = pct_s
        else:
            index_pct_s += pct_s
    index_nav_s = (index_pct_s + 1).cumprod()
    return index_nav_s


def statistic_fund_by_strategy(date_from_str, date_to_str, create_csv=True, do_filter=True,
                               strategy_type_list=['债券策略', '套利策略', '管理期货策略', '股票多头策略', '阿尔法策略']):
    # strategy_type_list = ['债券策略', '套利策略', '管理期货策略', '股票多头策略', '阿尔法策略']
    stg_index_dic, statistic_dic = {}, {}
    for strategy_type in strategy_type_list:
        strategy_type_en = STRATEGY_TYPE_CN_EN_DIC[strategy_type]
        df_rr_df = get_strategy_index_quantile(strategy_type_en, date_from_str, date_to_str, do_filter=do_filter)
        if df_rr_df is None:
            continue
        if create_csv:
            file_path = get_cache_file_path('%s 分位图 %s %s.csv' % (strategy_type, date_from_str, date_to_str))
            df_rr_df.to_csv(file_path)
        stg_index_s, stg_statistic_dic = get_strategy_index_by_name(strategy_type_en, date_from_str, date_to_str, create_csv=True)
        stg_index_dic[strategy_type] = stg_index_s
        statistic_dic[strategy_type] = stg_statistic_dic
    fund_index_df = pd.DataFrame(stg_index_dic)
    fund_index_df.interpolate(inplace=True)
    statistic_df = pd.DataFrame(statistic_dic)
    if create_csv:
        file_path = get_cache_file_path('策略指数 %s %s.csv' % (date_from_str, date_to_str))
        fund_index_df.to_csv(file_path)
        file_path = get_cache_file_path('策略指数统计 %s %s.csv' % (date_from_str, date_to_str))
        statistic_df.to_csv(file_path)
    stat_df = return_risk_analysis(fund_index_df, date_from_str, date_to_str, freq=50, rf=0.02)
    logger.info('统计区间：%s - %s\n%s', date_from_str, date_to_str, stat_df)

    return stat_df


def calc_fof_index(wind_code_p):
    """
    计算FOF产品净值走势
    根据持仓资金走势按比例计算权重组合收益指数
    :param wind_code_p: 
    :return: 
    """
    engine = get_db_engine()
    # sql_str = 'select wind_code, date_adj, invest_scale from fof_fund_pct where wind_code_p = %s'
    sql_str = """select ffm.wind_code, date_adj, invest_scale 
from fof_fund_pct ffp,
fund_essential_info ffm
where ffp.wind_code_p = %s
and ffp.wind_code_s = ffm.wind_code_s"""
    data_df = pd.read_sql(sql_str, engine, params=[wind_code_p], parse_dates=['date_adj'])
    fund_inv_df = data_df.pivot(columns='date_adj', index='wind_code', values='invest_scale')
    fund_inv_df.columns = [dt.date() for dt in fund_inv_df.columns]
    nav_index_pct_all_s = None
    for dt in fund_inv_df.columns:
        fund_s = fund_inv_df[dt]
        tot_inv = fund_s.sum()
        wind_code_dic = {wind_code: float(invest_scale) / tot_inv for wind_code, invest_scale in fund_s.items()
                         if not np.isnan(invest_scale)}
        nav_index_pct_s = calc_wind_code_weighted_index(wind_code_dic, dt)
        if nav_index_pct_all_s is None:
            nav_index_pct_all_s = nav_index_pct_s
        else:
            nav_index_pct_all_s = nav_index_pct_all_s[nav_index_pct_all_s.index < dt].append(nav_index_pct_s[nav_index_pct_s.index >= dt])
    nav_index_pct_all_s.drop_duplicates(inplace=True)

    return nav_index_pct_all_s


def calc_wind_code_weighted_index(wind_code_dic, date_since, file_name=None):
    """
    计算 wind_code_list 组成的指数
    :param wind_code_list:
    :param date_since:
    :param file_name: 默认为None，不生成文件
    :return: 合成后的指数每日收益率列表
    """
    # 获取样本子基金行情数据
    wind_code_list_str = ', '.join(["'" + wind_code + "'" for wind_code in wind_code_dic])
    query_base_str = r'''select fv.wind_code, nav_date_week, fv.nav_acc
from (
        select wind_code, adddate(nav_date, 4 - weekday(nav_date)) as nav_date_week, max(nav_date) as nav_date_max
        from fund_nav
        where wind_code in (%s)
        group by wind_code, nav_date_week
        ) as ffv,
        fund_nav fv
    where ffv.nav_date_week >= '%s'
    and fv.wind_code = ffv.wind_code
    and fv.nav_date = ffv.nav_date_max
    group by wind_code, nav_date_week
    order by nav_date_week desc'''
    query_str = query_base_str % (wind_code_list_str, date_since)
    # print(query_str)
    engine = get_db_engine()
    fund_nav_df = pd.read_sql_query(query_str, engine)
    # 获取样本子基金名称
    sql_str = """select wind_code, sec_name 
from fund_info 
where wind_code in (%s)"""
    query_str = sql_str % wind_code_list_str
    with get_db_session(engine) as session:
        table = session.execute(query_str)
        fund_code_name_dic = dict(table.fetchall())
    # print(df_fund_nav)
    df_fund = fund_nav_df.pivot(index='nav_date_week', columns='wind_code', values='nav_acc')
    df_fund.rename(columns=fund_code_name_dic, inplace=True)
    weight_dic = {fund_name: wind_code_dic[wind_code] for wind_code, fund_name in fund_code_name_dic.items()}
    # df_fund.to_csv('%s-%s【%d】 %s_%s.csv' % (strategy_name, sample_name, len(wind_code_list), date_from, date_to))
    df_fund.interpolate(inplace=True)
    df_fund.dropna(inplace=True)
    fund_name_list = list(df_fund.columns)
    wind_code_count = len(fund_name_list)
    if wind_code_count == 0:
        print('wind_code_list_str has no data')

    # df_fund.to_csv('%s_df_fund.csv' % sample_name)
    weight = 1 / wind_code_count
    # print(df_fund)
    fund_pct_df = df_fund.pct_change().fillna(0)
    if file_name is not None:
        file_path = get_cache_file_path(file_name)
        fund_index_df = (1 + fund_pct_df).cumprod()
        fund_index_df.to_csv(file_path)
    # fund_pct_df *= weight
    # print(df_fund_pct)
    nav_index_pct_s = None
    for wind_code in fund_name_list:
        if nav_index_pct_s is None:
            nav_index_pct_s = fund_pct_df[wind_code] * weight_dic[wind_code]
        else:
            nav_index_pct_s += fund_pct_df[wind_code] * weight_dic[wind_code]
            # print("df_nav_index_pct_s[%s]:\n" % wind_code, df_nav_index_pct_s)
    date_list = list(fund_pct_df.index)
    if len(date_list) == 0:
        file_path = get_cache_file_path('df_fund_%s_%s.csv' % (file_name, date_since))
        print('子基金净值日期交集为空, 参见 %s文件查看具体数据' % file_path)
        df_fund.to_csv(file_path)
    print('between: %s ~ %s' % (min(date_list), max(date_list)))
    return nav_index_pct_s


def get_fund_nav_with_index(wind_code, date_from_str, date_to_str, quantile_list=[0.5], normalized=True, use_alias=False):
    """
    获取基金净值与对应策略指数的对比走势
    :param wind_code: 
    :param date_from_str: 
    :param date_to_str: 
    :param quantile_list: 
    :param normalized: 
    :return: 
    """
    engine = get_db_engine()
    # 获取策略信息
    with get_db_session(engine) as session:
        table = session.execute('select sec_name, alias, strategy_type from fund_info where wind_code = :wind_code',
                        params={'wind_code': wind_code})
        content = table.first()
        if content is None:
            logger.error('没有找到%s的策略类型', wind_code)
            return None
        sec_name, alias, strategy_type_cn = content
    # logger.debug(strategy_type_cn)
    if strategy_type_cn not in STRATEGY_TYPE_CN_EN_DIC:
        logger.error("基金%s 策略%s 没有对应的英文")
    strategy_type_en = STRATEGY_TYPE_CN_EN_DIC[strategy_type_cn]
    # 获取周级别净值信息
    sql_str = """select nav_date_week, fv.nav_acc
    from (
        select wind_code, adddate(nav_date, 4 - weekday(nav_date)) as nav_date_week, max(nav_date) as nav_date_max
        from fund_nav
        where wind_code = %s
        group by wind_code, nav_date_week
        having nav_date_week between %s and %s
        ) as ffv,
        fund_nav fv
    where fv.wind_code = %s
    and fv.wind_code = ffv.wind_code
    and fv.nav_date = ffv.nav_date_max
    group by fv.wind_code, nav_date_week
    order by nav_date_week"""
    fund_nav_df = pd.read_sql(sql_str, engine, params=[wind_code, date_from_str, date_to_str, wind_code])
    column_name = alias if use_alias else wind_code + sec_name
    fund_nav_df.rename(columns={'nav_acc': column_name}, inplace=True)
    fund_nav_df.set_index('nav_date_week', inplace=True)
    # logger.debug('fund_nav_df.shape:%s', fund_nav_df.shape)
    # logger.debug('fund_nav_df:\n%s', fund_nav_df)

    # 获取周级别策略指数信息
    stg_index_s, stg_statistic_dic = get_strategy_index_by_name(strategy_type_en, date_from_str, date_to_str, create_csv=False)
    # logger.debug("\n%s", stg_index_s)
    fund_nav_df[strategy_type_cn] = stg_index_s
    if normalized:
        fund_nav_df = (1 + fund_nav_df.pct_change().fillna(0)).cumprod()
    return fund_nav_df

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s: %(levelname)s [%(name)s:%(funcName)s] %(message)s')
    # 生成指数
    # date_from_str, date_to_str = '2017-8-1', '2017-8-31'
    # strategy_name_list = ['alpha', 'arbitrage', 'cta', 'fixed_income', 'long_only', 'macro']
    # strategy_name_list = ['arbitrage']
    # build_index_with_strategy_name_list(strategy_name_list, date_from_str, date_to_str)

    # 用于市场回顾功能 策略分位数显示，市场策略统计使用
    # strategy_type_en = 'fof'
    # date_from_str, date_to_str = '2017-5-31', '2017-9-1'
    # df_rr_df = get_strategy_index_quantile(strategy_type_en, date_from_str, date_to_str,
    #                                        [0.95, 0.90, 0.75, 0.6, 0.50, 0.4, 0.25, 0.10, 0.05])
    # df_rr_df.to_csv('df_rr_df.csv')
    # logger.info(df_rr_df)
    # df_rr_df.plot(grid=True)
    # plt.legend(loc=2)
    # plt.show()

    # get_strategy_index_hist(strategy_type_en, date_from_str, date_to_str)

    # strategy_name = 'long_only'
    # build_index_with_strategy_name(strategy_name, date_from, date_to)

    # 更新指数
    # update_strategy_index(date_from_str, date_to_str)
    # do_update_strategy_index()

    # 统计子基金
    # 2017-07-05 wind 数据中发现部分数据存在 净值从100多将到1.00的现象，涉及基金600多支
    # 使用相应的检查脚本及update脚本进行修复，脚本如下
    # select fn1.wind_code, fn1.nav_acc acc1, fn2.nav_acc acc2, fn2.nav_acc/fn1.nav_acc, fn1.nav, fn2.nav
    # from
    # (select * from fund_nav where nav_date='2017-05-26') fn1,
    # (select * from fund_nav where nav_date='2017-06-02') fn2
    # where fn1.wind_code = fn2.wind_code
    # and fn2.nav_acc/fn1.nav_acc < 0.8
    # update 脚本
    # update fund_nav
    # set nav = nav*100, nav_acc = nav_acc*100
    # where wind_code in (
    # select fn1.wind_code
    # from
    # (select * from fund_nav where nav_date='2017-05-26') fn1,
    # (select * from fund_nav where nav_date='2017-06-02') fn2
    # where fn1.wind_code = fn2.wind_code
    # and fn2.nav_acc/fn1.nav_acc < 0.5
    # and fn1.nav_date < '2017-06-02'
    # GROUP BY fn1.wind_code
    # )
    # and nav_date >= '2017-06-02'

    # 计算市场各个策略分位数走势、统计策略绩效
    # date_from_str, date_to_str = '2017-8-1', '2017-8-31'
    # stat_df = statistic_fund_by_strategy(date_from_str, date_to_str, do_filter=False)
    # print(stat_df)
    # date_from_str_year = str(str_2_date(date_to_str) - timedelta(days=365))
    # statistic_fund_by_strategy(date_from_str_year, date_to_str)
    # 单独统计某一策略绩效
    # stg_index_s, stg_statistic_dic = get_strategy_index_by_name('long_only', date_from_str, date_to_str,
    #                                                             create_csv=False)
    # print(stg_index_s)

    # 获取某只基金与其对应策略指数走势对比图
    # wind_code, quantile_list = 'XT148671.XT', [0.5]
    # date_from_str, date_to_str = '2016-09-01', '2017-08-25'
    # fund_and_index_df = get_fund_nav_with_index(wind_code, date_from_str, date_to_str, quantile_list, normalized=True, use_alias=True)
    # print(fund_and_index_df)
    # fund_and_index_df.to_excel(get_cache_file_path('%s 与策略指数对比走势.xls' % wind_code))

    # 根据子基金投资比例获得历史加权净值走势
    # 例如：杉树欣欣 300 千象全景1号 600 开拓者 400
    # wind_code_dic = {'XT1605537.XT': 300, 'XT1521015.XT': 600, 'XT1612348.XT': 400}
    # date_from_str, date_to_str = '2017-1-1', '2017-6-30'
    # index_nav_s = calc_index_by_wind_code_dic(wind_code_dic, date_from_str, date_to_str)
    # print(index_nav_s)
    # data = [ i for i in index_nav_s]
    # print(data)
    # time_line = index_nav_s.index
    # print([i.strtime() for i in time_line])
    # index_nav_s.plot()
    # plt.show()

    # 临时计算 展示收益率曲线
    # wind_code_p = 'FHF-P-101602'
    # nav_index_pct_all_s = calc_fof_index(wind_code_p)
    # nav_index_pct_all_s.to_csv('%s nav_index_pct_all_s.csv' % wind_code_p)
    # rr = (nav_index_pct_all_s + 1).cumprod()
    # rr.to_csv('%s rr.csv' % wind_code_p)
    # print(rr)
