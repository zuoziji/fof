# -*- coding: utf-8 -*-
"""
:copyright: (c) 2016 by Huang Dong 
:mail: bjdhuang@cn.ibm.com.
:license: Apache 2.0, see LICENSE for more details.
"""

# from periodic_task.perform_attribution import cal_perform_attrib
import pandas as pd
import logging
import numpy as np
from fh_tools.fh_utils import str_2_date
from config_fh import get_db_engine, get_db_session, STR_FORMAT_DATE, get_redis
import json
logger = logging.getLogger()

fac_name_map = {'fac_Inverse_Moment': '动量反转因子', 'fac_Mv': '市值因子', 'fac_Pe': '估值因子', 'fac_Turn': '换手率因子', \
                'fac_Vol': '异质波动率因子', 'Const': '常数项因子'}


def cal_factor_profit(startdate, enddate):
    # 获取股票暴露信息以及起止日期收益率信息

    sql_origin = '''select stock_facexposure.*, stock_facexposure.Trade_Date, sti1.Trade_Date as date_start,sti1.close AS close_start, sti2.Trade_Date as date_end, sti2.close as close_end 
from stock_facexposure, wind_stock_daily as sti2 , wind_stock_daily as sti1 
where stock_facexposure.Trade_Date= '%s'
 and sti1.wind_code = stock_facexposure.stock_code
 and sti2.wind_code = stock_facexposure.stock_code
 and sti1.Trade_Date = '%s'
 and sti2.Trade_Date = '%s' '''

    sql_pd = sql_origin % (startdate, startdate, enddate)
    engine = get_db_engine()
    origin_data_df = pd.read_sql(sql_pd, engine)
    # 剔除价格没变的股票
    origin_data_sameclose = origin_data_df[origin_data_df['close_end'] == origin_data_df['close_start']].index
    origin_data_df.drop(origin_data_sameclose, axis=0, inplace=True)
    # 获取 因子数据（因子变量存储以fac开头）
    data_columns = origin_data_df.columns.values
    fac_names = [i for i in data_columns if i[:3] == 'fac']
    fac_data = origin_data_df[fac_names].copy()
    # 对因子进行标准化归一化去极值处理
    fac_data = (fac_data - fac_data.mean()) / fac_data.std()
    fac_data[fac_data > 3] = 3
    fac_data[fac_data < -3] = -3
    # 增加常数项
    fac_data['Const'] = 1
    # 计算因子收益率
    x_traindata = np.array(fac_data)
    y_traindata = np.array(origin_data_df['close_end'] / origin_data_df['close_start'] - 1)
    origin_data_df['Profit'] = y_traindata
    facprofit = np.linalg.lstsq(x_traindata, y_traindata)[0]
    fac_names = fac_data.columns
    return facprofit, fac_names, fac_data, origin_data_df


def cal_perform_attrib(wind_code, date_from, date_to):
    fac_profit, fac_name, fac_data, origin_data = cal_factor_profit(date_from, date_to)
    # 获取指数数据，中证500
    engine = get_db_engine()
    # sql_origin = "select * from index_tradeinfo where index_tradeinfo.Index_Code = '000905.SH' and " + \
    #              "(index_tradeinfo.Trade_Date = '%s' or index_tradeinfo.Trade_Date = '%s' )"
    # sql_close = sql_origin % (date_from, date_to)
    sql_close = """select * from wind_index_daily 
where wind_code = '000905.SH'
and (Trade_Date = %s or Trade_Date = %s )"""
    index_close = pd.read_sql(sql_close, engine, params=[date_from, date_to])
    index_change = index_close['CLOSE'][index_close.trade_date == str_2_date(date_to)].values / \
                   index_close['CLOSE'][index_close.trade_date == str_2_date(date_from)].values - 1
    if len(index_change) == 0:
        return None
    origin_data['Profit'] = origin_data['Profit'] - index_change  # 对冲后增长率
    # 获取Fund的涨跌幅
    sql_fund_origin = "select * FROM fund_nav where wind_code = '%s' and nav_date between '%s' and '%s'"
    sql_fundnv = sql_fund_origin % (wind_code, date_from, date_to)
    fund_nav_df = pd.read_sql(sql_fundnv, engine)
    # sql_fund_origin = "select * FROM fundnav where wind_code = %s and (fundnav.trade_date = %s or fundnav.trade_date = %s)"
    # fund_close = pd.read_sql(sql_fund_origin, engine, [fundId, startdate, enddate])
    date_from = fund_nav_df['nav_date'].max()
    date_to = fund_nav_df['nav_date'].min()
    fund_profit = fund_nav_df['nav'][fund_nav_df['nav_date'] == date_to].values / fund_nav_df['nav'][fund_nav_df['nav_date'] == date_from].values - 1
    # fund_close_df['nav_date'] = fund_close_df['nav_date'].apply(lambda x: x.strftime('%Y-%m-%d'))  # 存储格式
    std_stock = origin_data['Profit'].std()
    # 寻找最接近的涨幅
    fund_profit_low = fund_profit[0] - .01 * std_stock
    fund_profit_high = fund_profit[0] + .01 * std_stock
    target_data = fac_data[
        (origin_data['Profit'] > fund_profit_low) & (origin_data['Profit'] < fund_profit_high)]
    # return origin_data, fund_profit_low, fund_profit_high
    fund_exposure = target_data.mean()
    fund_exposure = fund_exposure[fac_name]
    # fund_fac_profit = fund_exposure[fac_name] * np.array(fac_profit)
    index_names = fund_exposure.index.values
    fund_exposure_pd = pd.DataFrame(fund_exposure)
    indictor_names = [fac_name_map[i] for i in index_names]
    fund_exposure_pd[u'因子名称'] = indictor_names
    fund_exposure_pd.rename(columns={0: u'因子暴露'}, inplace=True)
    # 去除Const
    index_ig = fund_exposure_pd[fund_exposure_pd.index == 'Const'].index
    fund_exposure_pd.drop(index_ig, axis=0, inplace=True)
    fund_exposure_pd.set_index(u'因子名称', inplace=True)
    return fund_exposure_pd


def cal_perform_attrib_bak(fundId, startdate, enddate):
    fac_profit, fac_name, fac_data, origin_data = cal_factor_profit(startdate, enddate)
    # 获取指数数据，中证500
    engine = get_db_engine()
    sql_origin = "select * from index_tradeinfo where index_tradeinfo.Index_Code = '000905.SH' and " + \
                 "(index_tradeinfo.Trade_Date = '%s' or index_tradeinfo.Trade_Date = '%s' )"
    sql_close = sql_origin % (startdate, enddate)
    index_close = pd.read_sql(sql_close, engine)
    index_change = index_close['CLOSE'][index_close.Trade_Date == enddate].values / \
                   index_close['CLOSE'][index_close.Trade_Date == startdate].values - 1
    if len(index_change) == 0:
        return None
    origin_data['Profit'] = origin_data['Profit'] - index_change  # 对冲后增长率
    # 获取Fund的涨跌幅
    sql_fund_origin = "select * FROM fund_nav where wind_code = '%s' and nav_date between '%s' and '%s'"
    sql_fundnv = sql_fund_origin % (fundId, startdate, enddate)
    fund_nav_df = pd.read_sql(sql_fundnv, engine)
    # sql_fund_origin = "select * FROM fundnav where wind_code = %s and (fundnav.trade_date = %s or fundnav.trade_date = %s)"
    # fund_close = pd.read_sql(sql_fund_origin, engine, [fundId, startdate, enddate])
    date_from = fund_nav_df['nav_date'].max()
    date_to = fund_nav_df['nav_date'].min()
    fund_profit = fund_nav_df['nav'][fund_nav_df['nav_date'] == date_to].values / fund_nav_df['nav'][fund_nav_df['nav_date'] == date_from].values - 1
    # fund_close_df['nav_date'] = fund_close_df['nav_date'].apply(lambda x: x.strftime('%Y-%m-%d'))  # 存储格式
    std_stock = origin_data['Profit'].std()
    # 寻找最接近的涨幅
    fund_profit_low = fund_profit[0] - .01 * std_stock
    fund_profit_high = fund_profit[0] + .01 * std_stock
    target_data = fac_data[
        (origin_data['Profit'] > fund_profit_low) & (origin_data['Profit'] < fund_profit_high)]
    # return origin_data, fund_profit_low, fund_profit_high
    fund_exposure = target_data.mean()
    fund_exposure = fund_exposure[fac_name]
    # fund_fac_profit = fund_exposure[fac_name] * np.array(fac_profit)
    index_names = fund_exposure.index.values
    fund_exposure_pd = pd.DataFrame(fund_exposure)
    indictor_names = [fac_name_map[i] for i in index_names]
    fund_exposure_pd[u'因子名称'] = indictor_names
    fund_exposure_pd.rename(columns={0: u'因子暴露'}, inplace=True)
    # 去除Const
    index_ig = fund_exposure_pd[fund_exposure_pd.index == 'Const'].index
    fund_exposure_pd.drop(index_ig, axis=0, inplace=True)
    fund_exposure_pd.set_index(u'因子名称', inplace=True)
    return fund_exposure_pd

def get_fund_exposure_his(wind_code, period_count=12):
    # 链接数据库，并获取fundnav旧表
    with get_db_session() as session:
        # 获取 nav_date 列表
        nav_date_table = session.execute(
            'select nav_date from fund_nav, (select trade_date from stock_facexposure group by trade_date) as td where wind_code=:code and td.trade_date=fund_nav.nav_date group by nav_date order by nav_date',
            {'code': wind_code})
        nav_date_list = [nav_date[0] for nav_date in nav_date_table.fetchall()]
    nav_date_count = len(nav_date_list)
    logger.info('nav_date_count:{}'.format(nav_date_count))
    if nav_date_count > 0:
        fund_exposure_df_list = []
        for n in range(max([0, nav_date_count - period_count]), nav_date_count - 6):
            nav_date_str1 = nav_date_list[n - 1].strftime(STR_FORMAT_DATE)
            nav_date_str = nav_date_list[n].strftime(STR_FORMAT_DATE)
            logger.info('calc cal_perform_attrib(%s, %s, %s)', wind_code, nav_date_str1, nav_date_str)
            fund_exposure_df = cal_perform_attrib(wind_code,
                                                  nav_date_str1,
                                                  nav_date_str)
            # '2016-01-04', '2016-01-29')
            if fund_exposure_df is None:
                continue
            fund_exposure_df_list.append(fund_exposure_df.rename(columns={'因子暴露': nav_date_str}))
        if len(fund_exposure_df_list) != 0:
            fund_exposure_his_df = pd.concat(fund_exposure_df_list, axis=1)
        else:
            fund_exposure_his_df = None
        return fund_exposure_his_df
    else:
        return None


def do_fund_multi_factor():
    sql_str = """select distinct wind_code
    from fof_fund_pct ffp,
    (
    select wind_code_p, max(date_adj) date_latest from fof_fund_pct group by wind_code_p
    ) ff_date_latest,
    fund_essential_info ffm
    where ffp.wind_code_p = ff_date_latest.wind_code_p
    and ffp.wind_code_s = ffm.wind_code_s
    and ffp.date_adj = ff_date_latest.date_latest"""

    with get_db_session() as session:
        table = session.execute(sql_str)
        wind_code_list = [wind_code[0] for wind_code in table.fetchall()]
    do_fund_multi_factor_by_wind_code_list(wind_code_list)


def do_fund_multi_factor_by_wind_code_list(wind_code_list):
    """
    对基金列表分别进行归因分析
    :param wind_code_list: 
    :return: 
    """
    r = get_redis()
    for n, wind_code in enumerate(wind_code_list):
        logger.info('')
        fund_exposure_his_df = get_fund_exposure_his(wind_code)
        if fund_exposure_his_df is not None:
            key = wind_code + 's:multi_factor'
            fund_exposure_str = json.dumps(fund_exposure_his_df.to_dict())
            r.set(key, fund_exposure_str)
            logger.debug('multi factor on %s with key: %s value:\n%s', wind_code, key, fund_exposure_str)


def do_fund_multi_factor_by_scheme(scheme_id):
    """
    根据 scheme 进行所有子基金的压力测试
    :param scheme_id: 
    :return: 
    """
    sql_str = "SELECT wind_code, invest_scale FROM scheme_fund_pct where scheme_id=%(scheme_id)s"
    engine = get_db_engine()
    fund_pct_df = pd.read_sql(sql_str, engine, params={'scheme_id': str(scheme_id)})
    wind_code_list = list(fund_pct_df['wind_code'])  # ['XT1410445.XT', 'J11039.OF']
    wind_code_count = len(wind_code_list)
    logger.info('multi factor for %d on wind_code_p with %s', scheme_id, wind_code_list)
    if wind_code_count <= 0:
        logger.warning('scheme %s has no sub fund list', scheme_id)
        return
    do_fund_multi_factor_by_wind_code_list(wind_code_list)


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s: %(levelname)s [%(name)s:%(funcName)s] %(message)s')
    # target_data = cal_perform_attrib('XT1605537.XT', '2017-05-12', '2017-05-18')
    # print(target_data, len(target_data), type(target_data))

    # wind_code = 'XT1605537.XT'  # 'J166414.OF'
    # fund_exposure_his_df = get_fund_exposure_his(wind_code)
    # print(fund_exposure_his_df.to_dict())

    # do_fund_multi_factor()

    # 对基金列表中每一只基金进行归因分析并传入redis
    wind_code_list = ['FHF-101602']
    do_fund_multi_factor_by_wind_code_list(wind_code_list)

    # scheme_id = 21
    # do_fund_multi_factor_by_scheme(scheme_id)
