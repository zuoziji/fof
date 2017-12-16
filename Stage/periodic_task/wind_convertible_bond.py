# -*- coding: utf-8 -*-
"""
Created on 2017/12/5
@author: MG
"""
from datetime import date, datetime, timedelta

import math
import pandas as pd
import numpy as np
from config_fh import get_db_engine, get_db_session, STR_FORMAT_DATE, UN_AVAILABLE_DATE, WIND_REST_URL
from fh_tools.windy_utils_rest import WindRest
from fh_tools.fh_utils import get_last, get_first
import logging
from sqlalchemy.types import String, Date, Float, Integer
logger = logging.getLogger()
DATE_BASE = datetime.strptime('1998-01-01', STR_FORMAT_DATE).date()
ONE_DAY = timedelta(days=1)
w = WindRest(WIND_REST_URL)


def get_cb_set(date_fetch):
    date_fetch_str = date_fetch.strftime(STR_FORMAT_DATE)
    data_df = w.wset("sectorconstituent", "date=%s;sectorid=1000021892000000" % date_fetch_str)
    if data_df is None:
        logging.warning('%s 获取股票代码失败', date_fetch_str)
        return None
    data_count = data_df.shape[0]
    logging.info('get %d convertible bond on %s', data_count, date_fetch_str)
    return set(data_df['wind_code'])


def import_cb_info(first_time=False):
    """
    获取全市场可转债数据
    :param first_time: 第一次执行时将从 1999 年开始查找全部基本信息
    :return: 
    """
    if first_time:
        date_since = datetime.strptime('1999-01-01', STR_FORMAT_DATE).date()
        date_list = []
        one_year = timedelta(days=365)
        while date_since < date.today() - ONE_DAY:
            date_list.append(date_since)
            date_since += one_year
        else:
            date_list.append(date.today() - ONE_DAY)
    else:
        date_list = [date.today() - ONE_DAY]

    # 获取 wind_code 集合
    wind_code_set = set()
    for fetch_date in date_list:
        data_set = get_cb_set(fetch_date)
        if data_set is not None:
            wind_code_set |= data_set

    # 获取股票对应上市日期，及摘牌日期
    # w.wss("300005.SZ,300372.SZ,000003.SZ", "ipo_date,trade_code,mkt,exch_city,exch_eng")
    wind_code_list = list(wind_code_set)
    wind_code_count = len(wind_code_list)
    seg_count = 1000
    # loop_count = math.ceil(float(wind_code_count) / seg_count)
    data_info_df_list = []
    for n in range(0, wind_code_count, seg_count):
        num_start = n * seg_count
        num_end = (n + 1) * seg_count
        # num_end = num_end if num_end <= wind_code_count else wind_code_count
        sub_list = wind_code_list[n:(n+seg_count)]
        # 尝试将 stock_code_list_sub 直接传递给wss，是否可行
        stock_info_df = w.wss(sub_list,
                              "issue_announcement,trade_code,fullname,sec_name,clause_conversion_2_swapsharestartdate,clause_conversion_2_swapshareenddate,ipo_date,underlyingcode,underlyingname,clause_conversion_code,clause_interest_5,clause_interest_8,clause_interest_6,clause_interest_compensationinterest,issueamount,term",
                              "unit=1")
        data_info_df_list.append(stock_info_df)

    data_info_all_df = pd.concat(data_info_df_list)
    data_info_all_df.index.rename('WIND_CODE', inplace=True)
    logging.info('%s stock data will be import', data_info_all_df.shape[0])
    engine = get_db_engine()
    data_info_all_df.reset_index(inplace=True)
    data_dic_list = data_info_all_df.to_dict('records')
    sql_name_param_dic = {
        'wind_code': 'WIND_CODE',
        'trade_code': 'TRADE_CODE',
        'full_name': 'FULLNAME',
        'sec_name': 'SEC_NAME',
        'issue_announcement_date': 'ISSUE_ANNOUNCEMENT',
        'ipo_date': 'IPO_DATE',
        'start_date': 'CLAUSE_CONVERSION_2_SWAPSHARESTARTDATE',
        'end_date': 'CLAUSE_CONVERSION_2_SWAPSHAREENDDATE',
        'conversion_code': 'CLAUSE_CONVERSION_CODE',
        'is_floating_rate': 'CLAUSE_INTEREST_5',
        'is_interest_compensation': 'CLAUSE_INTEREST_8',
        'interest_compensation_desc': 'CLAUSE_INTEREST_6',
        'interest_compensation': 'CLAUSE_INTEREST_COMPENSATIONINTEREST',
        'term': 'TERM',
        'underlying_code': 'UNDERLYINGCODE',
        'underlying_name': 'UNDERLYINGNAME',
    }
    name_list = list(sql_name_param_dic.keys())
    name_list_str = ', '.join(name_list)
    param_list_str = ', '.join([':' + sql_name_param_dic[name] for name in name_list])
    sql_str = "REPLACE INTO wind_convertible_bond_info (%s) values (%s)" % (name_list_str, param_list_str)
    # sql_str = "insert INTO wind_stock_info (wind_code, trade_code, sec_name, ipo_date, delist_date, mkt, exch_city, exch_eng, prename) values (:WIND_CODE, :TRADE_CODE, :SEC_NAME, :IPO_DATE, :DELIST_DATE, :MKT, :EXCH_CITY, :EXCH_ENG, :PRENAME)"
    with get_db_session(engine) as session:
        session.execute(sql_str, data_dic_list)
        stock_count = session.execute('select count(*) from wind_convertible_bond_info').first()[0]
    logging.info("%d stocks have been in wind_convertible_bond_info", stock_count)


def import_cb_daily():
    """
    导入可转债日线数据
    :return: 
    """
    w = WindRest(WIND_REST_URL)
    engine = get_db_engine()
    with get_db_session(engine) as session:
        # 获取每只股票最新交易日数据
        sql_str = 'select wind_code, max(trade_date) from wind_convertible_bond_daily group by wind_code'
        table = session.execute(sql_str)
        trade_date_latest_dic = dict(table.fetchall())
        # 获取市场有效交易日数据
        sql_str = "select trade_date from wind_trade_date where trade_date > '1997-1-1'"
        table = session.execute(sql_str)
        trade_date_sorted_list = [t[0] for t in table.fetchall()]
        trade_date_sorted_list.sort()
        # 获取每只股票上市日期、退市日期
        table = session.execute('SELECT wind_code, ipo_date, end_date FROM wind_convertible_bond_info')
        stock_date_dic = {wind_code: (ipo_date, end_date if end_date is None or end_date > UN_AVAILABLE_DATE else None) for
                          wind_code, ipo_date, end_date in table.fetchall()}
    today_t_1 = date.today() - ONE_DAY
    data_df_list = []
    logger.info('%d stocks will been import into wind_trade_date_wch', len(stock_date_dic))
    # 获取股票量价等行情数据
    field_col_name_dic = {
        'outstandingbalance': 'outstanding_balance',
        'clause_conversion2_bondlot': 'conversion2_bondlot',
        'clause_conversion2_bondproportion': 'conversion2_bondproportion',
        'clause_conversion2_swapshareprice': 'conversion2_swapshareprice',
        'clause_conversion2_conversionproportion': 'conversion2_conversionproportion',
        'convpremium': 'conv_premium',
        'convpremiumratio': 'conv_premium_ratio',
        'convvalue': 'conv_value',
        'convpe': 'conv_pe',
        'convpb': 'conv_pb',
        'underlyingpe': 'underlying_pe',
        'underlyingpb': 'underlying_pb',
        'diluterate': 'dilute_rate',
        'ldiluterate': 'ldilute_rate',
        'close': 'close',
    }
    wind_indictor_str = ",".join(field_col_name_dic.keys())
    upper_col_2_name_dic = {name.upper(): val for name, val in field_col_name_dic.items()}
    try:
        for data_num, (wind_code, date_pair) in enumerate(stock_date_dic.items()):
            date_ipo, end_date = date_pair
            # 初次加载阶段全量载入，以后 ipo_date为空的情况，直接warning跳过
            if date_ipo is None:
                # date_ipo = DATE_BASE
                logging.warning("%d) %s 缺少 ipo date", data_num, wind_code)
                continue
            # 获取 date_from
            if wind_code in trade_date_latest_dic:
                date_latest_t1 = trade_date_latest_dic[wind_code] + ONE_DAY
                date_from = max([date_latest_t1, DATE_BASE, date_ipo])
            else:
                date_from = max([DATE_BASE, date_ipo])
            date_from = get_first(trade_date_sorted_list, lambda x: x >= date_from)
            # 获取 date_to
            if end_date is None:
                date_to = today_t_1
            else:
                date_to = min([end_date, today_t_1])
            date_to = get_last(trade_date_sorted_list, lambda x: x <= date_to)
            if date_from is None or date_to is None or date_from > date_to:
                continue
            data_df = w.wsd(wind_code, wind_indictor_str, date_from, date_to, "unit=1")
            if data_df is None:
                logger.warning('%d) %s has no ohlc data during %s %s', data_num, wind_code, date_from, date_to)
                continue
            data_df.rename(columns=upper_col_2_name_dic, inplace=True)
            logger.info('%d) %d data of %s between %s and %s', data_num, data_df.shape[0], wind_code, date_from, date_to)
            data_df['wind_code'] = wind_code
            data_df_list.append(data_df)
            # if len(data_df_list) > 10:
            #     break
    finally:
        # 导入数据库
        if len(data_df_list) > 0:
            data_df_all = pd.concat(data_df_list)
            data_df_all.index.rename('trade_date', inplace=True)
            data_df_all.reset_index(inplace=True)
            data_df_all.set_index(['wind_code', 'trade_date'], inplace=True)
            data_df_all.to_sql('wind_convertible_bond_daily', engine, if_exists='append')
            logger.info('%d data imported into wind_convertible_bond_daily', data_df_all.shape[0])

if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s: %(levelname)s [%(name)s:%(funcName)s] %(message)s')

    import_cb_info()
    # import_cb_daily()
