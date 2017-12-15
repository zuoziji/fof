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


def get_wind_code_set(date_fetch):
    date_fetch_str = date_fetch.strftime(STR_FORMAT_DATE)
    # 纯股票型基金
    data_df = w.wset("sectorconstituent", "date=%s;sectorid=2001010101000000" % date_fetch_str)
    if data_df is None:
        logging.warning('%s 获取股票代码失败', date_fetch_str)
        return None
    data_count = data_df.shape[0]
    logging.info('get %d public offering fund on %s', data_count, date_fetch_str)
    wind_code_s = data_df['wind_code']
    wind_code_s = wind_code_s[wind_code_s.apply(lambda x: x.find('!') == -1)]
    return set(wind_code_s)


def import_pub_fund_info():
    # 获取全市场可转债数据
    date_since = datetime.strptime('2004-01-01', STR_FORMAT_DATE).date()
    date_list = []
    one_year = timedelta(days=365)
    while date_since < date.today() - ONE_DAY:
        date_list.append(date_since)
        date_since += one_year
    else:
        date_list.append(date.today() - ONE_DAY)
    wind_code_set = set()
    for fetch_date in date_list:
        data_set = get_wind_code_set(fetch_date)
        if data_set is not None:
            wind_code_set |= data_set

    engine = get_db_engine()
    # 剔除数据库中已有数据
    with get_db_session(engine) as session:
        sql_str = "select wind_code from wind_pub_fund_info"
        table = session.execute(sql_str)
        wind_code_set_existed = {content[0] for content in table.fetchall()}
    wind_code_set -= wind_code_set_existed
    # 获取股票对应上市日期，及摘牌日期
    # w.wss("300005.SZ,300372.SZ,000003.SZ", "ipo_date,trade_code,mkt,exch_city,exch_eng")
    wind_code_list = list(wind_code_set)
    wind_code_count = len(wind_code_list)
    seg_count = 1000
    # loop_count = math.ceil(float(wind_code_count) / seg_count)
    data_info_df_list = []
    fund_info_field_col_name_dic = {'fund_fullname': "full_name",
                                    'fund_exchangeshortname': "short_name",
                                    'fund_benchmark': "bench_mark",
                                    'fund_benchindexcode': "bench_index_code",
                                    'fund_setupdate': "setup_date",
                                    'fund_maturitydate': "maturity_date",
                                    'fund_fundmanager': "fund_fundmanager",
                                    'fund_predfundmanager': "fund_predfundmanager",
                                    'fund_mgrcomp': "fund_mgrcomp",
                                    'fund_custodianbank': "custodian_bank",
                                    'fund_type': "fund_type",
                                    'fund_firstinvesttype': "invest_type_level1",
                                    'fund_investtype': "invest_type",
                                    'fund_structuredfundornot': "structured_fund_or_not",
                                    'fund_investstyle': "invest_style",
                                    }

    for n in range(0, wind_code_count, seg_count):
        sub_list = wind_code_list[n:(n + seg_count)]
        # 尝试将 stock_code_list_sub 直接传递给wss，是否可行
        # w.wss("000309.OF", "fund_fullname,fund_exchangeshortname,fund_benchmark,fund_benchindexcode,fund_setupdate,fund_maturitydate,fund_fundmanager,fund_mgrcomp,fund_custodianbank,fund_type,fund_firstinvesttype,fund_investtype,fund_structuredfundornot,fund_investstyle")
        field_str = ",".join(fund_info_field_col_name_dic.keys())
        stock_info_df = w.wss(sub_list,field_str)
        data_info_df_list.append(stock_info_df)
    if len(data_info_df_list) == 0:
        logger.info("wind_pub_fund_info 没有数据可以导入")
        return
    data_info_all_df = pd.concat(data_info_df_list)
    data_info_all_df.index.rename('wind_code', inplace=True)
    data_info_all_df.rename(columns={field.upper(): col_name for field, col_name in fund_info_field_col_name_dic.items()}, inplace=True)
    logging.info('%s stock data will be import', data_info_all_df.shape[0])

    data_info_all_df.reset_index(inplace=True)
    data_dic_list = data_info_all_df.to_dict('records')

    name_list = list(fund_info_field_col_name_dic.values())
    name_list_str = ', '.join(name_list)
    param_list_str = ', '.join([':' + name for name in name_list])
    sql_str = "REPLACE INTO wind_pub_fund_info (wind_code,%s) values (:wind_code,%s)" % (name_list_str, param_list_str)
    # sql_str = "insert INTO wind_stock_info (wind_code, trade_code, sec_name, ipo_date, delist_date, mkt, exch_city, exch_eng, prename) values (:WIND_CODE, :TRADE_CODE, :SEC_NAME, :IPO_DATE, :DELIST_DATE, :MKT, :EXCH_CITY, :EXCH_ENG, :PRENAME)"
    with get_db_session(engine) as session:
        session.execute(sql_str, data_dic_list)
        stock_count = session.execute('select count(*) from wind_pub_fund_info').first()[0]
    logging.info("%d stocks have been in wind_pub_fund_info", stock_count)


def import_df_list_2_db(data_df_list):
    engine = get_db_engine()
    data_df_all = pd.concat(data_df_list)
    # data_df_all.index.rename('trade_date', inplace=True)
    # data_df_all.reset_index(inplace=True)
    data_df_all.set_index(['wind_code', 'NAV_date'], inplace=True)
    data_df_all.to_sql('wind_pub_fund_daily', engine, if_exists='append')
    data_count = data_df_all.shape[0]
    logger.info('%d data imported into wind_pub_fund_daily', data_count)
    return data_count


def import_pub_fund_daily():
    """
    导入公募基金日线数据
    :return: 
    """
    w = WindRest(WIND_REST_URL)
    engine = get_db_engine()
    with get_db_session(engine) as session:
        # 获取每只股票最新交易日数据
        sql_str = 'select wind_code, max(nav_date) from wind_pub_fund_daily group by wind_code'
        table = session.execute(sql_str)
        trade_date_latest_dic = dict(table.fetchall())
        # 获取市场有效交易日数据
        sql_str = "select trade_date from wind_trade_date where trade_date > '1997-1-1'"
        table = session.execute(sql_str)
        trade_date_sorted_list = [t[0] for t in table.fetchall()]
        trade_date_sorted_list.sort()
        # 获取每只股票上市日期、退市日期
        table = session.execute('SELECT wind_code, setup_date, maturity_date FROM wind_pub_fund_info')
        stock_date_dic = {wind_code: (setup_date, maturity_date if maturity_date is None or maturity_date > UN_AVAILABLE_DATE else None)
                          for
                          wind_code, setup_date, maturity_date in table.fetchall()}
    today_t_1 = date.today() - ONE_DAY
    data_df_list = []
    logger.info('%d stocks will been import into wind_trade_date_wch', len(stock_date_dic))
    # 获取股票量价等行情数据
    field_col_name_dic = {
        'NAV_date': 'NAV_date',
        'NAV_acc': 'NAV_acc',
        'netasset_total': 'netasset_total',
    }
    wind_indictor_str = ",".join(field_col_name_dic.keys())
    upper_col_2_name_dic = {name.upper(): val for name, val in field_col_name_dic.items()}
    try:
        data_tot = 0
        for data_num, (wind_code, (setup_date, maturity_date)) in enumerate(stock_date_dic.items()):
            # 初次加载阶段全量载入，以后 ipo_date为空的情况，直接warning跳过
            if setup_date is None:
                # date_ipo = DATE_BASE
                logging.warning("%d) %s 缺少 ipo date", data_num, wind_code)
                continue
            # 获取 date_from
            if wind_code in trade_date_latest_dic:
                date_latest_t1 = trade_date_latest_dic[wind_code] + ONE_DAY
                date_from = max([date_latest_t1, DATE_BASE, setup_date])
            else:
                date_from = max([DATE_BASE, setup_date])
            date_from = get_first(trade_date_sorted_list, lambda x: x >= date_from)
            # 获取 date_to
            if maturity_date is None:
                date_to = today_t_1
            else:
                date_to = min([maturity_date, today_t_1])
            date_to = get_last(trade_date_sorted_list, lambda x: x <= date_to)
            if date_from is None or date_to is None or date_from > date_to:
                continue
            data_df = w.wsd(wind_code, wind_indictor_str, date_from, date_to, "unit=1;Days=Weekdays")
            if data_df is None:
                logger.warning('%d) %s has no ohlc data during %s %s', data_num, wind_code, date_from, date_to)
                continue
            data_df = data_df.drop_duplicates().dropna()
            data_df.rename(columns=upper_col_2_name_dic, inplace=True)
            logger.info('%d) %d data of %s between %s and %s', data_num, data_df.shape[0], wind_code, date_from,
                        date_to)
            data_df['wind_code'] = wind_code
            data_tot += data_df.shape[0]
            data_df_list.append(data_df)
            if data_tot > 10000:
                import_df_list_2_db(data_df_list)
                data_df_list = []
                data_tot = 0
    finally:
        # 导入数据库
        if len(data_df_list) > 0:
            import_df_list_2_db(data_df_list)


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s: %(levelname)s [%(name)s:%(funcName)s] %(message)s')

    # import_pub_fund_info()
    import_pub_fund_daily()
