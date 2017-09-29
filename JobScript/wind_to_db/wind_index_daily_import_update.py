# -*- coding: utf-8 -*-
"""
Created on Thu Apr  6 11:11:26 2017

@author: Yupeng Guo
"""

from fh_tools.windy_utils_rest import WindRest
from fh_tools.fh_utils import get_first, get_last
import pandas as pd
from datetime import date, timedelta
from sqlalchemy.types import String, Date
from config_fh import get_db_engine, WIND_REST_URL, get_db_session
import logging

rest = WindRest(WIND_REST_URL)  # 初始化服务器接口，用于下载万得数据

def import_wind_index_daily_first(table_name, wind_codes):
    # wind_codes 可以是字符串，也可以是字符串的集合, table_name是数据库的表名
    engine = get_db_engine()
    yestday = (date.today() - timedelta(days=1)).strftime('%Y-%m-%d')
    info = rest.wss(wind_codes, "basedate,sec_name")
    for code in info.index:
        begin_date = info.loc[code, 'BASEDATE'].strftime('%Y-%m-%d')
        index_name = info.loc[code, 'SEC_NAME']
        temp = rest.wsd(code, "open,high,low,close,volume,amt", begin_date, yestday)
        temp.reset_index(inplace=True)
        temp.rename(columns={'index': 'trade_date'}, inplace=True)
        temp.trade_date = pd.to_datetime(temp.trade_date)
        temp.trade_date = temp.trade_date.map(lambda x: x.date())
        temp['wind_code'] = code
        temp['index_name'] = index_name
        temp.set_index(['wind_code', 'trade_date'], inplace=True)
        temp.to_sql(table_name, engine, if_exists='append', index_label=['wind_code', 'trade_date'],
                    dtype={
                        'wind_code': String(20),
                        'trade_date': Date,
                    })
        print('Success import %s - %s' % (code, index_name))


def import_wind_index_daily():
    engine = get_db_engine()
    yestday = date.today() - timedelta(days=1)
    query = pd.read_sql_query(
        'select wind_code,index_name, max(trade_date) as latest_date from wind_index_daily group by wind_code', engine)
    query.set_index('wind_code', inplace=True)
    with get_db_session(engine) as session:
        # 获取市场有效交易日数据
        sql_str = "select trade_date from wind_trade_date where trade_date > '2005-1-1'"
        table = session.execute(sql_str)
        trade_date_sorted_list = [t[0] for t in table.fetchall()]
        trade_date_sorted_list.sort()
    date_to = get_last(trade_date_sorted_list, lambda x: x <= yestday)
    for code in query.index:
        date_from = (query.loc[code, 'latest_date'] + timedelta(days=1))
        date_from = get_first(trade_date_sorted_list, lambda x: x >= date_from)
        if date_from is None or date_to is None or date_from > date_to:
            continue
        index_name = query.loc[code, 'index_name']
        temp = rest.wsd(code, "open,high,low,close,volume,amt", date_from, date_to)
        temp.reset_index(inplace=True)
        temp.rename(columns={'index': 'trade_date'}, inplace=True)
        temp.trade_date = pd.to_datetime(temp.trade_date)
        temp.trade_date = temp.trade_date.map(lambda x: x.date())
        temp['wind_code'] = code
        temp['index_name'] = index_name
        temp.set_index(['wind_code', 'trade_date'], inplace=True)
        temp.to_sql('wind_index_daily', engine, if_exists='append', index_label=['wind_code', 'trade_date'],
                    dtype={
                        'wind_code': String(20),
                        'trade_date': Date,
                    })
        logging.info('Success update %s - %s' % (code, index_name))


if __name__ == '__main__':
    #    wind_codes = "000001.SH,000016.SH,399101.SZ,399102.SZ,399001.SZ,399005.SZ,399006.SZ,000300.SH"
    #    wind_index_daily_import('wind_index_daily', wind_codes)
    import_wind_index_daily()
