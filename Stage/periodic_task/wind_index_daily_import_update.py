# -*- coding: utf-8 -*-
"""
Created on Thu Apr  6 11:11:26 2017

@author: Yupeng Guo
"""

from fh_tools.windy_utils_rest import WindRest
from fh_tools.fh_utils import get_first, get_last, str_2_date
import pandas as pd
from datetime import date, timedelta
from sqlalchemy.types import String, Date
from config_fh import get_db_engine, WIND_REST_URL, get_db_session
import logging
logger = logging.getLogger()

rest = WindRest(WIND_REST_URL)  # 初始化服务器接口，用于下载万得数据


def fill_wind_index_daily_col():
    with get_db_session() as session:
        sql_str = "select wind_code, min(trade_date), max(trade_date) from wind_index_daily group by wind_code"
        table = session.execute(sql_str)
        wind_date_dic = {content[0]: (content[1], content[2]) for content in table.fetchall()}
        for wind_code, date_pair in wind_date_dic.items():
            logger.debug('invoke wsd for %s between %s and %s', wind_code, date_pair[0], date_pair[1])
            data_df = rest.wsd(wind_code, "turn,free_turn", date_pair[0], date_pair[1], "")
            data_df.dropna(inplace=True)
            if data_df.shape[0] == 0:
                continue
            logger.debug('%d data importing for %s', data_df.shape[0], wind_code)
            data_df['WIND_CODE'] = wind_code
            data_df.index.rename('TRADE_DATE', inplace=True)
            data_df.reset_index(inplace=True)
            data_list = list(data_df.T.to_dict().values())
            sql_str = """update wind_index_daily
    set turn=:TURN, free_turn=:FREE_TURN
    where wind_code= :WIND_CODE
    and trade_date = :TRADE_DATE"""
            session.execute(sql_str, params=data_list)


def import_wind_index_daily_first(wind_codes):
    """
    首次导入某指数使用
    :param wind_codes: 可以是字符串，也可以是字符串的list 
    :return: 
    """
    engine = get_db_engine()
    yestday = date.today() - timedelta(days=1)
    info = rest.wss(wind_codes, "basedate,sec_name")
    for code in info.index:
        begin_date = str_2_date(info.loc[code, 'BASEDATE'])
        index_name = info.loc[code, 'SEC_NAME']
        index_df = rest.wsd(code, "open,high,low,close,volume,amt,turn,free_turn", begin_date, yestday)
        index_df.reset_index(inplace=True)
        index_df.rename(columns={'index': 'trade_date'}, inplace=True)
        index_df.trade_date = pd.to_datetime(index_df.trade_date)
        index_df.trade_date = index_df.trade_date.map(lambda x: x.date())
        index_df['wind_code'] = code
        index_df['index_name'] = index_name
        index_df.set_index(['wind_code', 'trade_date'], inplace=True)
        table_name = 'wind_index_daily'
        index_df.to_sql(table_name, engine, if_exists='append', index_label=['wind_code', 'trade_date'],
                        dtype={
                        'wind_code': String(20),
                        'trade_date': Date,
                    })
        logger.info('Success import %s - %s with %d data' % (code, index_name, index_df.shape[0]))


def import_wind_index_daily():
    """导入指数数据"""
    engine = get_db_engine()
    yesterday = date.today() - timedelta(days=1)
    query = pd.read_sql_query(
        'select wind_code,index_name, max(trade_date) as latest_date from wind_index_daily group by wind_code', engine)
    query.set_index('wind_code', inplace=True)
    with get_db_session(engine) as session:
        # 获取市场有效交易日数据
        sql_str = "select trade_date from wind_trade_date where trade_date > '2005-1-1'"
        table = session.execute(sql_str)
        trade_date_sorted_list = [t[0] for t in table.fetchall()]
        trade_date_sorted_list.sort()
    date_to = get_last(trade_date_sorted_list, lambda x: x <= yesterday)
    logger.info('%d indexes will been import', query.shape[0])
    for code in query.index:
        date_from = (query.loc[code, 'latest_date'] + timedelta(days=1))
        date_from = get_first(trade_date_sorted_list, lambda x: x >= date_from)
        if date_from is None or date_to is None or date_from > date_to:
            continue
        index_name = query.loc[code, 'index_name']
        temp = rest.wsd(code, "open,high,low,close,volume,amt,turn,free_turn", date_from, date_to)
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
        logger.info('Success update %s - %s' % (code, index_name))


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s: %(levelname)s [%(name)s] %(message)s')

    # 数据库 wind_index_daily 表中新增加指数
    wind_codes = ['000905.SH']
    import_wind_index_daily_first(wind_codes)

    # 每日更新指数信息
    # import_wind_index_daily()
    # fill_wind_index_daily_col()