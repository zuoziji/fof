# -*- coding: utf-8 -*-
"""
Created on 2017/4/20
@author: MG
"""
from datetime import date, datetime, timedelta
import pandas as pd
import numpy as np
from config_fh import get_db_engine, get_db_session, STR_FORMAT_DATE, UN_AVAILABLE_DATE, WIND_REST_URL
from fh_tools.windy_utils_rest import WindRest
import logging


def import_trade_date():
    """
    增量导入交易日数据导数据库表 wind_trade_date，默认导入未来300天的交易日数据
    :return: 
    """
    w = WindRest(WIND_REST_URL)
    engine = get_db_engine()
    trade_date_start = None
    with get_db_session(engine) as session:
        try:
            trade_date_max = session.execute('select max(trade_date) from wind_trade_date').first()[0]
            if trade_date_max is not None:
                trade_date_start = (trade_date_max + timedelta(days=1)).strftime(STR_FORMAT_DATE)
        except Exception as exp:
            logging.exception("交易日获取异常")
        if trade_date_start is None:
            trade_date_start = '1990-01-01'

    end_date_str = (date.today() + timedelta(days=310)).strftime(STR_FORMAT_DATE)
    date_df = w.tdays(trade_date_start, end_date_str)
    if date_df is not None:
        print(date_df.shape)
        date_df = date_df.set_index('date').rename(columns={'date': 'trade_date'})
        date_df.to_sql('wind_trade_date', engine, if_exists='append')
        logging.info('%d trade date has been imported', date_df.shape[0])
