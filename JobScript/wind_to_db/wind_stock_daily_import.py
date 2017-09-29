from datetime import date, datetime, timedelta
import pandas as pd
import numpy as np
from config_fh import get_db_engine, get_db_session, STR_FORMAT_DATE, UN_AVAILABLE_DATE, WIND_REST_URL
from fh_tools.windy_utils_rest import WindRest
from fh_tools.fh_utils import get_last, get_first
import logging
from sqlalchemy.types import String, Date, Float, Integer

DATE_BASE = datetime.strptime('2005-01-01', STR_FORMAT_DATE).date()
ONE_DAY = timedelta(days=1)


def get_datelist(startdate, enddate):
    datelist = w.tdays(startdate, enddate)
    datelist = datelist.Data[0]
    datelist = [i.strftime(STR_FORMAT_DATE) for i in datelist]
    return datelist


def get_stockcodes(targerdate):
    codesinfo = w.wset("sectorconstituent", "date=%s;windcode=881001.WI" % targerdate)
    codes = codesinfo.Data[1]
    names = codesinfo.Data[2]
    return codes, names


def get_tradeinfo(stockcode, stockname, startdate, enddate):
    wind_indictor_str = "open,high,low,close,adjfactor,volume,amt,pct_chg,maxupordown," + \
                        "swing,turn,free_turn,trade_status,susp_days"

    stock_tradeinfo = w.wsd(stockcode, wind_indictor_str, startdate, enddate)
    stock_times = stock_tradeinfo.Times
    stock_data = stock_tradeinfo.Data
    stockre = pd.DataFrame()
    stockre['Trade_Date'] = [i.strftime('%Y-%m-%d') for i in stock_times]
    stockcode_list = [stockcode] * len(stock_data[0])
    stockname_list = [stockname] * len(stock_data[0])
    stockre['Stock_Code'] = stockcode_list
    stockre['Stock_Name'] = stockname_list
    wind_list = wind_indictor_str.split(',')
    for index, wincode in enumerate(wind_list):
        stockre[wincode] = stock_data[index]
    # 去除nan数据
    open_tmp = stockre['close']
    open_tmp_nan = np.isnan(open_tmp)
    stockre = stockre[open_tmp_nan != 1]
    return stockre


def save_df2db(stockre, indexnames, conn):
    stockre.to_sql('stock_tradeinfo', conn, if_exists='append', flavor='mysql',
                   index_label=['Trade_Date', 'Stock_Code', 'Stock_Name'])


def import_stock_daily():
    w = WindRest(WIND_REST_URL)
    engine = get_db_engine()
    with get_db_session(engine) as session:
        # 获取每只股票最新交易日数据
        sql_str = 'select wind_code, max(Trade_date) from wind_stock_daily group by wind_code'
        table = session.execute(sql_str)
        stock_trade_date_latest_dic = dict(table.fetchall())
        # 获取市场有效交易日数据
        sql_str = "select trade_date from wind_trade_date where trade_date > '2005-1-1'"
        table = session.execute(sql_str)
        trade_date_sorted_list = [t[0] for t in table.fetchall()]
        trade_date_sorted_list.sort()
        # 获取每只股票上市日期、退市日期
        table = session.execute('SELECT wind_code, ipo_date, delist_date FROM wind_stock_info')
        stock_date_dic = {wind_code: (ipo_date, delist_date if delist_date is None or delist_date > UN_AVAILABLE_DATE else None) for
                          wind_code, ipo_date, delist_date in table.fetchall()}
    today_t_1 = date.today() - ONE_DAY
    data_df_list = []

    try:
        for wind_code, date_pair in stock_date_dic.items():
            date_ipo, date_delist = date_pair
            # 获取 date_from
            if wind_code in stock_trade_date_latest_dic:
                date_latest_t1 = stock_trade_date_latest_dic[wind_code] + ONE_DAY
                date_from = max([date_latest_t1, DATE_BASE, date_ipo])
            else:
                date_from = max([DATE_BASE, date_ipo])
            date_from = get_first(trade_date_sorted_list, lambda x: x >= date_from)
            # 获取 date_to
            if date_delist is None:
                date_to = today_t_1
            else:
                date_to = min([date_delist, today_t_1])
            date_to = get_last(trade_date_sorted_list, lambda x: x <= date_to)
            if date_from is None or date_to is None or date_from > date_to:
                continue
            # 获取股票量价等行情数据
            wind_indictor_str = "open,high,low,close,adjfactor,volume,amt,pct_chg,maxupordown," + \
                                "swing,turn,free_turn,trade_status,susp_days"
            data_df = w.wsd(wind_code, wind_indictor_str, date_from, date_to)
            if data_df is None:
                logging.warning('%s has no data during %s %s', wind_code, date_from, date_to)
                continue
            logging.info('%d data of %s', data_df.shape[0], wind_code)
            data_df['wind_code'] = wind_code
            data_df_list.append(data_df)
    finally:
        # 导入数据库
        if len(data_df_list) > 0:
            data_df_all = pd.concat(data_df_list)
            data_df_all.index.rename('trade_date', inplace=True)
            data_df_all.reset_index(inplace=True)
            data_df_all.set_index(['wind_code', 'trade_date'], inplace=True)
            data_df_all.to_sql('wind_stock_daily', engine, if_exists='append',
                               dtype={
                                   'wind_code': String(20),
                                   'trade_date': Date,
                                   'open': Float,
                                   'high': Float,
                                   'low': Float,
                                   'close': Float,
                                   'adjfactor': Float,
                                   'volume': Float,
                                   'amt': Float,
                                   'pct_chg': Float,
                                   'maxupordown': Integer,
                                   'swing': Float,
                                   'turn': Float,
                                   'free_turn': Float,
                                   'trade_status': String(20),
                                   'susp_days': Integer,
                               }
                               )
            logging.info('%d data imported', data_df_all.shape[0])


if __name__ == '__main__':
    import_stock_daily()

# startdate = '2005-01-03'
# enddate = '2014-12-31'
# stockcodes, stocknames = get_stockcodes(enddate)
# stockloc = 1085
# costtime = 0
# stockcodes = stockcodes[stockloc:]
# stocknames = stocknames[stockloc:]
# with get_db_session() as session:
#     for stockcode, stockname in zip(stockcodes, stocknames):
#         timestart = time.time()
#         stockre = get_tradeinfo(stockcode, stockname, startdate, enddate)
#         stockre.set_index(['Trade_Date', 'Stock_Code', 'Stock_Name'], inplace=True)  #
#         indexnames = ['Trade_Date', 'Stock_Code', 'Stock_Name']
#         save_df2db(stockre, indexnames, session)
#         timeend = time.time()
#         costtime = costtime + timeend - timestart
#         # conn.close()
#         print('Success Transfer %s, %s' % (stockcode, stockname),
#               "本次耗时：%d" % round(timeend - timestart), "累计耗时：%d" % costtime)
