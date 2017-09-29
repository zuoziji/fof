# -*- coding: utf-8 -*-
import numpy as np
from config_fh import get_db_engine, get_db_session, WIND_REST_URL, STR_FORMAT_DATE
from fh_tools.windy_utils_rest import WindRest
from datetime import date, datetime
import logging


def save_factor_explore(date_start, date_end):
    logging.info('save_factor_explore(%s, %s)', date_start, date_end)
    rest = WindRest(WIND_REST_URL)
    date_start = rest.tdaysoffset(0, date_start)  # 获取起始交易日
    date_end = rest.tdaysoffset(0, date_end)  # 获取结束交易日
    # 提取数据
    # 全部数据按照20天计算
    lagnum = 20
    date_start_getdata = rest.tdaysoffset(-1 * (lagnum - 1), date_start)
    # 首先，提取股票代码
    options_set = "date = %s; windcode = 881001.WI" % date_end
    stock_info_df = rest.wset("sectorconstituent", options_set)
    stock_code_s = stock_info_df['wind_code']
    stock_name_s = stock_info_df['sec_name']
    stock_count = len(stock_name_s)
    # 获取各个因子
    options_stock = "unit=1;currencyType=;PriceAdj=B"
    data_shortnames = "close,pe_ttm,mkt_cap_float,turn"
    logging.info('date between %s and %s', date_start_getdata, date_end)
    stock_num = 0
    logging.info('Need To update %d Stocks' % len(stock_code_s))
    for stock_code, stock_name in zip(stock_code_s, stock_name_s):
        stock_factor_df = rest.wsd(stock_code, data_shortnames, date_start_getdata, date_end, options_stock)
        stock_factor_df['fac_Inverse_Moment'] = stock_factor_df['CLOSE'] / stock_factor_df['CLOSE'].shift(19)  # 19日涨跌判断
        stock_factor_df['fac_Mv'] = np.log10(stock_factor_df.MKT_CAP_FLOAT)
        stock_factor_df['fac_Pe'] = stock_factor_df['PE_TTM']
        stock_factor_df['fac_Turn'] = stock_factor_df['TURN'].rolling(lagnum).mean()
        stock_factor_df['fac_Vol'] = (stock_factor_df['CLOSE'] / stock_factor_df['CLOSE'].shift(1)).rolling(
            lagnum).std()
        stock_factor_df['Stock_Name'] = stock_name
        stock_factor_df['Stock_Code'] = stock_code
        stock_factor_df.dropna(how='any', inplace=True)
        factor_name_saved = ['Stock_Name', 'Stock_Code', 'fac_Inverse_Moment', 'fac_Mv', 'fac_Pe', 'fac_Turn',
                             'fac_Vol']
        engine = get_db_engine()
        stock_factor_df[factor_name_saved].to_sql('stock_facexposure', engine, if_exists='append',
                                                  index_label=['Trade_Date'])
        stock_num += 1
        logging.info('Successful Input %s [%d / %d] stock' % (stock_name, stock_num, stock_count))


def update_factors():
    rest = WindRest(WIND_REST_URL)
    sql_str = 'SELECT max(Trade_Date) as trade_date_latest FROM stock_facexposure'
    with get_db_session() as session:
        trade_date_latest = session.execute(sql_str).fetchone()[0]
        date_start_str = rest.tdaysoffset(1, trade_date_latest)
        date_today_str = date.today().strftime('%Y-%m-%d')
        date_end_str = rest.tdaysoffset(-1, date_today_str)

    if datetime.strptime(date_start_str, STR_FORMAT_DATE) <= datetime.strptime(date_end_str, STR_FORMAT_DATE):
        save_factor_explore(date_start_str, date_end_str)

if __name__ == '__main__':
    # Factor_Profit.cal_FactorProfit('2016-01-02', '2016-05-04')
    # fp = Factor_Profit()
    # fp.save_factor_explore('2016-01-02', '2017-03-05')
    # fp.save_Indexdata('2016-01-02', '2017-03-05')
    # save_factor_explore('2016-01-02', '2017-03-05')
    # save_Indexdata('2016-01-02', '2017-03-05')
    update_factors()
