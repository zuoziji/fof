import pandas as pd
import pymysql
import numpy as np
from config_fh import get_db_engine, get_db_session

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


def cal_perform_attrib(fundId, startdate, enddate):
    fac_profit, fac_name, fac_data, origin_data = cal_factor_profit(startdate, enddate)
    # 获取指数数据，中证500
    engine = get_db_engine()
    sql_origin = "select * from index_tradeinfo where index_tradeinfo.Index_Code = '000905.SH' and " + \
                 "(index_tradeinfo.Trade_Date = '%s' or index_tradeinfo.Trade_Date = '%s' )"
    sql_close = sql_origin % (startdate, enddate)
    index_close = pd.read_sql(sql_close, engine)
    index_change = index_close['CLOSE'][index_close.Trade_Date == enddate].values / \
                   index_close['CLOSE'][index_close.Trade_Date == startdate].values - 1
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


if __name__ == '__main__':
    # fac_profit, fac_name,origin_data = pa.cal_facprofit('2016-01-04', '2016-01-08')
    target_data = cal_perform_attrib('XT1605537.XT', '2016-04-22', '2016-04-29')
    print(target_data, len(target_data), type(target_data))
    dflist = []
    # for i in range(10):
    #     dflist.append(target_data.rename(columns=[nav_date])

# print(target_data)
# re_1,re_2,re_3 = pa.cal_PerformAttrib('J11039.OF', '2016-01-04', '2016-01-08')

# print(fac_profit)
