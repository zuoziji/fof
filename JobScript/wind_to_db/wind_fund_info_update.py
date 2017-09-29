# -*- coding: utf-8 -*-
"""
Created on Fri Feb 17 10:17:38 2017

@author: Administrator
"""
import math
from fh_tools.windy_utils_rest import WindRest
from fh_tools.fh_utils import str_2_date
import pandas as pd
from sqlalchemy.types import String, Date
from datetime import date, timedelta
import logging
# from logging.handlers import RotatingFileHandler
from config_fh import get_db_engine, WIND_REST_URL, get_db_session


# formatter = logging.Formatter('[%(asctime)s:  %(levelname)s  %(name)s %(message)s')
# file_handle = RotatingFileHandler("app.log",mode='a',maxBytes=5 * 1024 * 1024,backupCount=2,encoding=None,delay=0)
# file_handle.setFormatter(formatter)
# console_handle = logging.StreamHandler()
# console_handle.setFormatter(formatter)
# loger = logging.getLogger()
# loger.setLevel(logging.INFO)
# loger.addHandler(file_handle)
# loger.addHandler(console_handle)
# STR_FORMAT_DATETIME = '%Y-%m-%d'


def update_wind_fund_info(get_df=False, mode='replace_insert'):
    table_name = 'wind_fund_info'
    rest = WindRest(WIND_REST_URL)  # 初始化服务器接口，用于下载万得数据
    engine = get_db_engine()
    # 初始化数据库，并获取旧表信息
    old = pd.read_sql_query('select wind_code from %s' % table_name, engine)
    old_set = set(old['wind_code'])
    # 从万得获取最新基金列表
    types = {u'股票多头策略': 1000023122000000,
             u'股票多空策略': 1000023123000000,
             u'其他股票策略': 1000023124000000,
             u'阿尔法策略': 1000023125000000,
             u'其他市场中性策略': 1000023126000000,
             u'事件驱动策略': 1000023113000000,
             u'债券策略': 1000023114000000,
             u'套利策略': 1000023115000000,
             u'宏观策略': 1000023116000000,
             u'管理期货策略': 1000023117000000,
             u'组合基金策略': 1000023118000000,
             u'货币市场策略': 1000023119000000,
             u'多策略': 100002312000000,
             u'其他策略': 1000023121000000}
    df = pd.DataFrame()
    yestday = (date.today() - timedelta(days=1)).strftime('%Y-%m-%d')
    for i in types.keys():
        temp = rest.wset("sectorconstituent", "date=%s;sectorid=%s" % (yestday, str(types[i])))
        temp['strategy_type'] = i
        logging.info('%s sectorconstituent %s df.shape:%s', yestday, i, temp.shape)
        df = pd.concat([df, temp], axis=0)
    fund_types_df = df[['wind_code', 'sec_name', 'strategy_type']]
    new_set = set(fund_types_df['wind_code'])
    target_set = new_set.difference(old_set)  # in new_set but not old_set

    fund_types_df.set_index('wind_code', inplace=True)

    # 获取新成立基金各项基本面信息
    code_list = list(target_set)
    code_count = len(code_list)
    seg_count = 2500
    #    info_df = None
    info_df = pd.DataFrame()
    for n in range(math.ceil(float(code_count) / seg_count)):
        num_start = n * seg_count
        num_end = (n + 1) * seg_count
        num_end = num_end if num_end <= code_count else code_count
        if num_start <= code_count:
            codes = ','.join(code_list[num_start:num_end])
            # 分段获取基金成立日期数据
            info2_df = rest.wss(codes, "fund_setupdate,fund_maturitydate,fund_mgrcomp,fund_existingyear,fund_ptmyear,fund_type,fund_fundmanager")
            logging.info('%05d ) [%d %d]' % (n, num_start, num_end))
            info_df = info_df.append(info2_df)
        else:
            break

    # 没有新增基金，直接退出
    if info_df.shape[0] == 0:
        return

    # 整理数据插入 wind_fund_info 表
    # info_df['FUND_SETUPDATE'] = pd.to_datetime(info_df['FUND_SETUPDATE']).apply(lambda x: x.date())
    info_df['FUND_SETUPDATE'] = info_df['FUND_SETUPDATE'].apply(str_2_date)
    # info_df['FUND_MATURITYDATE'] = pd.to_datetime(info_df['FUND_MATURITYDATE']).apply(lambda x: x.date())
    info_df['FUND_MATURITYDATE'] = info_df['FUND_MATURITYDATE'].apply(str_2_date)
    info_df = fund_types_df.join(info_df, how='right')

    info_df.rename(columns={'FUND_SETUPDATE': 'fund_setupdate',
                            'FUND_MATURITYDATE': 'fund_maturitydate',
                            'FUND_MGRCOMP': 'fund_mgrcomp',
                            'FUND_EXISTINGYEAR': 'fund_existingyear',
                            'FUND_PTMYEAR': 'fund_ptmyear',
                            'FUND_TYPE': 'fund_type',
                            'FUND_FUNDMANAGER': 'fund_fundmanager'
                            }, inplace=True)

    info_df.index.names = ['wind_code']
    info_df.drop_duplicates(inplace=True)
    if mode == 'append':
        info_df.to_sql(table_name, engine, if_exists='append',
                       dtype={
                           'wind_code': String(200),
                           'sec_name': String(200),
                           'strategy_type': String(200),
                           'fund_setupdate': Date,
                           'fund_maturitydate': Date
                       })
    elif mode == 'replace_insert':
        info_df.reset_index(inplace=True)
        data_list = list(info_df.T.to_dict().values())
        sql_str = """REPLACE INTO wind_fund_info
(wind_code,sec_name,strategy_type,fund_setupdate,fund_maturitydate,fund_mgrcomp,fund_existingyear,fund_ptmyear,fund_type,fund_fundmanager)
VALUES
(:wind_code,:sec_name,:strategy_type,:fund_setupdate,:fund_maturitydate,:fund_mgrcomp,:fund_existingyear,:fund_ptmyear,:fund_type,:fund_fundmanager);
"""
        with get_db_session() as session:
            session.execute(sql_str, data_list)
    else:
        raise ValueError('mode="%s" is not available' % mode)
    logging.info('%d funds inserted' % info_df.shape[0])

    # wind_fund_info 表中增量数据插入到 fund_info
    sql_str = """insert into fund_info(wind_code, sec_name, strategy_type, fund_setupdate, fund_maturitydate, fund_mgrcomp, 
fund_existingyear, fund_ptmyear, fund_type, fund_fundmanager)
select wfi.wind_code, wfi.sec_name, wfi.strategy_type, wfi.fund_setupdate, wfi.fund_maturitydate, wfi.fund_mgrcomp, 
wfi.fund_existingyear, wfi.fund_ptmyear, wfi.fund_type, wfi.fund_fundmanager
from wind_fund_info wfi left outer join fund_info fi on wfi.wind_code=fi.wind_code
where fi.wind_code is null"""
    with get_db_session(engine) as session:
        table = session.execute(sql_str)
    logging.info('new data was inserted into fund_info from wind_fund_info table')
    if get_df:
        return info_df


if __name__ == '__main__':
    update_wind_fund_info('wind_fund_info', get_df=False)
