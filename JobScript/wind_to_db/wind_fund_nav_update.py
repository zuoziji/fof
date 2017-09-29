# -*- coding: utf-8 -*-
"""
Created on Fri Feb 17 10:56:11 2017

@author: Administrator
"""

import pandas as pd
from fh_tools.windy_utils_rest import WindRest
from sqlalchemy.types import String, Date
from datetime import datetime, date, timedelta
import logging
from config_fh import get_db_engine, get_db_session, WIND_REST_URL, STR_FORMAT_DATE


def fund_nav_df_2_sql(table_name, fund_nav_df, engine, is_append=True):
    #    print('reorg dfnav data[%d, %d]' % fund_nav_df.shape)
    try:
        fund_nav_df['NAV_DATE'] = pd.to_datetime(fund_nav_df['NAV_DATE']).apply(lambda x: x.date())
    except Exception as exp:
        logging.exception(str(fund_nav_df['NAV_DATE']))
        return None
    trade_date_s = pd.to_datetime(fund_nav_df.index)
    trade_date_latest = trade_date_s.max().date()
    fund_nav_df['trade_date'] = trade_date_s
    fund_nav_df.rename(columns={
        'NAV_DATE': 'nav_date',
        'NAV': 'nav',
        'NAV_ACC': 'nav_acc',
    }, inplace=True)
    fund_nav_df.set_index(['wind_code', 'trade_date'], inplace=True)
    action_str = 'append' if is_append else 'replace'
    #    print('df--> sql fundnav table if_exists="%s"' % action_str)
    fund_nav_df.to_sql(table_name, engine, if_exists=action_str, index_label=['wind_code', 'trade_date'],
                       dtype={
                           'wind_code': String(200),
                           'nav_date': Date,
                           'trade_date': Date,
                       })  # , index=False
    logging.info('%d data inserted', fund_nav_df.shape[0])
    return trade_date_latest


def update_trade_date_latest(wind_code_trade_date_latest):
    params = [{'wind_code': wind_code, 'trade_date_latest': trade_date_latest}
              for wind_code, trade_date_latest in wind_code_trade_date_latest.items()]
    with get_db_session() as session:
        session.execute(
            'update fund_info set trade_date_latest = :trade_date_latest where wind_code = :wind_code',
            params)
    logging.info('%d funds update latest trade date', len(wind_code_trade_date_latest))


def import_wind_fund_nav_to_fund_nav():
    sql_str = """insert fund_nav(wind_code, nav_date, nav, nav_acc, source_mark)
select wfn.wind_code, wfn.nav_date, wfn.nav, wfn.nav_acc, 0 source_mark 
from
(
select wind_code, nav_date, nav, nav_acc, 0
from wind_fund_nav
group by wind_code, nav_date
) as wfn
left outer join
fund_nav fn
on 
wfn.wind_code = fn.wind_code and 
wfn.nav_date = fn.nav_date
where fn.nav is null"""
    with get_db_session() as session:
        session.execute(sql_str)
    logging.info('wind_fund_nav has been imported to fund_nav table')


def update_wind_fund_nav(get_df=False):
    table_name = 'wind_fund_nav'
    rest = WindRest(WIND_REST_URL)  # 初始化数据下载端口
    # 初始化数据库engine
    engine = get_db_engine()
    # 链接数据库，并获取fundnav旧表
    with get_db_session(engine) as session:
        table = session.execute('select wind_code, max(trade_date) from wind_fund_nav group by wind_code')
        fund_trade_date_latest_in_nav_dic = dict(table.fetchall())
        wind_code_set_existed_in_nav = set(fund_trade_date_latest_in_nav_dic.keys())
    # 获取wind_fund_info表信息
    fund_info_df = pd.read_sql_query(
        """SELECT DISTINCT wind_code as wind_code,fund_setupdate,fund_maturitydate,trade_date_latest
from fund_info group by wind_code""", engine)
    trade_date_latest_dic = {wind_code: trade_date_latest for wind_code, trade_date_latest in
                             zip(fund_info_df['wind_code'], fund_info_df['trade_date_latest'])}
    fund_info_df.set_index('wind_code', inplace=True)
    wind_code_list = list(fund_info_df.index)
    date_end = date.today() - timedelta(days=1)
    date_end_str = date_end.strftime(STR_FORMAT_DATE)
    fund_nav_all_df = []
    no_data_count = 0
    code_count = len(wind_code_list)
    # 对每个新获取的基金名称进行判断，若存在 fundnav 中，则只获取部分净值
    wind_code_trade_date_latest = {}
    try:
        for i, wind_code in enumerate(wind_code_list):
            # 设定数据获取的起始日期
            wind_code_trade_date_latest[wind_code] = date_end
            if wind_code in wind_code_set_existed_in_nav:
                trade_latest = fund_trade_date_latest_in_nav_dic[wind_code]
                if trade_latest >= date_end:
                    continue
                date_begin = trade_latest + timedelta(days=1)
            else:
                date_begin = trade_date_latest_dic[wind_code]
            if date_begin is None:
                continue
            elif isinstance(date_begin, str):
                date_begin = datetime.strptime(date_begin, STR_FORMAT_DATE).date()

            if isinstance(date_begin, date):
                if date_begin.year < 1900:
                    continue
                if date_begin > date_end:
                    continue
                date_begin = date_begin.strftime('%Y-%m-%d')
            else:
                continue

            # 尝试获取 fund_nav 数据
            for k in range(2):
                try:
                    fund_nav_tmp_df = rest.wsd(codes=wind_code, fields='nav,NAV_acc,NAV_date', begin_time=date_begin,
                                               end_time=date_end_str, options='Fill=Previous')
                    break
                except Exception as exp:
                    logging.error("%s Failed, ErrorMsg: %s" % (wind_code, str(exp)))
                    continue
            else:
                del wind_code_trade_date_latest[wind_code]
                fund_nav_tmp_df = None

            if fund_nav_tmp_df is None:
                logging.info('%s No data', wind_code)
                del wind_code_trade_date_latest[wind_code]
                no_data_count += 1
                logging.warning('%d funds no data', no_data_count)
            else:
                fund_nav_tmp_df.dropna(how='all', inplace=True)
                df_len = fund_nav_tmp_df.shape[0]
                if df_len == 0:
                    continue
                fund_nav_tmp_df['wind_code'] = wind_code
                # 此处删除 trade_date_latest 之后再加上，主要是为了避免因抛出异常而导致的该条数据也被记录更新
                del wind_code_trade_date_latest[wind_code]
                trade_date_latest = fund_nav_df_2_sql(table_name, fund_nav_tmp_df, engine, is_append=True)
                if trade_date_latest is None:
                    logging.error('%s[%d] data insert failed', wind_code)
                else:
                    wind_code_trade_date_latest[wind_code] = trade_date_latest
                    logging.info('%d) %s updated, %d funds left', i, wind_code, code_count - i)
                    if get_df:
                        fund_nav_all_df = fund_nav_all_df.append(fund_nav_tmp_df)
    finally:
        import_wind_fund_nav_to_fund_nav()
        update_trade_date_latest(wind_code_trade_date_latest)
    return fund_nav_all_df


if __name__ == '__main__':
    # wind_fund_nav_update(get_df=False)
    import_wind_fund_nav_to_fund_nav()
