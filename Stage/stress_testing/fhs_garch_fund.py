# -*- coding: utf-8 -*-
"""
1）根据 wind_code 获取净值记录，调整成 return rate 生成csv文件
2）调用 fhs_garch.R，返回对应压力测试 csv文件
3）读取压力测试文件，进行整个
"""
from rpy2 import robjects
from os import path
import matplotlib.pyplot as plt  # pycharm 需要通过现实调用 plt.show 才能显示plot
from itertools import product
import numpy as np
import pandas as pd
from datetime import datetime, date, timedelta
import matplotlib.pyplot as plt
from fh_tools.fh_utils import get_cache_file_path
import json
from config_fh import get_db_engine, ANALYSIS_CACHE_FILE_NAME, get_db_session, get_redis, STR_FORMAT_DATE,\
    STRESS_TESTING_SIMULATE_COUNT_FHS_GARCH
import logging
import numpy as np
logger = logging.getLogger()

DATE_STR_FORMAT = '%Y-%m-%d'
ENABLE_CACHE = False
SQL_FUND_NAV_FRIDAY = """SELECT nav_date_friday, nav_acc
FROM fund_nav,
	(
	SELECT wind_code,
		   (nav_date + INTERVAL (4 - WEEKDAY(nav_date)) DAY) AS nav_date_friday,
		   MAX(nav_date) AS nav_date_max
	FROM fund_nav
    where wind_code = :wind_code1 
	GROUP BY wind_code, nav_date_friday
	) as fund_nav_g
where fund_nav.wind_code = :wind_code2 
and fund_nav.wind_code = fund_nav_g.wind_code
and fund_nav.nav_date = fund_nav_g.nav_date_max"""
QUANTILE_LIST = [.05, .15, .25, .50, .75, .85, .95]


def get_fund_nav_acc(wind_code):
    """
    获取制定基金净值数据，日期转换到每周周五，每周之多一条数据
    :param wind_code: 
    :return: 
    """
    with get_db_session() as session:
        table = session.execute(SQL_FUND_NAV_FRIDAY, {'wind_code2': wind_code, 'wind_code1': wind_code})
        date_nav_dic = dict(table.fetchall())
    nav_series = pd.Series(date_nav_dic, name=wind_code)
    return nav_series


def get_return_rate_csv(nav_s, input_file_path):
    """
    将净值数据转化为收益率数据生成csv文件，同时返回对应的日期序列
    如果该净值没有回撤，则返回空，同时记录warning
    :param nav_s: 净值数据序列
    :param input_file_path: 生成csv文件的路径
    :return:
    """
    # wind_code = nav_s.name
    return_rate_s = nav_s.pct_change()
    return_rate_s.dropna(inplace=True)
    if ENABLE_CACHE and path.exists(input_file_path):
        date_index = nav_s.index
    else:
        return_rate_s.name = 'x'
        # nav_df = pd.DataFrame([nav_s, return_rate_s]).T
        # print(type(nav_df))
        # print(nav_df)
        if any(return_rate_s < 0):
            return_rate_s.to_csv(input_file_path, index=False, header=True)
            date_index = return_rate_s.index
        else:
            logger.warning('wind_code:%s will be ignored because of no drawback' % nav_s.name)
            date_index = None
        return date_index


def cal_fhs_garch(wind_code, simulate_count, nav_acc_s):
    """
    获取子基金净值，生成csv文件，调用FHSGACH3Py.R文件计算，返回 csv文件
    :param wind_code: 基金代码
    :return: 压力测试数据 DataFrame, index为日期，column每一组压力测试数据
    """
    nav_date_latest = nav_acc_s.index[-1]
    input_file_name = '%s_%s.csv' % (wind_code, nav_date_latest.strftime(DATE_STR_FORMAT))
    logger.debug("%s 将被  robjects.r['FHSGACH3Py'] 引用", input_file_name)
    input_file_path = get_cache_file_path(ANALYSIS_CACHE_FILE_NAME, input_file_name)
    date_index = get_return_rate_csv(nav_acc_s, input_file_path)
    if date_index is None:
        logger.warning('%s has not data for fhs_garch test', wind_code)
        return None
    output_file_name = '%s_simulate_%s.csv' % (wind_code, nav_date_latest.strftime(DATE_STR_FORMAT))
    output_file_path = get_cache_file_path(ANALYSIS_CACHE_FILE_NAME, output_file_name)
    if not ENABLE_CACHE or not path.exists(output_file_path):
        # 测试使用，临时将 input_file_name 替换为 样例csv文件
        # input_file_name = 'rr_example.csv'
        logger.info('invoke fhs_garch.R for %s %d', wind_code, simulate_count)
        r_file_path = get_cache_file_path('stress_testing', 'fhs_garch.R', create_if_not_found=False)
        robjects.r.source(r_file_path)
        robjects.r['FHSGACH3Py'](input_file_path, output_file_path, simulate_count)
    # 读取 csv文件，重置索引
    simulate_df = pd.DataFrame.from_csv(output_file_path).reset_index().set_index(date_index)
    # print('simulate_df.shape', simulate_df.shape)
    return simulate_df


def cal_fof_fhs_garch(wind_code_list, simulate_count, mode='pad'):
    """
    对制定的基金组合进行fhs-garch压力测试
    :param wind_code_list: 基金列表
    :param simulate_count: 对每一只子基金的压力测试数量
    :return: 返回每一只子基金的压力测试数据，各个子基金日期取交集
    """
    min_acceptable_data_len = 10
    # 获取净值数据
    nav_acc_s_dic = {}
    date_set = None
    for wind_code in wind_code_list:
        nav_acc_s = get_fund_nav_acc(wind_code)
        if nav_acc_s.shape[0] <= min_acceptable_data_len:
            continue
        if date_set is None:
            date_set = set(nav_acc_s.index)
        else:
            date_set &= set(nav_acc_s.index)
        nav_acc_s_dic[wind_code] = nav_acc_s
    date_list = list(date_set)
    # 生成 压力测试结果
    simulate_df_dic = {}
    if mode == 'pad':
        date_list.sort()
        for wind_code in nav_acc_s_dic.keys():
            nav_acc_s = nav_acc_s_dic[wind_code][date_list]
            try:
                simulate_df = cal_fhs_garch(wind_code, simulate_count, nav_acc_s)
                if simulate_df is not None:
                    simulate_df_dic[wind_code] = simulate_df
            except:
                logger.exception('cal_fhs_garch for %s got exception:', wind_code)
    elif len(date_list) == 0:
        logger.warning('%s 历史业绩交集为空，无法进行组合压力测试' % wind_code_list)
    return simulate_df_dic


def recursive_composition(key_list, key_index, data_df_dic, data_s=None, label_str=[]):
    """
    递归调用对key_list中每一个子基金，对应的 data_df_dic 中的 DataFram 进行组合，加权求和
    :param key_list:
    :param key_index:
    :param data_df_dic:
    :param data_s:
    :param label_str:
    :return:
    """
    key_value = key_list[key_index]
    key_index_next = 0 if len(key_list) <= (key_index + 1) else key_index + 1
    data_df = data_df_dic[key_value]
    data_count = data_df.shape[1]
    if key_index_next != 0:
        data_s_list = []
    else:
        data_s_list = [''] * data_count
    for n in range(data_count):
        data_s_curr = data_df.iloc[:, n]
        label_str_new = label_str.copy()
        label_str_new.append(str(n))
        if data_s is not None:
            data_s_new = data_s + data_s_curr
        else:
            data_s_new = data_s_curr
        if key_index_next != 0:
            data_s_list_new = recursive_composition(key_list, key_index_next, data_df_dic, data_s_new, label_str_new)
            data_s_list.extend(data_s_list_new)
        else:
            data_s_new.rename('_'.join(label_str_new))
            data_s_list[n] = data_s_new
    return data_s_list


def iter_composition(data_df_dic, simulate_count):
    """
    迭代器循环key_list中每一个子基金，对应的 data_df_dic 中的 DataFram 进行组合，加权求和
    :param data_df_dic:
    :param simulate_count:
    :return:
    """
    key_list = list(data_df_dic.keys())
    key_count = len(key_list)
    iter_result = product(range(simulate_count), repeat=key_count)
    data_s_list = [""] * (simulate_count ** key_count)
    # print("data_s_list len:", simulate_count ** key_count)
    data_s_count = 0
    for comp in iter_result:
        data_s = None
        for n_key in range(key_count):
            key = key_list[n_key]
            data_df = data_df_dic[key]
            n = comp[n_key]
            if data_s is None:
                data_s = data_df.iloc[:, n]
            else:
                data_s += data_df.iloc[:, n]
        data_s_list[data_s_count] = data_s
        data_s_count += 1
    return data_s_list


def fof_fhs_garch(wind_code_list, simulate_count):
    """
    对子基金组合进行组合压力测试
    :param wind_code_list: 子基金列表
    :param simulate_count: 每一只子基金的压力测试数量。母基金压力测试数量= simulate_count ** 子基金数量
    :return: 组合压力测试df，每一支子基金压力测试df
    """
    simulate_df_dic = cal_fof_fhs_garch(wind_code_list, simulate_count)
    sub_fund_count = len(simulate_df_dic)
    if simulate_df_dic is None or sub_fund_count == 0:
        logger.warning('FHS GARCH has no data for composition with fund codes:\n%s', wind_code_list)
        return None, None
    simulate_pct_df_dic = {}
    wind_code_new_list = list(simulate_df_dic.keys())
    # wind_code_count = len(wind_code_new_list)
    weight = 1.0 / sub_fund_count
    # weighted_dic = {}
    # 将子基金的压力测试数量连乘获得混合后的总数
    tot_comp_count = np.prod([df.shape[1] for df in simulate_df_dic.values()])
    # 如果混合后总数大于100万，则降低数据数量
    MAX_ALLOW_COUNT = 1000000
    if tot_comp_count > MAX_ALLOW_COUNT:
        base_div = (tot_comp_count / MAX_ALLOW_COUNT) ** (1.0 / sub_fund_count)
    else:
        base_div = 1

    for wind_code in wind_code_new_list:
        simulate_df = simulate_df_dic[wind_code]
        # simulate_df.plot(legend=False)
        # plt.show()
        logger.info('fund %s : %s', wind_code, simulate_df.shape)
        # print(simulate_df.head())
        simulate_pct_df = simulate_df.pct_change()
        simulate_pct_df.fillna(0, inplace=True)
        # print("simulate_pct_df")
        # print(simulate_pct_df.head())
        if base_div > 1:
            # 降低模拟数据的数量级
            willing_count = int(simulate_pct_df.shape[1] / base_div)
            simulate_pct_df = simulate_pct_df.ix[:, :willing_count]
        simulate_pct_df_dic[wind_code] = simulate_pct_df * weight
        # weighted_dic[wind_code] = weighted
    # time_1 = time.time()
    logger.debug('对%s进行组合', wind_code_new_list)
    data_s_list = recursive_composition(wind_code_new_list, 0, simulate_pct_df_dic)
    # time_2 = time.time()
    # data_s_list = iter_composition(simulate_pct_df_dic, simulate_count)
    # time_3 = time.time()
    # print("recursive_composition cost:", time_2 - time_1)
    # print("iter_composition cost:", time_3 - time_2)
    # print(len(data_s_list))
    data_df = pd.concat(data_s_list, axis=1)
    # print(data_df.shape)
    simulate_comp_df = (data_df + 1).cumprod()
    # print("simulate_comp_df:", simulate_comp_df.shape)
    # print(simulate_comp_df.head())
    return simulate_comp_df, simulate_df_dic


def savefig_df(df, file_name):
    file_path = get_cache_file_path(ANALYSIS_CACHE_FILE_NAME, file_name)
    df.plot(legend=False)
    plt.savefig(file_path)
    return file_path


def plot_fof_fhs_garch(wind_code_list, simulate_count):
    simulate_comp_df, simulate_df_dic = fof_fhs_garch(wind_code_list, simulate_count)
    if simulate_comp_df is None:
        return None
    print("simulate_comp_df.shape:", simulate_comp_df.shape)
    # plt.show()
    fund_file_path_dic = {}
    figure_time = datetime.now()
    # for wind_code, simulate_df in simulate_df_dic.items():
    #     file_name = '%s_%s.png' % (wind_code, figure_time.strftime('%Y_%m_%d %H_%M_%S'))
    #     fof_file_path = savefig_df(simulate_df, file_name)
    #     fund_file_path_dic[wind_code] = fof_file_path
    fof_file_name = 'fof_%s.png' % (figure_time.strftime('%Y_%m_%d %H_%M_%S'))
    fof_file_path = savefig_df(simulate_comp_df, fof_file_name)
    fund_file_path_dic['fof'] = fof_file_path
    return fund_file_path_dic


def do_fhs_garch(wind_code_p_list=[]):
    """
    后台作业，对数据库中所有fof基金进行压力测试
    如果 wind_code_p_list 不为空，则近执行 wind_code_p_list 中的母基金
    :return: 
    """
    # sql_str = """select id, ffp.wind_code_p, wind_code_s, date_adj, invest_scale
    # from fof_fund_pct ffp,
    # (
    # select wind_code_p, max(date_adj) date_latest from fof_fund_pct group by wind_code_p
    # ) ff_date_latest
    # where ffp.wind_code_p = ff_date_latest.wind_code_p
    # and ffp.date_adj = ff_date_latest.date_latest"""
    sql_str = """select id, ffp.wind_code_p, ffp.wind_code_s, wind_code, date_adj, invest_scale 
from fof_fund_pct ffp,
(
	select wind_code_p, max(date_adj) date_latest 
    from fof_fund_pct group by wind_code_p
) ff_date_latest,
fund_essential_info ffm
where ffp.wind_code_p = ff_date_latest.wind_code_p
and ffp.wind_code_s = ffm.wind_code_s
and ffp.date_adj = ff_date_latest.date_latest"""
    engine = get_db_engine()
    fof_fund_df = pd.read_sql(sql_str, engine)
    # 按 母基金代码进行分组
    wind_code_fund_dic = dict(list(fof_fund_df.groupby('wind_code_p')))
    # 按 基金代码进行分组，对应所有 wind_code_s 代码
    wind_code_df_dic = dict(list(fof_fund_df[['wind_code', 'wind_code_s']].groupby('wind_code')))
    wind_code_s_dic = {wind_code: set(df['wind_code_s'].values) for wind_code, df in wind_code_df_dic.items()}

    date_to = date.today()
    date_to_str = date_to.strftime(STR_FORMAT_DATE)
    date_from = date_to - timedelta(days=365)
    date_from_str = date_from.strftime(STR_FORMAT_DATE)
    simulate_count = STRESS_TESTING_SIMULATE_COUNT_FHS_GARCH
    r = get_redis()
    if wind_code_p_list is not None and len(wind_code_p_list) > 0:
        wind_code_p_set = set(wind_code_p_list)
    else:
        wind_code_p_set = None
    for wind_code_p, fof_fund_sub_df in wind_code_fund_dic.items():
        if wind_code_p_set is not None and wind_code_p not in wind_code_p_set:
            logger.debug('%s 不在列表中 跳过', wind_code_p)
            continue
        wind_code_list = list(fof_fund_sub_df['wind_code'])  # ['XT1410445.XT', 'J11039.OF']
        wind_code_count = len(wind_code_list)
        if wind_code_count <= 0:
            continue
        simulate_comp_df, simulate_df_dic = fof_fhs_garch(wind_code_list, simulate_count)
        if simulate_comp_df is None:
            logger.error('%s has no FHS GARCH test data. sub fund list: %s', wind_code_p, wind_code_list)
            continue
        for wind_code, simulate_df in simulate_df_dic.items():

            time_line = simulate_df.index
            time_line = [i.strftime('%Y-%m-%d') for i in time_line]
            df = simulate_df.T.quantile(QUANTILE_LIST).T
            result = {"time": time_line, "data": [{"name": i, "data": np.array(df[i]).tolist()} for i in df.columns]}
            result['show_count'] = simulate_count
            # 将wind_code 及 wind_code_s 对应的压力测试结果插入redis
            key = '%s_%s' % (wind_code, 'fhs_garch')
            logger.info('%s has been complete,', key)
            r.set(key, json.dumps(result))
            for wind_code_s in wind_code_s_dic[wind_code]:
                key = '%s_%s' % (wind_code_s, 'fhs_garch')
                logger.info('%s has been complete,', key)
                r.set(key, json.dumps(result))

        time_line = simulate_comp_df.index
        time_line = [i.strftime('%Y-%m-%d') for i in time_line]
        df = simulate_comp_df.T.quantile(QUANTILE_LIST).T
        result = {"time": time_line, "data": [{"name": i, "data": np.array(df[i]).tolist()} for i in df.columns],
                  'show_count': simulate_count}
        key = '%s_%s' % (wind_code_p, 'fhs_garch')
        logger.info('%s has benn complate', key)
        r.set(key, json.dumps(result))


def do_fhs_garch_4_scheme(scheme_id):
    """
    根据 scheme_id 计算相关组合的 fhs-garch 压力测试
    :param scheme_id: 
    :return: 
    """

    sql_str = "SELECT wind_code, invest_scale FROM scheme_fund_pct where scheme_id=%s"
    engine = get_db_engine()
    fund_pct_df = pd.read_sql(sql_str, engine, params=[str(scheme_id)])

    simulate_count = STRESS_TESTING_SIMULATE_COUNT_FHS_GARCH
    wind_code_list = list(fund_pct_df['wind_code'])  # ['XT1410445.XT', 'J11039.OF']
    wind_code_count = len(wind_code_list)
    if wind_code_count <= 0:
        logger.warning('scheme %s has no sub fund list', scheme_id)
        return
    # 执行 fhs-garch压力测试
    simulate_comp_df, simulate_df_dic = fof_fhs_garch(wind_code_list, simulate_count)
    if simulate_comp_df is None:
        logger.error('scheme %s has no FHS GARCH test data. sub fund list: %s', scheme_id, wind_code_list)
        return
    logger.info('do_fhs_garch for %d on wind_code_p with %s', scheme_id, wind_code_list)
    # 将组合压力测试结果存储到 redis 上面
    r = get_redis()
    for wind_code, simulate_df in simulate_df_dic.items():
        time_line = simulate_df.index
        time_line = [i.strftime('%Y-%m-%d') for i in time_line]
        df = simulate_df.T.quantile(QUANTILE_LIST).T
        result = {"time": time_line, "data": [{"name": i, "data": np.array(df[i]).tolist()} for i in df.columns],
                  'show_count': simulate_count}
        # 将wind_code 及 wind_code_s 对应的压力测试结果插入redis
        key = '%s_%s' % (wind_code, 'fhs_garch')
        logger.info('%s has been complete,', key)
        r.set(key, json.dumps(result))

    time_line = simulate_comp_df.index
    time_line = [i.strftime('%Y-%m-%d') for i in time_line]
    df = simulate_comp_df.T.quantile(QUANTILE_LIST).T
    result = {"time": time_line, "data": [{"name": i, "data": np.array(df[i]).tolist()} for i in df.columns],
              'show_count': simulate_count}
    key = 'scheme_%s_%s' % (scheme_id, 'fhs_garch')
    val_str = json.dumps(result)
    logger.info('%s has benn complate\n%s', key, val_str)
    r.set(key, val_str)

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s: %(levelname)s [%(name)s:%(funcName)s] %(message)s')
    # wind_code = 'XT1410445.XT'
    # simulate_df = cal_fhs_garch(wind_code, 10)
    # print(simulate_df.head())
    # simulate_df.plot(legend=False)
    # plt.show()
    # wind_code_list = ['XT144787.XT', 'XT146247.XT', 'XT147219.XT']
    # wind_code_list = ['XT1507116.XT', 'XT1502707.XT', 'XT1512218.XT', 'XT1510534.XT', 'XT1522002.XT', 'XT144816.XT']
    # wind_code_list = ['XT1614831.XT', 'XT1520429.XT', 'XT145733.XT', 'ZG130482.OF', 'ZG130483.OF']
    # simulate_count = 10
    # fund_file_path_dic = plot_fof_fhs_garch(wind_code_list, simulate_count)
    # print(fund_file_path_dic)
    # wind_code_p_list = ['FHF-101602']
    # do_fhs_garch(wind_code_p_list=wind_code_p_list)
    scheme_id = 210
    do_fhs_garch_4_scheme(scheme_id)
