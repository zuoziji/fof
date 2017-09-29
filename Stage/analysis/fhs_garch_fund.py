# -*- coding: utf-8 -*-
"""
1）根据 wind_code 获取净值记录，调整成 return rate 生成csv文件
2）调用 fhs_garch.R，返回对应压力测试 csv文件
3）读取压力测试文件，进行整个
"""
from rpy2 import robjects
from os import path
import pandas as pd
import matplotlib
import matplotlib.pyplot as plt  # pycharm 需要通过现实调用 plt.show 才能显示plot
from backends.config_fh import get_db_session, ANALYSIS_CACHE_FILE_NAME
from backends.fh_tools.fh_utils import get_cache_file_path
import logging
from itertools import product
# import time
from datetime import datetime

DATE_STR_FORMAT = '%Y-%m-%d'
ENABLE_CACHE = False


def get_fund_nav_acc(wind_code):
    # 链接数据库，并获取fundnav旧表
    with get_db_session() as session:
        table = session.execute(
            # 'select adddate(nav_date, 4 - weekday(nav_date)), max(nav_acc) as nav_acc_max from fundnav where wind_code=:code group by nav_date order by nav_date',
            'select * from (select adddate(nav_date, 4 - weekday(nav_date)) as nav_date2, max(nav_acc) as nav_acc_max from fundnav where wind_code=:code group by nav_date) as nav_t group by nav_date2 order by nav_date2',
            {'code': wind_code})
    date_nav_dic = dict()
    nav_acc_last = 0
    # nav_date_latest = None
    # 将于上一净值数据相同的记录剔除
    for content in table.fetchall():
        nav_acc = content[1]
        nav_date_latest = content[0]
        if nav_acc_last != nav_acc:
            date_nav_dic[nav_date_latest] = nav_acc
            nav_acc_last = nav_acc

    # date_nav_dic = {content[0]: content[1] }
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
    return_rate_s.fillna(0, inplace=True)
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
            logging.warning('wind_code:%s will be ignored because of no drawback' % nav_s.name)
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
    input_file_path = get_cache_file_path(ANALYSIS_CACHE_FILE_NAME, input_file_name)
    date_index = get_return_rate_csv(nav_acc_s, input_file_path)
    if date_index is None:
        return None
    output_file_name = '%s_simulate_%s.csv' % (wind_code, nav_date_latest.strftime(DATE_STR_FORMAT))
    output_file_path = get_cache_file_path(ANALYSIS_CACHE_FILE_NAME, output_file_name)
    if not ENABLE_CACHE or not path.exists(output_file_path):
        # 测试使用，临时将 input_file_name 替换为 样例csv文件
        # input_file_name = 'rr_example.csv'
        print('invoke fhs_garch.R for', wind_code, simulate_count)

        robjects.r.source(r"/root/NewTemplate/analysis/fhs_garch.R")

        robjects.r['FHSGACH3Py'](input_file_path, output_file_path, simulate_count)
    # 读取 csv文件，重置索引
    simulate_df = pd.DataFrame.from_csv(output_file_path).reset_index().set_index(date_index)
    # print('simulate_df.shape', simulate_df.shape)
    return simulate_df


def cal_fof_fhs_garch(wind_code_list, simulate_count):
    """
    对制定的基金组合进行fhs-garch压力测试
    :param wind_code_list: 基金列表
    :param simulate_count: 对每一只子基金的压力测试数量
    :return: 返回每一只子基金的压力测试数据，各个子基金日期取交集
    """
    # 获取净值数据
    nav_acc_s_dic = {}
    date_set = None
    for wind_code in wind_code_list:
        nav_acc_s = get_fund_nav_acc(wind_code)
        if len(nav_acc_s) == 0:
            continue
        if date_set is None:
            date_set = set(nav_acc_s.index)
        else:
            date_set &= set(nav_acc_s.index)
        nav_acc_s_dic[wind_code] = nav_acc_s
    date_list = list(date_set)
    if len(date_list) == 0:
        logging.warning('%s 历史业绩交集为空，无法进行组合压力测试' % wind_code_list)
        return None
    date_list.sort()
    # 生成 压力测试结果
    simulate_df_dic = {}
    for wind_code in nav_acc_s_dic.keys():
        nav_acc_s = nav_acc_s_dic[wind_code][date_list]
        simulate_df = cal_fhs_garch(wind_code, simulate_count, nav_acc_s)
        if simulate_df is not None:
            simulate_df_dic[wind_code] = simulate_df
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
    if simulate_df_dic is None or len(simulate_df_dic) == 0:
        return None, None
    simulate_pct_df_dic = {}
    wind_code_new_list = list(simulate_df_dic.keys())
    wind_code_count = len(wind_code_new_list)
    weight = 1.0 / wind_code_count
    # weighted_dic = {}
    for wind_code in wind_code_new_list:
        simulate_df = simulate_df_dic[wind_code]
        # simulate_df.plot(legend=False)
        # plt.show()
        # print(wind_code, ":", simulate_df.shape)
        # print(simulate_df.head())
        simulate_pct_df = simulate_df.pct_change()
        simulate_pct_df.fillna(0, inplace=True)
        # print("simulate_pct_df")
        # print(simulate_pct_df.head())
        simulate_pct_df_dic[wind_code] = simulate_pct_df * weight
        # weighted_dic[wind_code] = weighted
    # time_1 = time.time()
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
    fig = matplotlib.pyplot.gcf()
    fig.set_size_inches(15.5, 10.5)
    plt.savefig(file_path, dpi=100)
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
    fof_file_name = 'fof_%s.png' % (figure_time.strftime('%Y_%m_%d_%H_%M_%S'))
    fof_file_path = savefig_df(simulate_comp_df, fof_file_name)
    fund_file_path_dic['fof'] = fof_file_path
    return fund_file_path_dic


if __name__ == '__main__':
    # wind_code = 'XT1410445.XT'
    # simulate_df = cal_fhs_garch(wind_code, 10)
    # print(simulate_df.head())
    # simulate_df.plot(legend=False)
    # plt.show()
    # wind_code_list = ['XT144787.XT', 'XT146247.XT', 'XT147219.XT']
    # wind_code_list = ['XT1507116.XT', 'XT1502707.XT', 'XT1512218.XT', 'XT1510534.XT', 'XT1522002.XT', 'XT144816.XT']
    wind_code_list = ['XT1614831.XT', 'XT1520429.XT', 'XT145733.XT', 'ZG130482.OF', 'ZG130483.OF']
    simulate_count = 10
    fund_file_path_dic = plot_fof_fhs_garch(wind_code_list, simulate_count)
    print(fund_file_path_dic)
