# -*- coding:utf-8 -*-
"""
Created on 2016-12-22
@author: MG
"""
from mpl_toolkits.mplot3d import axes3d
import os
from datetime import datetime, date
import pytz
import matplotlib.pyplot as plt
import numpy as np
# from pandas.tslib import Timestamp
from pandas import Timestamp
import re
import pandas as pd
from collections import OrderedDict
import logging
import warnings

STR_FORMAT_DATE = '%Y-%m-%d'
PATTERN_DATE_FORMAT_RESTRICT = re.compile(r"\d{4}(\D)*\d{2}(\D)*\d{2}")
PATTERN_DATE_FORMAT = re.compile(r"\d{4}(\D)*\d{1,2}(\D)*\d{1,2}")


def date_2_str(dt):
    if dt is not None and type(dt) in (date, datetime):
        dt_str = dt.strftime(STR_FORMAT_DATE)
    else:
        dt_str = dt
    return dt_str


def str_2_bytes(input_str):
        """
        用于将 str 类型转换为 bytes 类型
        :param input_str: 
        :return: 
        """
        return input_str.encode(encoding='GBK')


def bytes_2_str(bytes_str):
    """
    用于将bytes 类型转换为 str 类型
    :param bytes_str: 
    :return: 
    """
    return str(bytes_str, encoding='GBK')


def pattern_data_format(data_str):
    """
    识别日期格式（例如：2017-12-23），并将其翻译成 %Y-%m-%d 类似的格式
    :param data_str: 
    :return: 
    """
    date_str_format = PATTERN_DATE_FORMAT_RESTRICT.sub(r'%Y\1%m\2%d', data_str)
    if date_str_format == data_str:
        date_str_format = PATTERN_DATE_FORMAT.sub(r'%Y\1%m\2%d', data_str)
    return date_str_format


def try_2_date(something):
    """
    兼容各种格式尝试将 未知对象转换为 date 类型，相对比 str_2_date 消耗资源，支持更多的类型检查，字符串格式匹配
    :param something: 
    :return: 
    """
    if something is None:
        date_ret = something
    else:
        something_type = type(something)
        if something_type in (int, np.int64, np.int32, np.int16, np.int8):
            something = str(something)
            something_type = type(something)
        if type(something) == str:
            date_str_format = pattern_data_format(something)
            date_ret = datetime.strptime(something, date_str_format).date()
        elif type(something) in (Timestamp, datetime):
            date_ret = something.date()
        else:
            date_ret = something
    return date_ret


def df_plot(df):
    fig, ax = plt.subplots(subplot_kw={'projection': '3d'})
    x1 = np.array(df.index)
    y1 = np.array(df.columns)
    xx = np.tile(x1, (len(y1), 1))
    yy = np.tile(y1, (len(x1), 1)).T
    zz = df.T.as_matrix()
    print(xx.shape, yy.shape, zz.shape)
    ax.plot_wireframe(xx, yy, zz, rstride=10, cstride=0)
    plt.tight_layout()
    plt.show()


def get_first(iterable, func):
    for n in iterable:
        if func(n):
            return n
    return None


def get_last(iterable, func):
    count = len(iterable)
    for n in range(count - 1, -1, -1):
        if func(iterable[n]):
            return iterable[n]
    return None


def replace_none_2_str(string, replace=''):
    return replace if string is None else string


def str_2_date(date_str, date_str_format=STR_FORMAT_DATE):
    """
    将日期字符串转换成 date 类型对象，如果字符串为 None 则返回None
    :param date_str: 日期字符串
    :param date_str_format: 日期字符串格式
    :return: 
    """
    if date_str is not None:
        if type(date_str) == str:
            date_ret = datetime.strptime(date_str, date_str_format).date()
        elif type(date_str) in (Timestamp, datetime):
            date_ret = date_str.date()
        else:
            date_ret = date_str
    else:
        date_ret = date_str
    return date_ret


def date2datetime(dt):
    """
    date 类型转换问 datetime类型
    :param dt: 
    :return: 
    """
    return datetime(dt.year, dt.month, dt.day)


def clean_datetime_remove_time_data(atime):
    """
    将时间对象的 时、分、秒 全部清零
    :param atime: 
    :return: 
    """
    return datetime(atime.year, atime.month, atime.day)


def clean_datetime_remove_ms(atime):
    """
    将时间对象的 毫秒 全部清零
    :param atime: 
    :return: 
    """
    return datetime(atime.year, atime.month, atime.day, atime.hour, atime.minute, atime.second)


def utc2local(utc):
    localtime = datetime.utcfromtimestamp(utc).replace(tzinfo=pytz.utc).astimezone(pytz.timezone('Asia/Shanghai'))
    return localtime


def get_cntr_kind_name(contract_name):
    left_idx = len(contract_name) - 1
    for num_str in '1234567890':
        idx = contract_name.find(num_str, 0, left_idx)
        if idx == -1:
            continue
        if idx < left_idx:
            left_idx = idx
        if left_idx <= 1:
            break
    # print(lidx, contractname[:lidx])
    return contract_name[:left_idx]


CACHE_FOLDER_PATH_DIC = {}


def get_folder_path(target_folder_name=None, create_if_not_found=True):
    """
    获得系统缓存目录路径
    :param target_folder_name: 缓存目录名称
    :param create_if_not_found: 如果不存在则创建一个目录，默认：True
    :return: 缓存路径
    """
    global CACHE_FOLDER_PATH_DIC
    if target_folder_name is None:
        target_folder_name = 'cache'
    if target_folder_name not in CACHE_FOLDER_PATH_DIC:
        cache_folder_path_tmp = None
        print(u'查找数据目录path:', end="")
        parent_folder_path = os.path.abspath(os.curdir)
        par_path = parent_folder_path
        while not os.path.ismount(par_path):
            # print 'parent path = %s'%par_path
            dir_list = os.listdir(par_path)
            for dir_name in dir_list:
                # print d # .strip()
                if dir_name == target_folder_name:
                    cache_folder_path_tmp = os.path.join(par_path, dir_name)
                    print('<', cache_folder_path_tmp, '>')
                    break
            if cache_folder_path_tmp is not None:
                break
            par_path = os.path.abspath(os.path.join(par_path, os.path.pardir))
        if cache_folder_path_tmp is None:
            if create_if_not_found:
                cache_folder_path_tmp = os.path.abspath(os.path.join(parent_folder_path, target_folder_name))
                print('<', cache_folder_path_tmp, '> 创建缓存目录')
                os.makedirs(cache_folder_path_tmp)
                CACHE_FOLDER_PATH_DIC[target_folder_name] = cache_folder_path_tmp
        else:
            CACHE_FOLDER_PATH_DIC[target_folder_name] = cache_folder_path_tmp
    return CACHE_FOLDER_PATH_DIC.setdefault(target_folder_name, None)


def get_cache_file_path(cache_folder_name, file_name, create_if_not_found=True):
    """
    返回缓存文件的路径
    :param file_name: 缓存文件名称
    :param cache_folder_name: 缓存folder名称
    :param create_if_not_found: 如果不存在则创建一个目录，默认：True
    :return: 缓存文件路径
    """
    cache_folder_path = get_folder_path(cache_folder_name, create_if_not_found)
    return os.path.join(cache_folder_path, file_name)


def get_df_between_date(data_df, date_frm, date_to):
    """
    该函数仅用于 return_risk_analysis 中计算使用
    :param data_df: 
    :param date_frm: 
    :param date_to: 
    :return: 
    """
    if date_frm is not None and date_to is not None:
        new_data_df = data_df[(data_df.Date >= date_frm) & (data_df.Date <= date_to)]
    elif date_frm is not None:
        new_data_df = data_df[data_df.Date >= date_frm]
    elif date_to is not None:
        new_data_df = data_df[data_df.Date <= date_to]
    else:
        new_data_df = data_df
    new_data_df = new_data_df.reset_index(drop=True)
    return new_data_df


def return_risk_analysis(nav_df, date_frm=None, date_to=None, freq='weekly', rf=0.02):
    """
    按列统计 rr_df 收益率绩效 
    :param nav_df: 收益率DataFrame，index为日期，每一列为一个产品的净值走势
    :param date_frm: 统计日期区间，可以为空
    :param date_to: 统计日期区间，可以为空
    :param freq: None 自动识别, 'daily' 'weekly' 'monthly'
    :param rf: 无风险收益率，默认 0.02
    :return: 
    """
    nav_df.index = [try_2_date(idx) for idx in nav_df.index]
    nav_sorted_df = nav_df.sort_index()
    rr_df = (1 + nav_sorted_df.pct_change().fillna(0)).cumprod()
    rr_df.index = [try_2_date(d) for d in rr_df.index]
    # 计算数据实际频率是日频、周频、月頻
    rr_df_len = rr_df.shape[0]
    day_per_data = (rr_df.index[rr_df_len - 1] - rr_df.index[0]).days / rr_df_len
    if day_per_data <= 2:
        freq_real = 'daily'
    elif day_per_data <= 10:
        freq_real = 'weekly'
    else:
        freq_real = 'monthly'
    if freq is None:
        freq = freq_real
    elif freq != freq_real:
        warnings_msg = "data freq wrong, expect %s, but %s was detected" % (freq, freq_real)
        # warnings.warn(warnings_msg)
        # logging.warning(warnings_msg)
        raise ValueError(warnings_msg)

    freq_str = ''
    if freq == 'weekly':
        data_count_per_year = 50
        freq_str = '周'
    elif freq == 'monthly':
        data_count_per_year = 12
        freq_str = '月'
    elif freq == 'daily':
        data_count_per_year = 250
        freq_str = '日'
    else:
        raise ValueError('freq=%s 只接受 daily weekly monthly 三种之一', freq)
    stat_dic_dic = OrderedDict()
    # rr_df.index = [str_2_date(d) for d in rr_df.index]
    rr_uindex_df = rr_df.reset_index()
    col_name_list = list(rr_uindex_df.columns)
    date_col_name = col_name_list[0]
    col_name_list = col_name_list[1:]
    if type(date_frm) is str:
        date_frm = datetime.strptime(date_frm, '%Y-%m-%d').date()
    if type(date_to) is str:
        date_to = datetime.strptime(date_to, '%Y-%m-%d').date()
    for col_name in col_name_list:
        data_df = rr_uindex_df[[date_col_name, col_name]]
        # print(data_df)
        data_df.columns = ['Date', 'Value']
        data_df = get_df_between_date(data_df, date_frm, date_to)
        data_df.Value = data_df.Value / data_df.Value[0]
        data_df['ret'] = data_df.Value.pct_change().fillna(0)
        date_span = data_df.Date[data_df.index[-1]] - data_df.Date[data_df.index[0]]
        date_span_fraction = 365 / date_span.days
        # basic indicators
        CAGR = data_df.Value[data_df.index[-1]] ** date_span_fraction - 1
        period_rr = data_df.Value[data_df.index[-1]] - 1
        ann_vol = np.std(data_df.ret, ddof=1) * np.sqrt(data_count_per_year)
        down_side_vol = np.std(data_df.ret[data_df.ret < 0], ddof=1) * np.sqrt(data_count_per_year)
        # WeeksNum = data.shape[0]
        profit_loss_ratio = -np.mean(data_df.ret[data_df.ret > 0]) / np.mean(data_df.ret[data_df.ret < 0])
        win_ratio = len(data_df.ret[data_df.ret >= 0]) / len(data_df.ret)
        min_value = min(data_df.Value)
        final_value = data_df.Value[data_df.index[-1]]
        max_ret = max(data_df.ret)
        min_ret = min(data_df.ret)
        # End of basic indicators
        # max dropdown related
        data_df['mdd'] = data_df.Value / data_df.Value.cummax() - 1
        mdd_size = min(data_df.mdd)
        droparray = pd.Series(data_df.index[data_df.mdd == 0])
        if len(droparray) == 1:
            mdd_max_period = len(data_df.mdd)
        else:
            if float(data_df.Value[droparray.tail(1)]) > float(data_df.Value.tail(1)):
                droparray = droparray.append(pd.Series(data_df.index[-1]), ignore_index=True)
            mdd_max_period = max(droparray.diff().dropna()) - 1
        # End of max dropdown related
        # High level indicators
        sharpe_ratio = (CAGR - rf) / ann_vol
        sortino_ratio = (CAGR - rf) / down_side_vol
        calmar_ratio = CAGR / (-mdd_size)
        #  Natural month return
        j = 1
        for i in data_df.index:
            if i == 0:
                month_ret = pd.DataFrame([[data_df.Date[i], data_df.Value[i]]], columns=('Date', 'Value'))
            else:
                if data_df.Date[i].month != data_df.Date[i - 1].month:
                    month_ret.loc[j] = [data_df.Date[i - 1], data_df.Value[i - 1]]
                    j += 1
        month_ret.loc[j] = [data_df.Date[data_df.index[-1]], data_df.Value[data_df.index[-1]]]
        month_ret['ret'] = month_ret.Value.pct_change().fillna(0)
        max_rr_month = max(month_ret.ret)
        min_rr_month = min(month_ret.ret)
        # End of Natural month return
        data_len = data_df.shape[0]
        date_begin = data_df.Date[0]  # .date()
        date_end = data_df.Date[data_len-1]
        stat_dic = OrderedDict([('起始日期', date_begin),
                                ('截止日期', date_end),
                                ('区间收益率', '%.2f%%' % (period_rr * 100)),
                                ('最终净值', '%.4f' % final_value),
                                ('最低净值', '%.4f' % min_value),
                                ('年化收益率', '%.2f%%' % (CAGR * 100)),
                                ('年化波动率', '%.2f%%' % (ann_vol * 100)),
                                ('年化下行波动率', '%.2f%%' % (down_side_vol * 100)),
                                ('最大回撤', '%.2f%%' % (mdd_size * 100)),
                                ('夏普率', '%.2f' % sharpe_ratio),
                                ('索提诺比率', '%.2f' % sortino_ratio),
                                ('卡马比率', '%.2f' % calmar_ratio),
                                ('盈亏比', '%.2f' % profit_loss_ratio),
                                ('胜率', '%.2f' % win_ratio),
                                ('最长不创新高（%s）' % freq_str, mdd_max_period),
                                ('统计周期最大收益', '%.2f%%' % (max_ret * 100)),
                                ('统计周期最大亏损', '%.2f%%' % (min_ret * 100)),
                                ('最大月收益', '%.2f%%' % (max_rr_month * 100)),
                                ('最大月亏损', '%.2f%%' % (min_rr_month * 100))])
        stat_dic_dic[col_name] = stat_dic
    stat_df = pd.DataFrame(stat_dic_dic)
    stat_df = stat_df.ix[list(stat_dic.keys())]
    return stat_df


class DataFrame(pd.DataFrame):
    def interpolate_inner(self, columns=None, inplace=False):
        if columns is None:
            columns = list(self.columns)
        data = self if inplace else self.copy()
        for col_name in columns:
            index_not_nan = data.index[~np.isnan(data[col_name])]
            if index_not_nan.shape[0] == 0:
                continue
            index_range = (min(index_not_nan), max(index_not_nan))
            # data[col_name][index_range[0]:index_range[1]].interpolate(inplace=True)
            data[col_name][index_range[0]:index_range[1]] = data[col_name][index_range[0]:index_range[1]].interpolate()
        # print(data)
        if ~inplace:
            return data

    def map(self, func):
        row_count, col_count = self.shape
        columns = list(self.columns)
        indexes = list(self.index)
        for col_num in range(col_count):
            col_val = columns[col_num]
            for row_num in range(row_count):
                row_val = indexes[row_num]
                data_val = self.iloc[row_num, col_num]
                self.iloc[row_num, col_num] = func(col_val, row_val, data_val)
        return self


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s: %(levelname)s [%(name)s] %(message)s')
    file_path = r'd:\Works\F复华投资\L路演、访谈、评估报告\万霁\万霁资管一号.xlsx'
    file_path_no_extention, _ = os.path.splitext(file_path)
    data_df = pd.read_excel(file_path, index_col=0)
    stat_df = return_risk_analysis(data_df, freq=None)  # , freq='daily'
    print(stat_df)
    stat_df.to_csv(file_path_no_extention + '绩效统计.csv')

    # data_df = pd.DataFrame({'a': np.arange(1, 6),
    #               'b': np.arange(2, 7),
    #               'c': np.arange(3, 8),
    #               })
    # ret_df = DataFrame.map(data_df, lambda col_val, row_vol, data_vol: data_vol % 2 == 0 if col_val == 'a' else data_vol % 2 != 0)
    # print(ret_df)