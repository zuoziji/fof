# -*- coding: utf-8 -*-
"""
 （废弃）
 此文件内容已经并入 tasks.py 仅备份使用，稍后删除 
"""

import logging
from collections import OrderedDict

from periodic_task.factor_profit import update_factors
from periodic_task.wind_fund_info_update import update_wind_fund_info
from periodic_task.wind_index_daily_import_update import import_wind_index_daily
from periodic_task.wind_stock_daily_import import import_stock_daily, import_stock_daily_wch
from periodic_task.wind_stock_info_import import import_wind_stock_info
from periodic_task.wind_stock_quarterly import import_stock_quertarly
from periodic_task.wind_convertible_bond import import_cb_info, import_cb_daily
from stress_testing.copula_fof import do_copula
from stress_testing.fhs_garch_fund import do_fhs_garch
from periodic_task.fund_multi_factor_exp import do_fund_multi_factor
from periodic_task.build_strategy_index import do_update_strategy_index
from periodic_task.wind_fund_nav_update import update_wind_fund_nav
from periodic_task.wind_future_daily import import_wind_future_daily
from periodic_task.wind_future_info import import_wind_future_info


logger = logging.getLogger()

fund_group = OrderedDict([
    ('fund info', {'func': update_wind_fund_info, 'params': [], }),
    ('fund nav', {'func': update_wind_fund_nav, 'params': []}),
    ('strategy index val', {'func': do_update_strategy_index, 'params': []}),
])
stock_group = OrderedDict([
    ('stock info', {'func': import_wind_stock_info, 'params': []}),
    ('stock daily', {'func': import_stock_daily, 'params': []}),
    ('stock daily wch', {'func': import_stock_daily_wch, 'params': []}),
    ('factor exposure', {'func': update_factors, 'params': []}),
    ('convertible bond info', {'func': import_cb_info, 'params': []}),
    ('convertible bond daily', {'func': import_cb_daily, 'params': []}),
    ('stock quertarly', {'func': import_stock_quertarly, 'params': []}),
])
index_group = OrderedDict([
    ('index daily', {'func': import_wind_index_daily, 'params': []})
])

stress_testing_group = OrderedDict([
    ('fhs-garch', {'func': do_fhs_garch, 'params': []}),
    ('copula', {'func': do_copula, 'params': []}),
    ('multi_factor', {'func': do_fund_multi_factor, 'params': []})
])

future_daily_group = OrderedDict([
    ('futurn daily', {'func': import_wind_future_daily, 'params': []}),
])

future_info_weekly_group = OrderedDict([
    ('futurn info', {'func': import_wind_future_info, 'params': []}),
])


# noinspection PyBroadException
def do_task(task_dict):
    for task_name, task in task_dict.items():
        task_func = task['func']
        params = task.setdefault('params', [])
        # noinspection PyBroadException
        try:
            if len(params) == 0:
                task_func()
            else:
                task_func(*params)
            logger.info('--> %s task finished:' % task_name)
        except:
            logger.exception('--> %s task exception 该组任务终止:' % task_name)
            break
    logger.info('all task finished')


if __name__ == '__main__':
    do_task(stress_testing_group)
