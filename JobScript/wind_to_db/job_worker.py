from wind_fund_info_update import update_wind_fund_info
from wind_fund_nav_update import update_wind_fund_nav
from wind_index_daily_import_update import import_wind_index_daily
from wind_stock_info_import import import_wind_stock_info
from wind_stock_daily_import import import_stock_daily
from factor_profit import update_factors
from logging.handlers import RotatingFileHandler
import logging
from collections import OrderedDict

formatter = logging.Formatter('[%(asctime)s:  %(levelname)s  %(name)s %(message)s')
file_handle = RotatingFileHandler("app.log", mode='a', maxBytes=10 * 1024 * 1024, backupCount=3, encoding=None, delay=0)
file_handle.setFormatter(formatter)
console_handle = logging.StreamHandler()
console_handle.setFormatter(formatter)
loger = logging.getLogger()
loger.setLevel(logging.INFO)
loger.addHandler(file_handle)
loger.addHandler(console_handle)
STR_FORMAT_DATETIME = '%Y-%m-%d'
TASK_GROUP = {
    'fund group': {
        'enable': False,
        'task dic': OrderedDict([
            ('fund info', {'func': update_wind_fund_info, 'params': [], 'enable': True}),
            ('fund nav', {'func': update_wind_fund_nav, 'params': [], 'enable': True}),
        ]),
             },
    'stock group': {
        'enable': False,
        'task dic': OrderedDict([
            ('stock info', {'func': import_wind_stock_info, 'params': [], 'enable': True}),
            ('stock daily', {'func': import_stock_daily, 'params': [], 'enable': True}),
            ('factor exposure', {'func': update_factors, 'params': [], 'enable': True}),
        ])
    },
    'index group': {
        'enable': True,
        'task dic': OrderedDict([
            ('index daily', {'func': import_wind_index_daily, 'params': [], 'enable': True}),
        ])
    },
    'stress testing group': {
        'enable': False,
        'task dic': OrderedDict([
            ('fhs-garch', {'func': import_wind_stock_info, 'params': [], 'enable': True}),
            ('copula', {'func': import_stock_daily, 'params': [], 'enable': True}),
        ])
    },
}


def do_task(task_group_dic):
    for group_name, task_group in task_group_dic.items():
        group_enable = task_group.setdefault('enable', True)
        if not group_enable:
            continue
        task_dic = task_group['task dic']
        for task_name, task in task_dic.items():
            task_enable = task.setdefault('enable', True)
            if not task_enable:
                continue
            task_func = task['func']
            params = task.setdefault('params', [])
            try:
                if len(params) == 0:
                    task_func()
                else:
                    task_func(*params)
                logging.info('%s --> %s task finished:', group_name, task_name)
            except:
                logging.exception('%s --> %s task exception 该组任务终止:', group_name, task_name)
                break
    logging.info('all task finished')




if __name__ == '__main__':
    do_task(TASK_GROUP)
