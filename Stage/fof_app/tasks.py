# -*- coding: utf-8 -*-
"""
:copyright: (c) 2016 by Huang Dong
:mail: bjdhuang@cn.ibm.com.
:license: Apache 2.0, see LICENSE for more details.
"""
from .extensions import mail
from stress_testing import copula_fof, fhs_garch_fund
from celery import signals
import celery
from celery import Task
from flask import current_app, render_template
from flask_mail import Message
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
from periodic_task.wind_future import import_wind_future_daily, import_wind_future_info
from periodic_task.wind_pub_fund import import_pub_fund_info, import_pub_fund_daily
from periodic_task.wind_smfund_daily import import_smfund_daily
from periodic_task.wind_stock_hk import import_stock_quertarly_hk, import_stock_daily_hk, import_wind_stock_info_hk


logger = logging.getLogger()

daily_mid_night_task_group = OrderedDict([
    ('fund nav', {'func': update_wind_fund_nav, 'params': []}),
    ('strategy index val', {'func': do_update_strategy_index, 'params': []}),
    ('sm fund daily', {'func': import_smfund_daily, 'params': []}),
])

daily_task_group = OrderedDict([
    ('stock info', {'func': import_wind_stock_info, 'params': []}),
    ('stock daily', {'func': import_stock_daily, 'params': []}),
    ('stock daily wch', {'func': import_stock_daily_wch, 'params': []}),
    ('factor exposure', {'func': update_factors, 'params': []}),
    ('convertible bond daily', {'func': import_cb_daily, 'params': []}),
    ('futurn daily', {'func': import_wind_future_daily, 'params': []}),
    ('index daily', {'func': import_wind_index_daily, 'params': []}),
])

daily_night_task_group = OrderedDict([
    ('pub fund daily', {'func': import_pub_fund_daily, 'params': []}),
    ('stock info hk', {'func': import_wind_stock_info_hk, 'params': []}),
    ('stock daily hk', {'func': import_stock_daily_hk, 'params': []}),
])

stress_testing_group = OrderedDict([
    ('fhs-garch', {'func': do_fhs_garch, 'params': []}),
    ('copula', {'func': do_copula, 'params': []}),
    ('multi_factor', {'func': do_fund_multi_factor, 'params': []})
])

weekly_task_group = OrderedDict([
    ('pub fund info', {'func': import_pub_fund_info, 'params': []}),
    ('futurn info', {'func': import_wind_future_info, 'params': []}),
    ('fund info', {'func': update_wind_fund_info, 'params': [], }),
    ('convertible bond info', {'func': import_cb_info, 'params': []}),
    ('stock quertarly', {'func': import_stock_quertarly, 'params': []}),
    ('stock quertarly hk', {'func': import_wind_stock_info_hk, 'params': []}),
])


# noinspection PyBroadException
def do_task(task_dict, break_if_exception=True):
    success_task = []
    failure_task = []
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
            success_task.append(task_name)
        except Exception as exp:
            logger.exception('--> %s task exception 该组任务终止:' % task_name)
            failure_task.append((task_name, exp))
            if break_if_exception:
                break
    logger.info('all task finished')
    logger.info('%s task success', success_task)
    for task_name, exp in failure_task:
        logger.error('%s task failure exception info:%s', task_name, exp)


@signals.setup_logging.connect
def on_celery_setup_logging(**kwargs):
    pass


class MyTask(Task):
    def on_success(self, retval, task_id, args, kwargs):
        user = kwargs.get('user', None)
        if user is not None:
            send_email(user, '压力测试已完成', 'email/testing', user=user)
        return super(MyTask, self).on_success(retval, task_id, args, kwargs)

    def on_failure(self, exc, task_id, args, kwargs, einfo):
        admin = current_app.config['CELERY_ADMIN']
        message = 'task fail, reason: {0}'.format(exc)
        send_email(admin, "任务失败", 'email/task_failure', message=message, name=self.name)
        user = kwargs.get('user', None)
        if user is not None:
            send_email(user, '任务失败', 'email/task_failure', user=user, message=message, name=self.name)
        return super(MyTask, self).on_failure(exc, task_id, args, kwargs, einfo)


@celery.task(name='daily_mid_night_task', base=MyTask)
def daily_mid_night_task():
    do_task(daily_mid_night_task_group, break_if_exception=False)
    return {'status': 'success'}


@celery.task(name='stress_testing', base=MyTask)
def stress_testing():
    do_task(stress_testing_group)
    return {'status': 'success'}


@celery.task(name='daily_task', base=MyTask)
def daily_task():
    do_task(daily_task_group, break_if_exception=False)
    return {'status': 'success'}


@celery.task(name='daily_night_task', base=MyTask)
def daily_night_task():
    do_task(daily_night_task_group, break_if_exception=False)
    return {'status': 'success'}


@celery.task(name='weekly_task', base=MyTask)
def weekly_task():
    do_task(weekly_task_group, break_if_exception=False)
    return {'status': 'success'}


@celery.task(name="testing", base=MyTask)
def run_scheme_testing(sid, user):
    copula_fof.do_copula_4_scheme(sid)
    fhs_garch_fund.do_fhs_garch_4_scheme(sid)
    return {'status': 'success'}


@celery.task(serializer='pickle')
def send_async_email(msg):
    mail.send(msg)


def send_email(to, subject, template, **kwargs):
    app = current_app._get_current_object()
    msg = Message(app.config['FOF_MAIL_SUBJECT_PREFIX'] + ' ' + subject,
                  sender=app.config['FOF_MAIL_SENDER'], recipients=[to])
    msg.body = render_template(template + '.txt', **kwargs)
    msg.html = render_template(template + '.html', **kwargs)
    send_async_email.delay(msg)


@celery.task(name='test_log', base=MyTask)
def log(message):
    print("testing log 123")

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s: %(levelname)s [%(name)s] %(message)s')
    do_task(daily_task_group)
