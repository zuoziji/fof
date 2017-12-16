# -*- coding: utf-8 -*-
"""
:copyright: (c) 2016 by Huang Dong
:mail: bjdhuang@cn.ibm.com.
:license: Apache 2.0, see LICENSE for more details.
"""
# from backend.job_worker import do_task, fund_group, stress_testing_group, stock_group, index_group, future_daily_group, \
#     future_info_weekly_group
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
from periodic_task.wind_future_daily import import_wind_future_daily
from periodic_task.wind_future_info import import_wind_future_info
from periodic_task.wind_pub_fund import import_pub_fund_info, import_pub_fund_daily


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
    ('convertible bond daily', {'func': import_cb_daily, 'params': []}),
    ('stock quertarly', {'func': import_stock_quertarly, 'params': []}),
    ('pub fund daily', {'func': import_pub_fund_daily, 'params': []}),
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
    ('pub fund info', {'func': import_pub_fund_info, 'params': []}),
    ('futurn info', {'func': import_wind_future_info, 'params': []}),
    ('convertible bond info', {'func': import_cb_info, 'params': []}),
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


@celery.task(name='fund', base=MyTask)
def update_fund():
    do_task(fund_group)
    return {'status': 'success'}


@celery.task(name='stress_testing', base=MyTask)
def stress_testing():
    do_task(stress_testing_group)
    return {'status': 'success'}


@celery.task(name='index', base=MyTask)
def update_index():
    do_task(index_group)
    return {'status': 'success'}


@celery.task(name='stock', base=MyTask)
def update_stock():
    do_task(stock_group)
    return {'status': 'success'}


@celery.task(name='future_info', base=MyTask)
def update_future_info():
    do_task(future_info_weekly_group)
    return {'status': 'success'}


@celery.task(name='future_daily', base=MyTask)
def update_future_daily():
    do_task(future_daily_group)
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
    """Print some log messages"""

    print(123123123123)
    # #send_email("kuangmsn@163.com",'压力测试已完成','email/testing',user="kuangmsn@163.com")
    #   return {"status":"success"}

# # @celery.task
# # def reverse_messages():
# #     """Reverse all messages in DB"""
# #     for message in Message.query.all():
# #         words = message.text.split()
# #         message.text = " ".join(reversed(words))
# #         db.session.commit()
#

if __name__ == '__main__':
    do_task(stress_testing_group)
