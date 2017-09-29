# -*- coding: utf-8 -*-
"""
:copyright: (c) 2016 by Huang Dong
:mail: bjdhuang@cn.ibm.com.
:license: Apache 2.0, see LICENSE for more details.
"""
from backend.job_worker import do_task, fund_group, stress_testing_group, stock_group, index_group, future_daily_group, \
    future_info_weekly_group
from .extensions import mail
from stress_testing import copula_fof, fhs_garch_fund
from celery import signals
import celery
from celery import Task
from flask import current_app, render_template
from flask_mail import Message


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
