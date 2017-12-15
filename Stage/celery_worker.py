# -*- coding: utf-8 -*-
"""
:copyright: (c) 2016 by Huang Dong 
:mail: bjdhuang@cn.ibm.com.
:license: Apache 2.0, see LICENSE for more details.
"""
from celery import Celery
from celery.schedules import crontab
import os
from fof_app import create_app
from fof_app.tasks import log,update_fund,update_index,update_stock,stress_testing,update_future_daily,update_future_info
from config_fh import get_redis
from r_log import RedisHandler
import logging
logger = logging.getLogger()
r_handler = RedisHandler(channel='stream',redis_client=get_redis(host='10.0.5.107',db=5))
logger.addHandler(r_handler)


def create_celery(app):
    celery = Celery(app.import_name,
                    backend=app.config['CELERY_RESULT_BACKEND'],
                    broker=app.config['CELERY_BROKER_URL'])
    celery.conf.update(app.config)
    TaskBase = celery.Task
    class ContextTask(TaskBase):
        abstract = True
        def __call__(self, *args, **kwargs):
            with app.app_context():
                return TaskBase.__call__(self, *args, **kwargs)
    celery.Task = ContextTask
    return celery

env = os.environ.get('APP_ENV','dev')
flask_app = create_app('fof_app.config.%sConfig' % env.capitalize())
celery = create_celery(flask_app)
flask_app.app_context().push()


@celery.on_after_configure.connect
def setup_periodic_tasks(sender, **kwargs):
    sender.add_periodic_task(
        crontab(hour=17,minute=0),
        update_index
    )
    sender.add_periodic_task(
        crontab(hour=16, minute=30),
        update_stock
    )
    sender.add_periodic_task(
        crontab(hour=23, minute=0),
        update_fund
    )
    sender.add_periodic_task(
        crontab(hour=12, minute=0,day_of_week=6),
        stress_testing
    )
    sender.add_periodic_task(
        crontab(hour=19, minute=0,day_of_week=5),
        update_future_info
    )
    sender.add_periodic_task(
        crontab(hour=19, minute=0),
        update_future_daily
    )

celery.log.redirect_stdouts_to_logger(logger)


