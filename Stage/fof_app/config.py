# -*- coding: utf-8 -*-
"""
:copyright: (c) 2016 by Huang Dong 
:mail: bjdhuang@cn.ibm.com.
:license: Apache 2.0, see LICENSE for more details.
"""
import os
basedir = os.path.abspath(os.path.dirname(__file__))
import logging
from logging.config import  dictConfig
logger = logging.getLogger()
class Config():

    CSRF_ENABLED = True
    SECRET_KEY = 'afc365faebcd8aa504c890d00eea62c1'
    BABEL_DEFAULT_LOCALE = 'zh_CN'
    UPLOADS = os.path.join(basedir, "uploads")
    ACC_FOLDER = os.path.join(basedir,"acc")
    CORP_FOLDER = os.path.join(basedir,"corp")
    WHOOSH_BASE = os.path.join(basedir, "WHOOSH")
    CELERY_BROKER_URL = 'redis://localhost:6379/0'
    CELERY_RESULT_BACKEND = 'redis://localhost:6379/0'
    CELERY_TASK_SERIALIZER = 'json'
    CELERY_ACCEPT_CONTENT = ['json', 'pickle']
    CELERY_ADMIN = 'kuangmsn@163.com'
    SOCKETIO_MESSAGE_QUEUE = 'redis://localhost:6379/6'
    CELERY_ENABLE_UTC = True
    CELERY_TIMEZONE = "Asia/Shanghai"
    C_FORCE_ROOT = True
    DEBUG = True
    SQLALCHEMY_TRACK_MODIFICATIONS = False
    SQLALCHEMY_POOL_RECYCLE = 60
    APP_SLOW_DB_QUERY_TIME = 1
    MAIL_SERVER = 'smtp.263.net'
    MAIL_PORT = 25
    MAIL_USERNAME = os.environ.get('MAIL_USERNAME', "FOF_SYS@foriseinvest.com")
    MAIL_PASSWORD = os.environ.get('MAIL_PASSWORD', 'Abcd1234')
    FOF_MAIL_SUBJECT_PREFIX = '[FOF]'
    FOF_MAIL_SENDER = 'FOF Admin <FOF_SYS@foriseinvest.com>'
    FOF_ADMIN = os.environ.get('FOF_ADMIN','FOF_SYS@foriseinvest.com')
    REQUEST_LIMIT = ['details','change_acc','calendar','fof_upload']
    CACHE_DB_ID = 3
    CACHE_TIME = 0
    logging_config = dict(
        version=1,
        formatters={
            'simple': {
                'format': '%(levelname)s %(asctime)s { Module : %(module)s Line No : %(lineno)d} %(message)s'}
        },
        handlers={
            'file_handler': {'class': 'logging.handlers.RotatingFileHandler',
                  'filename': 'logger.log',
                  'maxBytes': 1024 * 1024 * 10,
                  'backupCount': 5,
                  'level': 'DEBUG',
                  'formatter': 'simple',
                  'encoding': 'utf8'},
            'console_handler':{'class':'logging.StreamHandler',
                               'level':'DEBUG',
                               'formatter':'simple'}
        },

        root={
            'handlers': ['console_handler','file_handler'],
            'level': logging.DEBUG,
        }
    )
    logging.getLogger('sqlalchemy.engine').setLevel(logging.WARN)
    dictConfig(logging_config)


    @staticmethod
    def init_app(app):
        pass


class ProdConfig(Config):
    SQLALCHEMY_DATABASE_URI = 'mysql://mg:Abcd1234@10.0.5.111/fof_ams_db'
    CACHE_DB = '10.0.5.107'
    @classmethod
    def init_app(cls, app):
        Config.init_app(app)




class DevConfig(Config):
    SQLALCHEMY_DATABASE_URI = 'mysql://mg:Abcd1234@10.0.3.66/fof_ams_dev'
    CACHE_DB = '127.0.0.1'
    @classmethod
    def init_app(cls, app):
        Config.init_app(app)




class TestingConfig(Config):
    SQLALCHEMY_DATABASE_URI = 'mysql://mg:Abcd1234@10.0.3.66/fof_ams_dev'
    CACHE_DB = '127.0.0.1'
    CSRF_ENABLED = False
    WTF_CSRF_ENABLED = False
    TESTING = True
    @classmethod
    def init_app(cls, app):
        Config.init_app(app)








