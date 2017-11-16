# -*- coding: utf-8 -*-
"""
:copyright: (c) 2016 by Huang Dong 
:mail: bjdhuang@cn.ibm.com.
:license: Apache 2.0, see LICENSE for more details.
"""

from functools import wraps
from flask import abort,request,flash,redirect,url_for
from flask_login import LoginManager, current_user
from flask_admin import Admin
from flask_babelex import Babel
from flask_celery import Celery
from flask_mail import Mail
from threading import Thread
from flask import current_app, render_template
from flask_mail import Message
from flask_socketio import SocketIO
import logging
from werkzeug.contrib.cache import RedisCache

logger = logging.getLogger()

celery = Celery()
login_manager = LoginManager()
admin = Admin(name='FOF ADMIN ')
babel = Babel()
mail = Mail()
SocketIo = SocketIO()
login_manager.login_view = 'auth.login'
login_manager.session_protection = "strong"
login_manager.login_message_category = "info"
login_manager.login_message = ''
cache = RedisCache(host="127.0.0.1", db=3,default_timeout=0)
permissions = list()


@login_manager.user_loader
def load_user(user_id):
    from .models import UserModel
    return UserModel.query.filter_by(id=user_id).first()


def send_async_email(app, msg):
    with app.app_context():
        mail.send(msg)


def send_email(to, subject, template, **kwargs):
    app = current_app._get_current_object()
    print(app)
    msg = Message(app.config['FOF_MAIL_SUBJECT_PREFIX'] + ' ' + subject,
                  sender=app.config['FOF_MAIL_SENDER'], recipients=[to])
    msg.body = render_template(template + '.txt', **kwargs)
    msg.html = render_template(template + '.html', **kwargs)
    thr = Thread(target=send_async_email, args=[app, msg])
    thr.start()
    return thr


class PermissionDeniedException(RuntimeError):
    """Permission denied to the resource."""

class Permission():
    def __init__(self, module=None, action=None):
        self.module = module
        self.action = action

    def check(self, module, func):
        if not current_user:
            return False
        return current_user.check('{module}.{action}'.format(
            module=module,
            action=func,
        ))

    def deny(self):
        logger.error("403　无权访问")
        flash("没有权限使用这个功能", "error")
        return redirect(request.referrer)

    def __call__(self, func):
        logger.info("权限检查 {} {} {}".format(func.__module__, func.__name__, func.__doc__))
        permissions.append({
            'action': '{}.{}'.format(func.__module__, func.__name__),
            'name': func.__doc__
        })

        @wraps(func)
        def decorator(*args, **kwargs):
            if not self.check(func.__module__, func.__name__):
                logger.warning("{}没有权限{}{}".format(current_user.username, func.__module__, func.__name__))
                return self.deny()
            return func(*args, **kwargs)
        return decorator

    def __enter__(self):
        if not self.check(self.module, self.action):
            try:
                self.deny()
            except Exception as e:
                raise e
            else:
                raise PermissionDeniedException()

    def __exit(self):
        pass


permission = Permission()
