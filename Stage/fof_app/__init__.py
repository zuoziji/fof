# -*- coding: utf-8 -*-
"""
:copyright: (c) 2016 by Huang Dong 
:mail: bjdhuang@cn.ibm.com.
:license: Apache 2.0, see LICENSE for more details.
"""


import flask_whooshalchemyplus as wa
from celery import platforms
from flask import Flask
from fof_app.controllers import f_app
from .config import Config
from .controllers import auth,manager_task
from .controllers.admin_view import FofAdmin, PctAdmin, StgAdmin, UserAdmin, \
    RoleAdmin, PerAdmin, FileTypeAdmin, FileAdmin, AccAdmin, SecAdmin, EventAdmin, ChildMapping,Invest_corp_admin,Invest_corp_file_admin
from .extensions import login_manager, babel, admin, mail,SocketIo,celery
from .models import db, RoleModel, UserModel, FoFModel, PermissionModel, FileType, \
    FundFile, FOF_FUND_PCT, FUND_STG_PCT, FUND_NAV, FUND_SEC_PCT, FUND_EVENT, FUND_ESSENTIAL,Invest_corp,Invest_corp_file

platforms.C_FORCE_ROOT = True



admin.add_view(UserAdmin(UserModel, name='用户管理', session=db.session))
admin.add_view(RoleAdmin(RoleModel,name="角色管理",session=db.session))
admin.add_view(FofAdmin(FoFModel, name='母基金管理', session=db.session))
admin.add_view(PerAdmin(PermissionModel, name='权限管理', session=db.session))
admin.add_view(FileTypeAdmin(FileType, name='文件类型管理', session=db.session))
admin.add_view(FileAdmin(FundFile, name='文件管理', session=db.session))
admin.add_view(PctAdmin(FOF_FUND_PCT, name='子基金管理', session=db.session))
admin.add_view(StgAdmin(FUND_STG_PCT, name='策略管理', session=db.session))
admin.add_view(AccAdmin(FUND_NAV, name='净值管理', session=db.session))
admin.add_view(SecAdmin(FUND_SEC_PCT, name='资产管理', session=db.session))
admin.add_view(EventAdmin(FUND_EVENT, name='提醒管理', session=db.session))
admin.add_view(ChildMapping(FUND_ESSENTIAL,name="jijinyasu",session=db.session))
admin.add_view(Invest_corp_admin(Invest_corp,name="投顾管理",session=db.session))
admin.add_view(Invest_corp_file_admin(Invest_corp_file,name="投顾文件管理",session=db.session))

def create_app(object_name):
    app = Flask(__name__)
    wa.whoosh_index(app,FUND_ESSENTIAL)
    wa.whoosh_index(app, FoFModel)
    app.config.from_object(object_name)
    db.init_app(app)
    login_manager.init_app(app)
    admin.init_app(app)
    celery.init_app(app)
    babel.init_app(app)
    mail.init_app(app)
    SocketIo.init_app(app,message_queue=app.config['SOCKETIO_MESSAGE_QUEUE'],async_mode='gevent')
    app.register_blueprint(f_app.f_app_blueprint)
    app.register_blueprint(auth.auth_blueprint)
    app.register_blueprint(manager_task.task_blueprint,url_prefix='/task')
    return app




