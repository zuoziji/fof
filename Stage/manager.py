# -*- coding: utf-8 -*-
"""
:copyright: (c) 2016 by Huang Dong 
:mail: bjdhuang@cn.ibm.com.
:license: Apache 2.0, see LICENSE for more details.
"""
#/usr/bin/python3.5

import os
from flask_script import Manager, Server as _Server, Option
from flask_migrate import Migrate,MigrateCommand
from fof_app import create_app,SocketIo
from fof_app import models
from logging import getLogger
logger = getLogger()
from gevent import monkey
monkey.patch_all()
from config_fh import get_redis


env = os.environ.get('APP_ENV','dev')
app = create_app('fof_app.config.%sConfig' %env.capitalize())
manager = Manager(app)
migrate = Migrate(app,models.db)

manager.add_command("db",MigrateCommand)

class Server(_Server):
    help = description = 'Runs the Socket.IO web server'
    def get_options(self):
        options = (
            Option('-h', '--host',
                   dest='host',
                   default=self.host),

            Option('-p', '--port',
                   dest='port',
                   type=int,
                   default=self.port),

            Option('-d', '--debug',
                   action='store_true',
                   dest='use_debugger',
                   help=('enable the Werkzeug debugger (DO NOT use in '
                         'production code)'),
                   default=self.use_debugger),
            Option('-D', '--no-debug',
                   action='store_false',
                   dest='use_debugger',
                   help='disable the Werkzeug debugger',
                   default=self.use_debugger),

            Option('-r', '--reload',
                   action='store_true',
                   dest='use_reloader',
                   help=('monitor Python files for changes (not 100%% safe '
                         'for production use)'),
                   default=self.use_reloader),
            Option('-R', '--no-reload',
                   action='store_false',
                   dest='use_reloader',
                   help='do not monitor Python files for changes',
                   default=self.use_reloader),
        )
        return options

    def __call__(self, app, host, port, use_debugger, use_reloader):
        # override the default runserver command to start a Socket.IO server
        if use_debugger is None:
            use_debugger = app.debug
            if use_debugger is None:
                use_debugger = True
        if use_reloader is None:
            use_reloader = app.debug
            r = get_redis(host='127.0.0.1',db=3)
            r.flushdb()
            logger.info("用户fof缓存已清空")
        SocketIo.run(app,
                     host=host,
                     port=port,
                     debug=use_debugger,
                     use_reloader=use_reloader,
                     **self.server_options)

manager.add_command("runserver", Server())


@manager.shell
def make_shell_context():
    return dict(app=app,
                db=models.db,
                User=models.UserModel,
                fof=models.FoFModel,
                Server=Server,
                Role=models.RoleModel,
                core=models.Fund_Core_Info,
                nav=models.FUND_NAV)


if __name__ == '__main__':
    logger.info("启动应用debug模式")
    manager.run()