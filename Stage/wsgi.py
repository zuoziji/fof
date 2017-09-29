# -*- coding: utf-8 -*-
"""
:copyright: (c) 2016 by Huang Dong 
:mail: bjdhuang@cn.ibm.com.
:license: Apache 2.0, see LICENSE for more details.
"""
from fof_app import create_app
import os
from config_fh import get_redis
import logging
logger = logging.getLogger()
env = os.environ.get('APP_ENV','prod')
wsgi_app = create_app('fof_app.config.%sConfig' % env.capitalize())
r = get_redis(db=3)
r.flushdb()
if  __name__ == '__main__':
    wsgi_app.run()
    logger.info("用户fof缓存已清空")