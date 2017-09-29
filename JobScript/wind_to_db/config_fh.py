from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from fh_tools import fh_utils, windy_utils_rest
from datetime import datetime
import redis

# 数据库地址 端口
DB_IP = "10.0.3.66"  # "10.0.5.111"
DB_PORT = "3306"
# 数据库名称
DB_NAME = "fof_ams_dev"
# 数据库用户名
DB_USER = "mg"
# 数据库密码
DB_PASSWORD = "Abcd1234"
# 策略中英文名称对照关系
STRATEGY_EN_CN_DIC = {
    'long_only': '股票多头策略',
    'long_short': '股票多空策略',
    'event_driven': '事件驱动策略',
    'other_equity': '其他股票策略',
    'alpha': '阿尔法策略',
    'fixed_income': '债券策略',
    'money_market': '货币市场策略',
    'cta': '管理期货策略',
    'arbitrage': '套利策略',
    'macro': '宏观策略',
}
STRATEGY_CN_EN_DIC = {cn: en for en, cn in STRATEGY_EN_CN_DIC.items()}
# 模型静态文件缓存目录
ANALYSIS_CACHE_FILE_NAME = 'analysis_cache'
STR_FORMAT_DATE = '%Y-%m-%d'
UN_AVAILABLE_DATE = datetime.strptime('1900-01-01', STR_FORMAT_DATE).date()

# "http://10.0.3.84:5000/wind/"
WIND_REST_URL = "http://10.0.3.66:5000/wind/"  # "http://10.0.5.110:5000/wind/"

# 配置Redis数据库地址、端口、db
REDIS_DB_HOST = '10.0.3.61'
REDIS_DB_PORT = 6379
REDIS_DB_DB = 1

def get_db_engine() -> object:
    """初始化数据库engine"""

    engine = create_engine("mysql+pymysql://%s:%s@%s:%s/%s?charset=utf8" % (
        DB_USER, DB_PASSWORD, DB_IP, DB_PORT, DB_NAME),
                           echo=False, encoding="utf-8")
    return engine


def get_wind_rest():
    rest = windy_utils_rest.WindRest(WIND_REST_URL)
    return rest


class session_wrapper:
    """用于对session对象进行封装，方便使用with语句进行close控制"""

    def __init__(self, session):
        self.session = session

    def __enter__(self):
        return self.session

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.session.close()
        # print("close session")


def get_db_session(engine=None):
    """创建session对象，返回 session_wrapper 可以使用with语句进行调用"""
    if engine is None:
        engine = get_db_engine()
    db_session = sessionmaker(bind=engine)
    session = db_session()
    return session_wrapper(session)


def get_cache_file_path(file_name):
    return fh_utils.get_cache_file_path(ANALYSIS_CACHE_FILE_NAME, file_name)


def get_redis():
    r = redis.Redis(host=REDIS_DB_HOST, port=REDIS_DB_PORT, db=REDIS_DB_DB)
    return r


if __name__ == '__main__':
    rest = get_wind_rest()
    ret_df = rest.wsd("000001.SH", "open,high,low,close,volume,amt", "2017-04-25", "2017-04-25", None)
    print(ret_df)
