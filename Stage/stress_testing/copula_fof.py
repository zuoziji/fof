import sympy as smp
import scipy.stats as sss
import scipy.optimize as sop
import numpy as np
import pandas as pd
from datetime import datetime, date, timedelta
import matplotlib.pyplot as plt
from fh_tools.fh_utils import get_cache_file_path, date_2_str
from scipy.stats import norm
import json
from config_fh import get_db_engine, ANALYSIS_CACHE_FILE_NAME, get_db_session, get_redis, STR_FORMAT_DATE, \
    STRESS_TESTING_SIMULATE_COUNT_COPULA
import logging
logger = logging.getLogger()


def copula_func(copula_family, Xs_in, alpha_in):
    Num_vars_in = len(Xs_in)
    inner_fun = 0

    if copula_family == 'Gumbel':
        for i in range(Num_vars_in):
            inner_fun += (-smp.log(Xs_in[i])) ** alpha_in
        gumbel_expr = smp.exp(-inner_fun ** (1 / alpha_in))
        return gumbel_expr
    elif copula_family == 'Clayton':
        for i in range(Num_vars_in):
            inner_fun += Xs_in[i] ** (-alpha_in)
        clayton_expr = (inner_fun - Num_vars_in + 1) ** (-1 / alpha_in)
        return clayton_expr
    else:
        logger.error('Copula[%s] function not ready yet', copula_family)


def copula_diff(func_in, Xs_in):
    Num_vars_in = len(Xs_in)
    temp_func = func_in.copy()
    for i in range(Num_vars_in):
        temp_func = smp.diff(temp_func, Xs_in[i])
    return temp_func


def estimate_parameter(diff_func, data_in, Xs_in, alpha_in):
    temp = 1
    for i in range(len(data_in)):
        temp = temp * diff_func.subs([(Xs_in[j], data_in[i, j]) for j in range(len(Xs_in))])
    myfunc = smp.lambdify(alpha_in, -smp.log(temp), 'math')
    ##function used to make alpha as para, x as value only
    alpha_num = sop.fminbound(myfunc, 1, 10)
    return alpha_num


def rnd_generator(alpha_in, var_num_in, size_in):
    temp = np.zeros((size_in, var_num_in))
    for i in range(size_in):
        gamma_rv = np.random.gamma(1, 1 / alpha_in)
        rnd_n = np.random.rand(var_num_in)
        rnd = (1 - 1 / gamma_rv * np.log(rnd_n)) ** (-1 / alpha_in)
        temp[i, :] = rnd
    return temp


def cal_maxdd(nvlist):
    dd = []
    for nvindex, nv in enumerate(nvlist):
        nvmax = max(nvlist[0:nvindex + 1])
        dd_now = nv / nvmax - 1
        dd.append(dd_now)
    return min(dd)


class StressTest:
    def __init__(self, copula_family):
        self.coupla_family = copula_family

    def get_fund_nav_df(self, fund_code_list, start_date, end_date, weight_list):

        start_date_str = date_2_str(start_date)
        end_date_str = date_2_str(end_date)
        fund_nav_df = pd.DataFrame()
        sql_str = '''select nav_date_friday, nav_acc 
from fund_nav fv, 
	(
	select wind_code, adddate(nav_date, 4 - weekday(nav_date)) as nav_date_friday, max(nav_date) as nav_date_max 
	from fund_nav 
	where wind_code=%s and nav_date between %s and %s
    group by nav_date_friday) as nav_t 
where fv.wind_code = %s
and fv.wind_code = nav_t.wind_code
and fv.nav_date = nav_t.nav_date_max
group by nav_date_friday 
order by nav_date_friday'''
        engine = get_db_engine()
        ok_list = []
        for fund_code in fund_code_list:
            fund_nv_tmp = pd.read_sql(sql_str, engine, params=[fund_code, start_date_str, end_date_str, fund_code])
            if fund_nv_tmp.shape[0] <= 1:
                logger.info('%s with %d nav data will be ignored', fund_code, fund_nv_tmp.shape[0])
                ok_list.append(False)
                continue
            if len(fund_nav_df) == 0:
                fund_nav_df = fund_nv_tmp
                logger.info('%s with %d nav data', fund_code, fund_nav_df.shape[0])
            else:
                fund_nav_df = fund_nav_df.merge(fund_nv_tmp, on='nav_date_friday')
                logger.info('%s with %d nav data have %d data merged',
                             fund_code, fund_nv_tmp.shape[0], fund_nav_df.shape[0])
            ok_list.append(True)

        fund_nav_df.dropna(how='any', inplace=True)
        if fund_nav_df.shape[0] <= 1:
            logger.error('no data found from fund_list: %s', fund_code_list)
            return None, None, None

        fund_nav_df.set_index('nav_date_friday', inplace=True)
        fund_nav_pct_df = fund_nav_df.pct_change().fillna(0)

        # 权重归一化处理
        weight_arr = np.array(weight_list)[ok_list]
        weight_arr = weight_arr / sum(weight_arr)

        # 筛选有效 wind_code_list
        wind_code_new_list = np.array(fund_code_list)[ok_list]
        return wind_code_new_list, weight_arr, fund_nav_pct_df

    def simulate_fund_profit(self, fund_nav_pct_df):
        # fund_profit = fund_nv.apply(lambda x: x / x.shift(1) - 1)
        # fund_profit.dropna(how='any', inplace=True)
        # fund_nav_pct_df = fund_nv.pct_change().fillna(0)
        # print(fund_profit)
        cdfs = np.zeros_like(fund_nav_pct_df)
        norm_mean = np.zeros(fund_nav_pct_df.columns.size)
        norm_var = np.zeros(fund_nav_pct_df.columns.size)
        for i in range(fund_nav_pct_df.columns.size):
            norm_mean[i], norm_var[i] = sss.norm.fit(fund_nav_pct_df.iloc[:, i])
            cdfs[:, i] = sss.norm.cdf(fund_nav_pct_df.iloc[:, i], norm_mean[i], norm_var[i])

        Num_vars = fund_nav_pct_df.columns.size
        Xs = smp.symbols('X1:%d' % (Num_vars + 1))
        # print(Num_vars, Xs)
        alpha = smp.symbols('alpha')
        myfunc = copula_func(self.coupla_family, Xs, alpha)
        myfunc_diff = copula_diff(myfunc, Xs)
        # print(Num_vars, Xs,1)
        alpha_num = estimate_parameter(myfunc_diff, cdfs, Xs, alpha)

        simu_data = rnd_generator(alpha_num, len(Xs), 500)
        simu_data_conditional = simu_data[simu_data[:, 0] < 0.1]
        simu_real = simu_data.copy()
        for i in range(fund_nav_pct_df.columns.size):
            simu_real[:, i] = norm.ppf(simu_real[:, i], norm_mean[i], norm_var[i])
        # for i in range(testdata.columns.size):
        #     simu_data_conditional[:,i]=sss.norm.ppf(simu_data_conditional[:,i], norm_mean[i], norm_var[i])
        # print(simu_data)
        return simu_real

    def get_max_drawdown(self, wind_code_list, start_date, end_date, weight_list, simulate_count):
        wind_code_count = len(wind_code_list)
        wind_code_new_list, weight_arr, fund_nav_pct_df = self.get_fund_nav_df(wind_code_list, start_date, end_date,
                                                                               weight_list)
        max_dd_list = []
        if fund_nav_pct_df is not None:
            try:
                for i in range(simulate_count):
                    logger.info('get_max_drawdown func simulate %d wind_code between %s - %s [%d / %d]',
                                 wind_code_count, start_date, end_date, i + 1, simulate_count)
                    simu = self.simulate_fund_profit(fund_nav_pct_df)
                    simu_pd = pd.DataFrame(simu, columns=wind_code_new_list)
                    # print(simu_pd.head(10))
                    simu_pd = simu_pd + 1
                    simu_pd = simu_pd.cumprod()
                    simu_pd = simu_pd.multiply(weight_arr, axis='columns')
                    # print(simu_pd)
                    simu_pd['FOF_nv'] = simu_pd.sum(axis='columns')
                    max_dd_tmp = cal_maxdd(simu_pd['FOF_nv'])
                    max_dd_list.append(max_dd_tmp)
            except:
                logger.exception('get_max_drawdown got exception')
        return max_dd_list


def plot_fof_copula(wind_code_list, weighted_list, startdate, enddate, simulate_count):
    start_time = datetime.now()
    st = StressTest('Clayton')
    dd = st.get_max_drawdown(wind_code_list, startdate, enddate, weighted_list, simulate_count)
    figure_time = datetime.now()
    file_name = '%s_%s.png' % ('fof', figure_time.strftime('%Y_%m_%d %H_%M_%S'))
    file_path = get_cache_file_path(ANALYSIS_CACHE_FILE_NAME, file_name)
    plt.hist(dd)
    # plt.show()
    plt.savefig(file_path)
    finished_time = datetime.now()
    print("time estimate:", finished_time - start_time)
    return file_path


def do_copula(wind_code_list=[]):
    # sql_str = """select id, ffp.wind_code_p, wind_code, date_adj, invest_scale
    # from fof_fund_pct ffp,
    # (
    # select wind_code_p, max(date_adj) date_latest from fof_fund_pct group by wind_code_p
    # ) ff_date_latest
    # where ffp.wind_code_p = ff_date_latest.wind_code_p
    # and ffp.date_adj = ff_date_latest.date_latest"""
    sql_str = """select id, ffp.wind_code_p, ffp.wind_code_s, wind_code, date_adj, invest_scale 
from fof_fund_pct ffp,
(
select wind_code_p, max(date_adj) date_latest from fof_fund_pct group by wind_code_p
) ff_date_latest,
fund_essential_info ffm
where ffp.wind_code_p = ff_date_latest.wind_code_p
and ffp.wind_code_s = ffm.wind_code_s
and ffp.date_adj = ff_date_latest.date_latest"""
    engine = get_db_engine()
    fof_fund_df = pd.read_sql(sql_str, engine)
    # 按 母基金代码进行分组
    wind_code_fund_dic = dict(list(fof_fund_df.groupby('wind_code_p')))
    # 按 基金代码进行分组，对应所有 wind_code_s 代码
    # wind_code_df_dic = dict(list(fof_fund_df[['wind_code', 'wind_code_s']].groupby('wind_code')))
    # wind_code_s_dic = {wind_code: set(df['wind_code_s'].values) for wind_code, df in wind_code_df_dic.items()}

    date_to = date.today()
    date_to_str = date_to.strftime(STR_FORMAT_DATE)
    date_from = date_to - timedelta(days=365)
    date_from_str = date_from.strftime(STR_FORMAT_DATE)
    simulate_count = STRESS_TESTING_SIMULATE_COUNT_COPULA
    r = get_redis()
    if wind_code_list is not None and len(wind_code_list) > 0:
        wind_code_set = set(wind_code_list)
    else:
        wind_code_set = None
    for wind_code_p, fof_fund_sub_df in wind_code_fund_dic.items():
        if wind_code_set is not None and wind_code_p not in wind_code_set:
            continue
        wind_code_list = list(fof_fund_sub_df['wind_code'])  # ['XT1410445.XT', 'J11039.OF']
        wind_code_count = len(wind_code_list)
        logger.info('do_copula on wind_code_p with %s', wind_code_list)
        if wind_code_count <= 0:
            continue
        st = StressTest('Clayton')
        weighted_list = np.ones(wind_code_count)
        # max_dd_list = st.get_max_drawdown(['XT1410445.XT', 'J11039.OF'], '2016-01-01', '2016-12-31', np.ones(2), 10)
        max_dd_list = st.get_max_drawdown(wind_code_list, date_from_str, date_to_str, weighted_list, simulate_count)
        if max_dd_list is None or len(max_dd_list) == 0:
            logger.error('%s has no copula test data. sub fund list: %s', wind_code_p, wind_code_list)
            continue
            # max_dd_list_str = json.dumps(max_dd_list)
        y, x, patches = plt.hist(max_dd_list, 10)
        y = list(map(int, y))
        x = list(map(float, ["%.3f" % i for i in x]))
        key = '%s:%s' % (wind_code_p, 'copula')
        logger.debug('%s has been completer' % key)
        r.set(key, json.dumps({"x": x, "y": y}))


def do_copula_4_scheme(scheme_id):
    """
    根据 scheme_id 计算相关组合的 copula 压力测试
    :param scheme_id: 
    :return: 
    """

#     sql_str = """select id, ffp.wind_code_p, ffp.wind_code_s, wind_code, date_adj, invest_scale
# from fof_fund_pct ffp,
# (
# select wind_code_p, max(date_adj) date_latest from fof_fund_pct group by wind_code_p
# ) ff_date_latest,
# fund_essential_info ffm
# where ffp.wind_code_p = ff_date_latest.wind_code_p
# and ffp.wind_code_s = ffm.wind_code_s
# and ffp.date_adj = ff_date_latest.date_latest"""
    sql_str = "SELECT wind_code, invest_scale FROM scheme_fund_pct where scheme_id=%(scheme_id)s"
    engine = get_db_engine()
    fund_pct_df = pd.read_sql(sql_str, engine, params={'scheme_id': str(scheme_id)})

    date_to = date.today()
    date_to_str = date_to.strftime(STR_FORMAT_DATE)
    date_from = date_to - timedelta(days=365)
    date_from_str = date_from.strftime(STR_FORMAT_DATE)
    simulate_count = STRESS_TESTING_SIMULATE_COUNT_COPULA
    r = get_redis()

    wind_code_list = list(fund_pct_df['wind_code'])  # ['XT1410445.XT', 'J11039.OF']
    wind_code_count = len(wind_code_list)
    logger.info('do_copula for %d on wind_code_p with %s', scheme_id, wind_code_list)
    if wind_code_count <= 0:
        logger.warning('scheme %s has no sub fund list', scheme_id)
        return
    st = StressTest('Clayton')
    weighted_list = np.ones(wind_code_count)
    # max_dd_list = st.get_max_drawdown(['XT1410445.XT', 'J11039.OF'], '2016-01-01', '2016-12-31', np.ones(2), 10)
    max_dd_list = st.get_max_drawdown(wind_code_list, date_from_str, date_to_str, weighted_list, simulate_count)
    if max_dd_list is None or len(max_dd_list) == 0:
        logger.error('scheme %s has no copula test data. sub fund list: %s', scheme_id, wind_code_list)
        return
        # max_dd_list_str = json.dumps(max_dd_list)
    y, x, patches = plt.hist(max_dd_list, 20)
    y = list(map(int, y))
    x = list(map(float, ["%.3f" % i for i in x]))
    key = 'scheme_%s_%s' % (scheme_id, 'copula')
    val_str = json.dumps({"x": x, "y": y})
    logger.debug('%s has been completer\n%s', key, val_str)
    r.set(key, val_str)


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s: %(levelname)s [%(name)s:%(funcName)s] %(message)s')
    # wind_code_list = ['XT1410445.XT', 'J11039.OF']
    # weight_list = [1, 1]
    # start_date = '2016-01-01'
    # end_date = '2016-12-31'
    # simulate_count = 100
    # file_path = plot_fof_copula(wind_code_list,
    # weight_list, start_date, end_date, simulate_count)
    # wind_code_list = ['FHF-101602']
    # do_copula(wind_code_list=wind_code_list)
    scheme_id = 211
    do_copula_4_scheme(scheme_id)
