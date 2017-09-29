import pandas as pd
# from WindPy import *
import sympy as smp
import scipy as scp
import scipy.stats as sss
import scipy.optimize as sop
import numpy as np
import pandas as pd
from datetime import datetime, date, timedelta
import matplotlib as mpl
import matplotlib.pyplot as plt
from fh_tools.fh_utils import get_cache_file_path
from scipy.stats import norm
import matplotlib.mlab as mlab
import pymysql
import json
from config_fh import get_db_engine, ANALYSIS_CACHE_FILE_NAME, get_db_session, get_redis, STR_FORMAT_DATE

zhfont1 = mpl.font_manager.FontProperties(fname='C:\Windows\Fonts\simkai.ttf')


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
        print('Copula function not ready yet')


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


class stress_test:
    def __init__(self, copula_family):
        self.coupla_family = copula_family
        self.sql_getfund = '''select nav_date2, max(nav_acc_max) as nav_acc_mm from (
            select adddate(nav_date, 4 - weekday(nav_date)) as nav_date2, max(nav_acc) as nav_acc_max 
            from fund_nav 
            where wind_code='%s' and nav_date >= '%s' and nav_date <= '%s' group by nav_date) as nav_t 
            group by nav_date2 order by nav_date2'''

    def get_fund_nv(self, fund_list, start_date, end_date):
        fund_nv = pd.DataFrame()
        for fund_code in fund_list:
            sql_fund = self.sql_getfund % (fund_code, start_date, end_date)
            engine = get_db_engine()
            fund_nv_tmp = pd.read_sql(sql_fund, engine)
            if len(fund_nv) == 0:
                fund_nv = fund_nv_tmp
            else:
                fund_nv = fund_nv.merge(fund_nv_tmp, on='nav_date2')
        fund_nv.dropna(how='any', inplace=True)
        fund_nv.set_index('nav_date2', inplace=True)
        return fund_nv

    def simulate_fund_profit(self, fund_nv):
        fund_profit = fund_nv.apply(lambda x: x / x.shift(1) - 1)
        fund_profit.dropna(how='any', inplace=True)
        # print(fund_profit)
        cdfs = np.zeros_like(fund_profit)
        norm_mean = np.zeros(fund_profit.columns.size)
        norm_var = np.zeros(fund_profit.columns.size)
        for i in range(fund_profit.columns.size):
            norm_mean[i], norm_var[i] = sss.norm.fit(fund_profit.iloc[:, i])
            cdfs[:, i] = sss.norm.cdf(fund_profit.iloc[:, i], norm_mean[i], norm_var[i])
        # %%
        Num_vars = fund_profit.columns.size
        Xs = smp.symbols('X1:%d' % (Num_vars + 1))
        # print(Num_vars, Xs)
        alpha = smp.symbols('alpha')
        myfunc = copula_func(self.coupla_family, Xs, alpha)
        myfunc_diff = copula_diff(myfunc, Xs)
        # print(Num_vars, Xs,1)
        alpha_num = estimate_parameter(myfunc_diff, cdfs, Xs, alpha)
        # %%

        simu_data = rnd_generator(alpha_num, len(Xs), 500)
        simu_data_conditional = simu_data[simu_data[:, 0] < 0.1]
        simu_real = simu_data.copy()
        for i in range(fund_profit.columns.size):
            simu_real[:, i] = norm.ppf(simu_real[:, i], norm_mean[i], norm_var[i])
        # for i in range(testdata.columns.size):
        #     simu_data_conditional[:,i]=sss.norm.ppf(simu_data_conditional[:,i], norm_mean[i], norm_var[i])
        # print(simu_data)
        return simu_real

    def get_max_drawdown(self, wind_code_list, start_date, end_date, weight_list, simulate_count):
        fnv = self.get_fund_nv(wind_code_list, start_date, end_date)
        weight_list = np.array(weight_list)
        weight_list = weight_list / sum(weight_list)
        max_dd_list = []
        for i in range(simulate_count):
            simu = self.simulate_fund_profit(fnv)
            simu_pd = pd.DataFrame(simu, columns=wind_code_list)
            # print(simu_pd.head(10))
            simu_pd = simu_pd + 1
            simu_pd = simu_pd.cumprod()
            simu_pd = simu_pd.multiply(weight_list, axis='columns')
            # print(simu_pd)
            simu_pd['FOF_nv'] = simu_pd.sum(axis='columns')
            max_dd_tmp = cal_maxdd(simu_pd['FOF_nv'])
            max_dd_list.append(max_dd_tmp)
        return max_dd_list


def plot_fof_copula(wind_code_list, weighted_list, startdate, enddate, simulate_count):
    start_time = datetime.now()
    st = stress_test('Clayton')
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


if __name__ == '__main__':
    # wind_code_list = ['XT1410445.XT', 'J11039.OF']
    # weight_list = [1, 1]
    # start_date = '2016-01-01'
    # end_date = '2016-12-31'
    # simulate_count = 5000
    # file_path = plot_fof_copula(wind_code_list, weight_list, start_date, end_date, simulate_count)
    sql_str = """select id, ffp.wind_code_p, wind_code, date_adj, invest_scale 
from fof_fund_pct ffp,
(
select wind_code_p, max(date_adj) date_latest from fof_fund_pct group by wind_code_p
) ff_date_latest
where ffp.wind_code_p = ff_date_latest.wind_code_p
and ffp.date_adj = ff_date_latest.date_latest"""
    engine = get_db_engine()
    fof_fund_df = pd.read_sql(sql_str, engine)
    wind_code_fund_dic = dict(list(fof_fund_df.groupby('wind_code_p')))
    date_to = date.today() - timedelta(days=1)
    date_to_str = '2016-12-31'  # date_to.strftime(STR_FORMAT_DATE)
    date_from = date_to - timedelta()
    date_from_str = '2016-01-01'  # date_from.strftime(STR_FORMAT_DATE)
    simulate_count = 1000
    r = get_redis()
    for wind_code_p, fof_fund_sub_df in wind_code_fund_dic.items():
        wind_code_list = list(fof_fund_sub_df['wind_code'])
        wind_code_count = len(wind_code_list)
        if wind_code_count <= 0:
            continue
        st = stress_test('Clayton')
        weighted_list = np.ones(wind_code_count)
        max_dd_list = st.get_max_drawdown(['XT1410445.XT', 'J11039.OF'], date_from_str, date_to_str, weighted_list, simulate_count)
        max_dd_list_str = json.dumps(max_dd_list)
        r.set(wind_code_p, max_dd_list_str)
    print('finished')
