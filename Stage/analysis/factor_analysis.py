# -*- coding: utf-8 -*-
"""
:copyright: (c) 2016 by Huang Dong 
:mail: bjdhuang@cn.ibm.com.
:license: Apache 2.0, see LICENSE for more details.
"""


from rpy2 import robjects
import os
from config_fh import  get_db_engine
import json
from pandas import DataFrame
import pandas as pd
import numpy as np



R_Type_Set = {robjects.vectors.ListVector, robjects.FloatVector, robjects.vectors.Array}
basedir = os.path.abspath(os.path.dirname(__file__))

def calc_portfolio_optim_fund():
    sql = "select nav_date, nav_acc from fund_nav where wind_code = 'fh_0052'"
    engine = get_db_engine()
    wind_code = 'fh_0052'
    fund_db_name = 'fund_sec_pct'
    query_df = pd.read_sql(sql, engine)
    query_df = query_df.T.to_dict()
    query_obj = [ {"nav_date":v['nav_date'].strftime("%Y-%m-%d"),"nav_acc":v['nav_acc']}   for _,v in query_df.items()]
    query_json = json.dumps({"result":query_obj})
    robjects.r.source(os.path.join(basedir,"Factor_Analysis_python.R"))
    mongo_ip = '10.0.5.107'

    ret_data = robjects.r['BigBro'](query_json,wind_code,fund_db_name,mongo_ip)
    print(ret_data)
    # py_data = r2py_data_transfer(ret_data)
    # print(py_data)



def r2py_data_transfer(r_data):
    print(r_data)
    if type(r_data) == robjects.vectors.DataFrame:
        ret_data = pd.DataFrame({col: list(l_data) for col, l_data in r_data.items()}, index=list(r_data.rownames))
    elif type(r_data) in R_Type_Set:
        # print('deep in:', r_data)
        r_data_count = len(r_data)
        if type(r_data.names) == robjects.vectors.ListVector:
            r_data_names = r_data.names[0]
        else:
            r_data_names = r_data.names
        ret_data = {r_data_names[n]: r2py_data_transfer(r_data[n]) for n in range(r_data_count)}
    else:
        ret_data = r_data
    return ret_data


def temp_load_method():
    with open(os.path.join(basedir,"test_factor1.json")) as f:
        r_obj = json.loads(f.read())
        capital_analysis = r_obj['capital_analysis']
        day_list = r_obj['day_list']
        r_capital = capital_analysis['Capital_exposure']
        r_factor = r_obj['factor_analysis']
        r_industry_analysis = r_obj['industry_analysis']
        F_LIST = r_obj['f_list']
        factor_data = []
        factor_key = []
        for k,v in r_factor.items():
            a0 = {}
            if k == "momentum":
                a0['name'] = "动量因子"
                a0['data'] = v
            elif k == "size":
                a0['name'] = "市值因子"
                a0['data'] = v
            elif k == "reverse":
                a0['name'] = "反转因子"
                a0['data'] = v
            factor_key.append(a0['name'])
            factor_data.append(a0)
        factor = {"factor":factor_data,"key":factor_key}
        pct_df = DataFrame(r_capital)
        capital_data_np = [{"name": i, "data": np.array(pct_df[i]).tolist()} for i in pct_df.columns if "pct" in i]
        capital_data = []
        for i in capital_data_np:
            a1 = {}
            if i['name'] == 'long_pct':
                a1['data'] = i['data']
                a1['name'] = "多头头寸"
            elif i['name'] == "short_pct":
                a1['data'] = i['data']
                a1['name'] = "空头头寸"
            elif i['name'] == "expo_pct":
                a1['data'] = i['data']
                a1['name'] = "头寸敞口"
            capital_data.append(a1)
        capital_key = [ i['name'] for i in capital_data]
        capital = {"capital":capital_data,"key":capital_key}
        CI_DIST = r_industry_analysis['CI_DIST']
        CI_EXPO_EARN = r_industry_analysis['CI_EXPO_EARN']
        CI_EXPO = r_industry_analysis['CI_EXPO']
        f_summary_coef_df = F_LIST['f_summary_coef']

        f_df = DataFrame(f_summary_coef_df,columns=["Estimate","Std_Error","t_Value","Pr(>|t|)"],index=['截距','动量','反转','市值']).T
        f_data = ([{"action":k,"data":v} for k,v in f_df.to_dict().items()])
        dist_df = DataFrame(CI_DIST, index=day_list).T
        ci_dist_pie_dict = dist_df.to_dict()

        ci_dist_key = dist_df.index.tolist()
        ci_dist_data = [{"name":k,"data":v }for k,v in CI_DIST.items()]
        ci_dist = {"ci_dist":ci_dist_data,"key":ci_dist_key}

        expo_df = DataFrame(CI_EXPO, index=day_list).T
        ci_expo_key = expo_df.index.tolist()
        ci_expo_data = [{"name":k,"data":v} for k,v in CI_EXPO.items()]
        ci_expo = {"ci_expo":ci_expo_data,"key":ci_expo_key}
        ci_expo_pie_dict = [{"name": i, "data": np.array(expo_df[i]).tolist()} for i in expo_df.columns ]


        return {"day":day_list,"capital":capital,"factor":factor,"ci_dist":ci_dist,"ci_dist_pie":ci_dist_pie_dict,
                "ci_expo_pie":ci_expo_pie_dict,"ci_expo":ci_expo,"ci_expo_earn":CI_EXPO_EARN,"f_data":f_data}





if __name__ == "__main__":
    temp_load_method()
    #calc_portfolio_optim_fund()