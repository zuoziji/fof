from rpy2 import robjects
import pandas as pd
from config_fh import JSON_DB
import os
R_Type_Set = {robjects.vectors.ListVector, robjects.FloatVector, robjects.vectors.Array}
basedir = os.path.abspath(os.path.dirname(__file__))

def calc_portfolio_optim_fund(json_str, json_db):
    robjects.r.source(os.path.join(basedir,"portfolio_optim_strategy.R"))
    ret_data = robjects.r['PortOptimStrategy'](json_str, json_db)
    print("ret_data", ret_data)
    py_data = r2py_data_transfer(ret_data)
    return py_data


def r2py_data_transfer(r_data):
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


if __name__ == "__main__":
    import json

    info = {'subjective_view': {'strategy2': 'money_market', 'value': 0.02, 'strategy1': 'alpha'}, 'volatility': 0.24,
            'annual_return': 0, 'strategies': ['alpha', 'money_market'], 'cvar': 0, 'var': 0}
    info2 = {'expectations': {'annual_return': 0, 'cvar': 0, 'volatility': 0.15},
             'strategies': ['alpha', 'arbitrage', 'cta'],
             'subjective_views': [{"strategy1": 'alpha', "strategy2": 'arbitrage', "value": 0.05}]
             }

    json_str = json.dumps(info2)
    print(json_str)

    py_data = calc_portfolio_optim_fund(json_str, JSON_DB)
    for k, v in py_data.items():
        print(k)
        print(v.to_dict()['Weight'])
