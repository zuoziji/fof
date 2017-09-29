from rpy2 import robjects
import pandas as pd
from  config_fh import JSON_DB
import os


R_Type_Set = {robjects.vectors.ListVector, robjects.FloatVector, robjects.vectors.Array}
basedir = os.path.abspath(os.path.dirname(__file__))

def calc_portfolio_optim_fund(json_str, json_db):
    robjects.r.source(os.path.join(basedir,"portfolio_optim_fund.R"))
    ret_data = robjects.r['PortOptimMultiConsFOF'](json_str, json_db)
    #print("ret_data", ret_data)
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
    x2 = {'funds': [{'strategies': [{'strategy': 'alpha', 'percent': 100}], 'fund': 'FHF-101601C'}, {'strategies': [{'strategy': 'alpha', 'percent': 100}], 'fund': 'XT1605537.XT'}, {'strategies': [{'strategy': 'long_only', 'percent': 100}], 'fund': 'XT1614159.XT'}], 'strategies': [{'strategy': 'alpha', 'percent': 39, 'operator': 'smallerorequal'}, {'strategy': 'long_only', 'percent': 60, 'operator': 'largerorequal'}], 'expectations': {'var': 0, 'annual_return': 0, 'cvar': 0, 'volatility': 0.17}, 'subjective_view': {'strategy1': 'alpha', 'strategy2': 'long_only', 'value': 0.01}}

    json_str = json.dumps(x2)
    py_data = calc_portfolio_optim_fund(json_str,JSON_DB)
   # print(py_data.keys())
   # print(py_data['fundWeightA'])
    print(py_data)