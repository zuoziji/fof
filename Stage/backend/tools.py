# -*- coding: utf-8 -*-
"""
:copyright: (c) 2016 by Huang Dong 
:mail: bjdhuang@cn.ibm.com.
:license: Apache 2.0, see LICENSE for more details.
"""
from functools import wraps
from fof_app.models import FoFModel, FUND_ESSENTIAL, global_user_cache, db, FOF_FUND_PCT, FUND_STG_PCT, code_get_name, \
    Fund_Core_Info, FUND_NAV, FUND_TRANSACTION
from flask import request, current_app, redirect, flash
from flask_login import current_user
from datetime import date
import logging, json
from config_fh import get_redis
from fh_tools.fh_utils import return_risk_analysis, drawback_analysis
from analysis.factor_analysis import temp_load_method
from sqlalchemy import and_, text
from pandas import DataFrame
import pandas as pd
import numpy as np
from random import randrange
from datetime import timedelta

logger = logging.getLogger()


def fund_owner(func):
    """
    装饰器,获得用户可管理的所有基金，包含批次等
    检查访问的基金是否在可管理的基金列表,同时限制对批次可能执行的操作，需要在confing中配置可访问的路由名称
    :param func:
    :param wind_code:基金代码
    :return: func
    """

    @wraps(func)
    def _deco(wind_code: str) -> object:
        logger.info("当前用户{}基金代码{}".format(current_user.username, wind_code))
        all_fund = set()
        for i in global_user_cache():
            all_fund.add(i['primary'].wind_code)
            all_child = FOF_FUND_PCT.query.filter_by(wind_code_p=i['primary'].wind_code).all()
            for c in all_child:
                if c.wind_code_s != 'fh0000':
                    all_fund.add(c.wind_code_s)
            if 'child' in i:
                for x in i['child']:
                    batch = FUND_ESSENTIAL.query.filter_by(wind_code_s=x['code']).first()
                    if batch is None:
                        fund = FoFModel.query.filter_by(wind_code=x['code']).first()
                    else:
                        fund = FoFModel.query.filter_by(wind_code=batch.wind_code).first()
                    if fund not in all_fund:
                        all_fund.add(fund.wind_code)
        if wind_code in all_fund:
            fund = FoFModel.query.filter_by(wind_code=wind_code).first()
            if fund is not None:
                ret = func(wind_code)
                return ret
            else:
                mapping = FUND_ESSENTIAL.query.filter_by(wind_code_s=wind_code).first()
                if mapping is not None and request.endpoint[6:] in current_app.config['REQUEST_LIMIT']:
                    ret = func(wind_code)
                    return ret
                else:
                    flash("看起来你现在访问的是一个基金的批次,当前的策略还不支持对子基金的批次执行这个操作，请稍后在访问", "error")
                    return redirect(request.referrer)
        else:
            flash("没有权限使用这个功能", "error")
            return redirect(request.referrer)

    return _deco


def check_code_order(wind_code):
    fof = FoFModel.query.filter_by(wind_code=wind_code).first()
    if fof is None:
        fof_mapping = FUND_ESSENTIAL.query.filter_by(wind_code_s=wind_code).first()
        fof = FoFModel.query.filter_by(wind_code=fof_mapping.wind_code).first()
        fof.sec_name = fof_mapping.sec_name_s
        fof.wind_code = fof_mapping.wind_code_s
        db.session.remove()
    return fof


def get_core_info(wind_code):
    core_info = Fund_Core_Info.query.filter_by(wind_code=wind_code).first()
    if core_info is None:
        fof = FoFModel.query.filter_by(wind_code=wind_code).first()
        if fof:
            return fof
        else:
            fof_mapping = FUND_ESSENTIAL.query.filter_by(wind_code_s=wind_code).first()
            fof = FoFModel.query.filter_by(wind_code=fof_mapping.wind_code).first()
            return fof
    else:
        return core_info


def chunks(l: list, n: int):
    for i in range(0, len(l), n):
        yield l[i:i + n]


def get_Value(dic: dict, value: str) -> str:
    if value == 'value':
        return value
    for name in dic:
        if dic[name] == value:
            return name


def range_years(start, end) -> tuple:
    dt = date.today()
    start = "%d-%d-%d" % (dt.year - start, dt.month, dt.day)
    end = "%d-%d-%d" % (dt.year - end, dt.month, dt.day)
    return start, end


def get_stress_data(wind_code):
    r = get_redis()
    fof = FoFModel.query.filter_by(wind_code=wind_code).first()
    if fof is None:
        fof_mapping = FUND_ESSENTIAL.query.filter_by(wind_code_s=wind_code).first()
        wind_code = fof_mapping.wind_code
    fhs = r.get(wind_code + '_fhs_garch')
    if fhs is not None:
        logger.info("{}有fhs_garch压力测试数据".format(wind_code))
        fhs_obj = json.loads(fhs.decode('utf-8'))
    else:
        logger.info("{}没有fhs_garch压力测试数据".format(wind_code))
        fhs_obj = ""
    copula = r.get(wind_code + ':copula')
    if copula is not None:
        logger.info("{}有copula压力测试数据".format(wind_code))
        copula_obj = json.loads(copula.decode('utf-8'))
    else:
        logger.info("{}没有copula压力测试数据".format(wind_code))
        copula_obj = ""
    multi = r.get(wind_code + 's:multi_factor')
    if multi is not None:
        logger.info("{}有多因子压力测试数据".format(wind_code))
        multi_obj = json.loads(multi.decode('utf-8'))
        legend = sorted([i for i in multi_obj])
        c_type = sorted({a for _, v in multi_obj.items() for a, _ in v.items()})
        indicator = [{'text': i, 'max': 5} for i in c_type]
        series = [{"name": k, "value": [v[i] + 3 for i in c_type]} for k, v in multi_obj.items()]
        multi_obj = {"legend": legend, "c_type": c_type, "indicator": indicator, "series": series}
    else:
        multi_obj = {"legend": []}
        logger.info("{}没有多因子压力测试数据".format(wind_code))
    if wind_code == 'fh_0052':
        capital_data = temp_load_method()
    else:
        capital_data = None
    return fhs_obj, copula_obj, multi_obj, capital_data


def get_stg(wind_code):
    fof = FoFModel.query.filter_by(wind_code=wind_code).first()
    if fof is None:
        fof_mapping = FUND_ESSENTIAL.query.filter_by(wind_code_s=wind_code).first()
        wind_code = fof_mapping.wind_code
    stg_pct = FUND_STG_PCT.query.filter_by(wind_code=wind_code)
    stg = [{"type": i.stg_code, "scale": i.stg_pct, "date": i.trade_date.strftime("%Y-%m-%d")} for i in stg_pct]
    # stg_charats
    stg_query = db.session.query(FUND_STG_PCT.trade_date.distinct().
                                 label("t_date")).filter_by(wind_code=wind_code).order_by(FUND_STG_PCT.trade_date)
    stg_time_line = [row.t_date.strftime('%Y-%m-%d') for row in stg_query.all()]
    stg_status = []
    for i in stg_time_line:
        status = FUND_STG_PCT.query.filter(
            and_(FUND_STG_PCT.trade_date == i, FUND_STG_PCT.wind_code == wind_code)).all()
        stg_status.append([{"name": i.stg_code, 'value': i.stg_pct} for i in status])
    stg_charts = {"date": stg_time_line, 'value': stg_status}
    return {"stg": stg, "stg_charts": stg_charts}


def child_charts(wind_code, display_type):
    query = db.session.query(FOF_FUND_PCT.date_adj.distinct().
                             label("t_date")).filter_by(wind_code_p=wind_code).order_by(FOF_FUND_PCT.date_adj)
    status_date = [row.t_date.strftime('%Y-%m-%d') for row in query.all()]
    t_status = []
    if display_type == 'batch':
        for i in status_date:
            status = FOF_FUND_PCT.query.filter(
                and_(FOF_FUND_PCT.wind_code_p == wind_code, FOF_FUND_PCT.date_adj == i)).all()
            t_status.append([{"name": code_get_name(x.wind_code_s), "value": x.invest_scale} for x in status])
        return {"date": status_date, "value": t_status}
    else:
        for i in status_date:
            status = FOF_FUND_PCT.query.filter(
                and_(FOF_FUND_PCT.wind_code_p == wind_code, FOF_FUND_PCT.date_adj == i)).all()
            content = []
            for x in status:
                data = {}
                data['value'] = x.invest_scale
                if x.wind_code_s == 'fh0000':
                    data['name'] = 'fh0000'
                else:
                    t1 = FUND_ESSENTIAL.query.filter_by(wind_code_s=x.wind_code_s).first()
                    if t1 is not None:
                        data['name'] = t1.wind_code
                    else:
                        data['name'] = x.wind_code_p
                content.append(data)
                df = DataFrame(content)
                sum_df = df.groupby(["name"]).sum()
                sum_dict = sum_df['value'].to_dict()
                sum_list = [{"name": code_get_name(k), 'value': v} for k, v in sum_dict.items()]
            t_status.append(sum_list)
        return {"date": status_date, "value": t_status}


def calc_periods(wind_code):
    """
    计算基金净值月度增长率
    :param wind_code: 基金代码
    :return: list
    """
    all_record = FUND_NAV.query.filter_by(wind_code=wind_code).all()
    if len(all_record) > 1:
        record_list = [i.as_dict() for i in all_record]
        df = DataFrame(record_list)
        df.index = pd.to_datetime(df['nav_date'])
        df = df.drop(['nav', 'nav_tot', 'source_mark', 'wind_code'], axis=1)
        risk_df = return_risk_analysis(df[['nav_acc']], freq=None)
        result_dict = {}
        risk = {k: v['nav_acc'] for k, v in risk_df.T.to_dict().items()}
        drawback_df = drawback_analysis((df[['nav_acc']]))
        drawback = {k.strftime("%Y-%m-%d"): "%.4f" % v['nav_acc'] for k, v in drawback_df.T.to_dict().items()}
        result = df.resample('M', convention='end').pct_change()
        for k, v in result.T.to_dict().items():
            month = k.strftime('%Y-%m')
            if not np.isnan(v['nav_acc']):
                value = v['nav_acc'] * 100
                result_dict[month] = "%.3f" % value
        return result_dict, risk, drawback
    else:
        return None, None, None

def random_date(start, end):
    """
    This function will return a random datetime between two datetime
    objects.
    """
    delta = end - start
    int_delta = (delta.days * 24 * 60 * 60) + delta.seconds
    random_second = randrange(int_delta)
    gen_date = start + timedelta(seconds=random_second)
    return gen_date.strftime('%Y-%m-%d')


def query_fund_cap(wind_code, query_day):
    sql = text("""
            select * from fund_transaction ft 
            inner join (
              select max(id) ft_id from fund_transaction ft 
              inner join(
                select wind_code_s, max(confirm_date) confirm_date_min 
                from fund_transaction ft 
                where wind_code= :wind_code and confirm_date<= :query_day 
                group by wind_code_s) fg 
              on ft.wind_code_s = fg.wind_code_s and ft.confirm_date = fg.confirm_date_min 
            group by ft.wind_code_s ) fg 
            on ft.id = fg.ft_id
            where ft.total_share > 0
            """)
    result = db.session.execute(sql, {"wind_code": wind_code, "query_day": query_day})
    tr_list = []
    for i in result:
        tr = dict(i)
        batch = FUND_ESSENTIAL.query.filter_by(wind_code_s=i['wind_code_s']).first()
        primary = FoFModel.query.filter_by(wind_code=batch.wind_code).first()

        nav = FUND_NAV.query.filter(and_(FUND_NAV.wind_code == primary.wind_code, FUND_NAV.nav_date <= query_day))\
            .order_by(FUND_NAV.nav_date.desc()).first()
        tr['f_code'] = primary.wind_code
        tr['f_name'] = primary.sec_name
        tr['cap'] = i['total_share'] * nav.nav
        tr['total_share'] = i['total_share']
        tr['nav'] = nav.nav
        tr_list.append(tr)
    cap_dict = {}
    for x in tr_list:
        if x['f_code'] in cap_dict:
            cap_dict[x['f_code']][0] = cap_dict[x['f_code']][0] + x['cap']
            cap_dict[x['f_code']][1] = cap_dict[x['f_code']][1] + x['total_share']
        else:
            cap_dict[x['f_code']] = [x['cap'], x['total_share'], x['f_name'], x['nav']]
    cap_list = [{"code": k, "cap": v[0], "total_share":v[1], "name": v[2], "nav":v[3]} for k, v in cap_dict.items()]
    return cap_list


if __name__ == "__main__":
    import os
    from fof_app import create_app
    env = os.environ.get('APP_ENV', 'dev')
    flask_app = create_app('fof_app.config.%sConfig' % env.capitalize())
    with flask_app.test_request_context():
        db.init_app(flask_app)
        print(query_fund_cap("FHF-101601","2017-11-22"))

