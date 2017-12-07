# -*- coding: utf-8 -*-
"""
:copyright: (c) 2016 by Huang Dong 
:mail: bjdhuang@cn.ibm.com.
:license: Apache 2.0, see LICENSE for more details.

"""
import datetime, json, logging, math, os, time
from os import path
from urllib.parse import quote
import numpy as np
from flask import render_template, Blueprint, redirect, url_for, abort, request, current_app, \
    jsonify, send_file, make_response, session,flash
from flask_login import login_required, current_user
from flask_sqlalchemy import get_debug_queries
from pandas import DataFrame
from sqlalchemy import and_, exc, or_
from analysis.portfolio_optim_fund import calc_portfolio_optim_fund as c4
from analysis.portfolio_optim_strategy import calc_portfolio_optim_fund
from backend import upload_file, data_handler, fund_nav_import_csv
from backend.tools import fund_owner, chunks, get_Value, range_years, check_code_order, \
    get_stress_data, get_stg,child_charts,get_core_info
from config_fh import get_redis, STRATEGY_EN_CN_DIC, JSON_DB, get_db_engine
from fof_app.models import db, FoFModel, FUND_STG_PCT, FOF_FUND_PCT, FileType, FundFile, FUND_NAV, \
    strategy_index_val, FUND_EVENT, FUND_ESSENTIAL, code_get_name, get_all_fof, PCT_SCHEME, INFO_SCHEME, UserModel, \
    Invest_corp, query_invest, Invest_corp_file,FUND_NAV_CALC,Fund_Core_Info
from fof_app.tasks import run_scheme_testing
from periodic_task.build_strategy_index import get_strategy_index_quantile
from fof_app.extensions import permission, cache
from periodic_task.build_strategy_index import calc_index_by_wind_code_dic
from backend.Datatables import DataTablesServer
from backend.fund_nav_calc import calc_fof_nav
from config_fh import get_db_session
from backend.market_report import gen_report
from backend.fund_nav_import_csv import  check_fund_nav_multi,import_fund_nav_multi


logger = logging.getLogger()

f_app_blueprint = Blueprint(
    'f_app', __name__, template_folder=path.join(path.pardir, 'templates', 'f_app', ), url_prefix='/f_app')

STG_TYPE = ['股票多头策略', '股票多空策略', '事件驱动策略', '其他股票策略', '阿尔法策略', '债券策略',
            '货币市场策略', '管理期货策略', '套利策略', '宏观策略', '组合基金策略', '现金管理']

ALLOWED_EXTENSIONS = ['pdf', 'xls', 'xlsx', 'csv', 'doc', 'docx', 'jpg', 'tif', 'ppt', 'pptx', 'csv']


@f_app_blueprint.before_app_request
def before_request():
    """
    hook 每一个新的请求前执行

    :return:
    """
    if current_user.is_authenticated:
        fof_list = cache.get(str(current_user.id))
        if fof_list is None:
            fof_list = get_all_fof()
            if fof_list is None:
                abort(409)
            else:
                logger.info("缓存用户{}可访问基金列表".format(current_user.username))
                cache.set(key=str(current_user.id), value=fof_list)


@f_app_blueprint.after_app_request
def after_request(response):
    """
    每个response返回后执行,慢查询记录
    :param response:
    :by hdhuang
    :return:
    """
    for query in get_debug_queries():
        if query.duration >= current_app.config['APP_SLOW_DB_QUERY_TIME']:
            logger.warning(
                'Slow query: % s\nParameters: % s\nDuration: % fs\nContext: % s\n' % (
                    query.statement, query.parameters, query.duration, query.context))
    return response


@f_app_blueprint.route('/')
@login_required
def home() -> object:
    """
    主页,用户可以管理的母基金以及子基金列表，包含最新净值和净值日期
    :by hdhuang
    :return:
    """
    fof_list = cache.get(str(current_user.id))
    return render_template("home.html", fof_list=fof_list)


@f_app_blueprint.route('/details')
@f_app_blueprint.route('/details/<string:wind_code>')
@login_required
@permission
@fund_owner
def details(wind_code: str) -> object:
    """
    基金的详细情况，包含基金的策略,子基金，批次，调仓历史，净值，资产，压力等,需要优化
    :param wind_code: 基金代码
    :by hdhuang
    :return:
    """
    logger.info('{}访问{}的详细情况'.format(current_user.username, wind_code))
    fof_list = cache.get(str(current_user.id))
    fof = check_code_order(wind_code)
    child_fof = FOF_FUND_PCT.query.filter_by(wind_code_p=wind_code).all()
    child = []
    for i in child_fof:
        data = {}
        data['name'] = code_get_name(i.wind_code_s)
        data['date'] = i.date_adj.strftime("%Y-%m-%d")
        data['scale'] = i.invest_scale
        data['code'] = i.wind_code_s
        child.append(data)
    file_json = []
    file_type = [i.type_name for i in current_user.files]
    logger.info("{}可管理的文件类型{}".format(current_user.username, file_type))
    if len(file_type) > 0:
        for i in file_type:
            fund_file = FundFile.query.filter(and_(FundFile.type_name == i, FundFile.wind_code == wind_code)).all()
            file_json.extend(fund_file)
        file_json = [{"date": i.upload_datetime.strftime("%Y-%m-%d %H:%M:%S"), "type": i.type_name, "name": i.show_name,
                      "action":i.id} for i in file_json]
    else:
        logger.info("{}暂时没有找到相关文件".format(wind_code))
        file_json = None
    full_year = datetime.datetime.now() - datetime.timedelta(days=365)
    ret_dic = data_handler.get_fof_nav_between(wind_code, full_year.strftime("%Y-%m-%d"),
                                               datetime.date.today().strftime('%Y-%m-%d'))
    if ret_dic is not None:
        pct_df, date_latest = ret_dic['fund_df'], ret_dic['date_latest']
        last_row = pct_df.iloc[[-1]]
        last_row.fillna(value="净值未更新", inplace=True)
        last_dict = last_row.T.to_dict()
        handler_fund = []
        for i in fof_list:
            if i.get('child'):
                handler_fund.extend([x['code'] for x in i['child']])
            else:
                primary_fund = i['primary']
                handler_fund.append(primary_fund.wind_code)
        format_float = lambda v: "%.3f" %v if isinstance(v,float) else v
        latest_nav = {k: format_float(v) for i in last_dict.values() for k, v in i.items() if k in handler_fund}
        result = [{"name": i, "data": np.array(pct_df[i]).tolist()} for i in pct_df.columns]
        result = [i if i['name'] == '000300.SH' else {'name': code_get_name(i['name']), 'data': i['data']} for i in
                  result]
        rr_html = ["<i class='col-md-4 col-sm-4 col-xs-4 col-lg-4'>%s &nbsp; %s</i>" % (code_get_name(k), v) for k, v in
                   latest_nav.items()]
        rr_chunk = [rr_html[i:i + 3] for i in range(0, len(rr_html), 3)]
        time_line = pct_df.index
        time_line = [i.strftime('%Y-%m-%d') for i in time_line]
        data_name = [i['name'] for i in result]
    else:
        time_line = []
        result = ""
        data_name = ""
        rr_chunk = ""
        date_latest = ""
    acc_data = data_handler.get_fund_nav_by_wind_code(wind_code)
    if acc_data is not None:
        acc_data.reset_index(inplace=True)
        acc_data = acc_data.to_dict(orient='records')
        acc = [{"nav_acc": "%0.4f" % i['nav_acc'], "pct": "%0.4f" % i['pct'],
                "nav_date": i['nav_date'].strftime('%Y-%m-%d'), "nav": "%0.4f" % i['nav']} for i in acc_data]
    else:
        logger.info("{}暂时没有净值".format(wind_code))
        acc = ""

    # child_charts

    fhs_obj, copula_obj, multi_obj, capital_data = get_stress_data(wind_code)
    stg_obj = get_stg(wind_code)
    stg = stg_obj['stg']
    stg_charts = stg_obj['stg_charts']

    core_info = Fund_Core_Info.query.filter_by(wind_code=wind_code).first()

    nav_df, nav_date_fund_scale_df = data_handler.get_fof_fund_pct_each_nav_date(wind_code)
    nav_obj = {} if nav_df is None else nav_df.to_dict()
    scale_obj = {} if nav_date_fund_scale_df is None else nav_date_fund_scale_df.to_dict()
    return render_template('details.html', fof=fof, child=child, stg=stg, fund_file=file_json,
                           time_line=time_line, result=result, data_name=data_name, fund_rr=rr_chunk
                           , date_latest=date_latest, acc=acc, fof_list=fof_list,
                           stg_charts=stg_charts,
                           fhs_obj=fhs_obj, copula_obj=copula_obj, multi_obj=multi_obj,
                           capital_data=capital_data,core_info=core_info,nav_obj=nav_obj,scale_obj=scale_obj) 
                           

@f_app_blueprint.route('/get_child_charts',methods=['POST','GETS'])
def get_child_charts():
    if request.method == 'POST':
        wind_code = request.json['wind_code']
        display_type = request.json['disPlayType']
        result = child_charts(wind_code,display_type)
        return jsonify(status='ok',data=result)


@f_app_blueprint.route('/download_main_charts')
def download_main_charts():
    logger.info("导出数据")
    data = request.args.to_dict()
    wind_code = data['wind_code']
    full_year = datetime.datetime.now() - datetime.timedelta(days=365)
    ret_dic = data_handler.get_fof_nav_between(wind_code, full_year.strftime("%Y-%m-%d"),
                                               datetime.date.today().strftime('%Y-%m-%d'))
    df = ret_dic['fund_df']
    file_name = wind_code+"净值数据"+".csv"
    df.to_csv()
    corp_path = current_app.config['CORP_FOLDER']
    corp_file_path = path.join(corp_path, file_name)
    df.to_csv(corp_file_path)
    response = make_response(send_file(corp_file_path, as_attachment=True, attachment_filename=file_name))
    basename = os.path.basename(file_name)
    response.headers["Content-Disposition"] = \
        "attachment;" \
        "filename*=UTF-8''{utf_filename}".format(
            utf_filename=quote(basename.encode('utf-8'))
        )
    os.remove(corp_file_path)
    return response
@f_app_blueprint.route('/edit_summary/<string:wind_code>', methods=['POST', 'GET'])
@login_required
@permission
@fund_owner
def edit_summary(wind_code: str) -> object:
    """
    编辑基金的基础信息  需要优化，计划使用flask-bootstrap和wtf生成表单
    :param wind_code:基金代码
    :by hdhuang
    :return:
    """
    if request.method == 'GET':
        fof = check_code_order(wind_code)
        fof_list = cache.get(str(current_user.id))
        return render_template('edit_summary.html',fof_list=fof_list,fof=fof.to_json())
    elif request.method == 'POST':
        data = request.form.to_dict()
        fund = FoFModel.query.filter_by(wind_code=data['wind_code']).first()
        for k,v in data.items():
            if len(v) == 0:
                v = None
            setattr(fund,k,v)
        db.session.commit()
        return redirect(url_for("f_app.details",wind_code=wind_code))


@f_app_blueprint.route('/add_child/<string:wind_code>', methods=['POST', 'GET'])
@login_required
@permission
@fund_owner
def add_child(wind_code: str) -> object:
    """
    持仓记录修改，子基金修改
    :param wind_code: 基金代码
    :by hdhuang
    :return:
    """
    if request.method == 'GET':
        fof_list = cache.get(str(current_user.id))
        fof = check_code_order(wind_code)
        query_child = FOF_FUND_PCT.query.filter_by(wind_code_p=wind_code).first()
        if query_child is not None:
            latest_date = FOF_FUND_PCT.query.filter_by(wind_code_p=wind_code).order_by(
                FOF_FUND_PCT.date_adj.desc()).first().date_adj
            child_fof = FOF_FUND_PCT.query.filter(
                and_(FOF_FUND_PCT.wind_code_p == wind_code, FOF_FUND_PCT.date_adj == latest_date)).all()
            child = []
            for i in child_fof:
                data = {}
                data['name'] = code_get_name(i.wind_code_s)
                data['date'] = i.date_adj.strftime("%Y-%m-%d")
                data['scale'] = i.invest_scale
                data['code'] = i.wind_code_s
                child.append(data)
        else:
            child = []
        return render_template("add_child.html", wind_code=wind_code, child=child, fof=fof, fof_list=fof_list)
    if request.method == 'POST':
        change_date = request.form.get('change_date')
        child_fof = FOF_FUND_PCT.query.filter(
            and_(FOF_FUND_PCT.wind_code_p == wind_code, FOF_FUND_PCT.date_adj == change_date)).all()
        if len(child_fof) >= 1:
            for i in child_fof:
                db.session.delete(i)
                db.session.commit()
        code_list = request.form.getlist('code')
        scale_list = request.form.getlist('scale')
        logger.info("用户{}更新持仓记录".format(current_user.username))
        for i in zip(code_list, scale_list):
            child = FOF_FUND_PCT(wind_code_p=wind_code, wind_code_s=i[0], date_adj=change_date,
                                 invest_scale=i[1])
            db.session.add(child)
            db.session.commit()
            logger.info("wind_code_p {} wind_code_s {} invest_scale {}".format(wind_code,i[0],i[1]))
        logger.info("用户{}更新持仓记录成功".format(current_user.username))
        fof_list = get_all_fof()
        logger.info("更新缓存用户{}可访问基金列表".format(current_user.username))
        cache.set(key=str(current_user.id), value=fof_list)
        data_handler.update_fof_stg_pct(wind_code)
        return redirect(url_for("f_app.details", wind_code=wind_code))


@f_app_blueprint.route('/get_child_status', methods=['GET', 'POST'])
@login_required
def get_child_status():
    """
    在添加子基金界面选择日期面板可以查到所有的交易日期标签,缺少js插件,功能暂时没有实现
    :by hdhuang
    :return:
    """
    wind_code = request.json['wind_code']
    last_date = request.json['date']
    child_fof = FOF_FUND_PCT.query.filter(
        and_(FOF_FUND_PCT.wind_code_p == wind_code, FOF_FUND_PCT.date_adj == last_date)).all()
    child = []
    for i in child_fof:
        data = {}
        data['name'] = code_get_name(i.wind_code_s)
        data['date'] = i.date_adj.strftime("%Y-%m-%d")
        data['scale'] = i.invest_scale
        data['code'] = i.wind_code_s
        child.append(data)
    return json.dumps({"data": child, 'status': 'ok'})


@f_app_blueprint.route('/get_fof_mapping')
@login_required
def get_fof_mapping() -> object:
    """
    获取所有的批次和基金
    :by hdhuang
    :return:
    """
    q = request.args.get('q')
    logger.info("关键字-----{}".format(q))
    fof_mapping  = FUND_ESSENTIAL.query.whoosh_search(q, like=True).all()
    fof_model = FoFModel.query.whoosh_search(q, like=True).all()
    if len(fof_mapping) ==  0:
        logger.warning("没有查询到关键字{}".format(q))
        return jsonify(status='error')
    else:

        data = [{"wind_code": i.wind_code_s, "sec_name": i.sec_name_s,"index":index} for index,i in enumerate(fof_mapping)]
        # else:
        #     mapping_data = []
        # if len(fof_model) >  0:
        #     fund_data = [{"wind_code": i.wind_code, "sec_name": i.sec_name} for i in fof_model]
        # else:
        #     fund_data = []
        # data = [{"wind_code": i['wind_code'], "id": index, "sec_name": i['sec_name']} for index, i in enumerate(fund_data + mapping_data)]
        return jsonify(status='ok',items=data)


@f_app_blueprint.route('/get_fof')
@login_required
def get_fof() -> object:
    """
    添加子基金页面，过滤没有级别的基金
    :by hdhuang
    :return:
    """
    q = request.args.get('q')
    logger.info("关键字-----{}".format(q))
    fof = FoFModel.query.whoosh_search(q, like=True).filter(FoFModel.rank.isnot(None))
    data = [{"wind_code": i.wind_code, "sec_name": i.sec_name, 'id': index} for
            index, i in enumerate(fof)]
    return json.dumps({'items': data})


@f_app_blueprint.route('/mapping/checkName', methods=['POST', 'GET'])
@login_required
def checkName():
    """
    添加基金新批次,检查名称是否可用
    :by hdhuang
    :return:
    """
    m_name = request.args['sec_name_s']
    name_mapping = FUND_ESSENTIAL.query.filter_by(sec_name_s=m_name).first()
    if name_mapping is None:
        return jsonify(valid=True)
    else:
        return jsonify(valid=False)


@f_app_blueprint.route('/mapping/checkCode', methods=['POST', 'GET'])
@login_required
def checkCode():
    """
    添加基金新批次,检查批次代码是否可用
    :by hdhuang
    :return:
    """
    m_code = request.args['wind_code_s']
    code_mapping = FUND_ESSENTIAL.query.filter_by(wind_code_s=m_code).first()
    if code_mapping is None:
        return jsonify(valid=True)
    else:
        return jsonify(valid=False)


@f_app_blueprint.route('/add_mapping', methods=['POST', 'GET'])
@login_required
def add_mapping():
    """
    使用表单添加一个新的批次
    :by hdhuang
    :return:
    """
    data = request.form.to_dict()
    new_mapping = FUND_ESSENTIAL(**data)
    db.session.add(new_mapping)
    db.session.commit()
    return jsonify(status="ok")


@f_app_blueprint.route('/add_stg/<string:wind_code>', methods=['POST', 'GET'])
@login_required
@permission
@fund_owner
def add_stg(wind_code: str) -> object:
    """
    添加新的策略
    :param wind_code:基金代码
    :by hdhuang
    :return:
    """
    if request.method == 'GET':
        fof_list = cache.get(str(current_user.id))
        fof = check_code_order(wind_code)
        stg_result = FUND_STG_PCT.query.filter_by(wind_code=wind_code).all()
        if len(stg_result) > 0:
            last_date = FUND_STG_PCT.query.filter_by(wind_code=wind_code).order_by(
                FUND_STG_PCT.trade_date.desc()).first().trade_date
            stg_data = FUND_STG_PCT.query.filter(
                and_(FUND_STG_PCT.wind_code == wind_code, FUND_STG_PCT.trade_date == last_date)).all()
        else:
            stg_data = ""
        return render_template("add_stg.html", wind_code=wind_code, stg_type=STG_TYPE, stg_result=stg_data, fof=fof,
                               fof_list=fof_list)

    if request.method == 'POST':
        change_date = request.form.get('change_date')
        stg_r = FUND_STG_PCT.query.filter(
            and_(FUND_STG_PCT.wind_code == wind_code, FUND_STG_PCT.trade_date == change_date)).all()
        if len(stg_r) >= 1:
            for i in stg_r:
                db.session.delete(i)
                db.session.commit()
        stg_list = request.form.getlist('stg')
        scale_list = request.form.getlist('scale')
        for i in zip(stg_list, scale_list):
            stg = FUND_STG_PCT(wind_code=wind_code, stg_code=i[0], trade_date=change_date, stg_pct=i[1])
            db.session.add(stg)
            db.session.commit()

        return redirect(url_for("f_app.details", wind_code=wind_code))


@f_app_blueprint.route('/fof_upload/<string:wind_code>', methods=['POST', 'GET'])
@login_required
@permission
@fund_owner
def fof_upload(wind_code: str) -> object:
    """
    上传文件表单页面
    :param wind_code:基金代码
    :by hdhuang
    :return:
    """
    if request.method == "GET":
        fof_list = cache.get(str(current_user.id))
        fof = check_code_order(wind_code)
        if fof is not None:
            file_type = FileType.query.all()
            file_type = [i.type_name for i in file_type]
            return render_template("fof_upload.html", wind_code=wind_code, file_type=file_type, fof=fof,
                                   fof_list=fof_list)
        else:
            abort(404)


@f_app_blueprint.route('/upload/<string:wind_code>', methods=['POST', 'GET'])
@login_required
def upload(wind_code: str) -> object:
    """
    jquery upload file 插件上传文件
    :param wind_code: 基金代码
    :by hdhuang
    :return:
    """
    if request.method == 'POST':
        if 'file[]' not in request.files:
            return redirect(request.url)
        file = request.files['file[]']
        file_type = dict(request.form)
        file_type = file_type['type'][0]
        old_name = file.filename
        mime_type = file.content_type
        if file and allowed_file(file.filename):
            result = upload_file.uploadfile(name=old_name, type=mime_type, size='*')
            content = file.read()
            fof_file = FundFile(wind_code=wind_code,show_name=old_name,type_name=file_type,upload_datetime=datetime.datetime.now(),file_content=content)
            db.session.add(fof_file)
            db.session.commit()
            return json.dumps({"files": [result.get_file()]})
    if request.method == 'GET':
        files = [f.show_name for f in FundFile.query.all]
        file_display = []
        for f in files:
            size = '*'
            file_saved = upload_file.uploadfile(name=f, size=size)
            file_display.append(file_saved.get_file())
        return json.dumps({"files": file_display})




@f_app_blueprint.route('/read_file/')
@login_required
@permission
def download_file():
    """
    下载文件并返回原始文件名称
    :by hdhuang
    :return:
    """

    post_data = request.args.to_dict()
    fid = post_data['id']
    fund_file = FundFile.query.get(fid)
    file_name = fund_file.show_name
    fund_path = current_app.config['UPLOADS']
    fund_file_path = path.join(fund_path,file_name)
    with open(fund_file_path,'wb') as f:
        f.write(fund_file.file_content)
    response = make_response(send_file(fund_file_path, as_attachment=True, attachment_filename=file_name))
    basename = os.path.basename(file_name)
    response.headers["Content-Disposition"] = \
        "attachment;" \
        "filename*=UTF-8''{utf_filename}".format(
            utf_filename=quote(basename.encode('utf-8'))
        )
    os.remove(fund_file_path)
    return response


@f_app_blueprint.route('/del_file', methods=['POST', 'GET'])
@login_required
@permission
def del_file():
    """
    删除文件
    :by hdhuang
    :return:
    """
    fid = request.json['fid']
    fund_file = FundFile.query.get(fid)
    logger.warning("用户{}删除文件ID {} 文件名　{}".format(current_user.username,fid,fund_file.show_name))
    db.session.delete(fund_file)
    db.session.commit()
    logger.warning("用户{}删除文件ID {}　成功".format(current_user.username,fid))
    return json.dumps({"status": "ok"})


@f_app_blueprint.route('/maintain_acc')
@login_required
@permission
def maintain_acc():
    fof_list = cache.get(str(current_user.id))
    fund_list = set()
    primary_list = []
    for i in fof_list:
        primary_list.append(i['primary'])
        for x in i['child']:
            batch = FUND_ESSENTIAL.query.filter_by(wind_code_s=x['code']).first()
            if batch is None:
                fund = FoFModel.query.filter_by(wind_code=x['code']).first()
            else:
                fund = FoFModel.query.filter_by(wind_code=batch.wind_code).first()
            fund_list.add(fund)
    fund_list = [ i.to_json() for i in fund_list]
    return render_template('maintain_acc.html',fof_list=fof_list,fund_list=fund_list,primary_list=primary_list)



@f_app_blueprint.route("/show_acc/<string:wind_code>", methods=['POST', 'GET'])
@login_required
def show_acc(wind_code):
    """
     基金的所有净值记录
    :param wind_code:基金代码
    :by hdhuang
    :return:
    """
    acc_data = data_handler.get_fund_nav_by_wind_code(wind_code, limit=0)
    if acc_data is not None:
        acc_data.reset_index(inplace=True)
        acc_data = acc_data.to_dict(orient='records')
        acc = [{"nav_acc": "%0.4f" % i['nav_acc'], "pct": "%0.4f" % i['pct'],
                "nav_date": i['nav_date'].strftime('%Y-%m-%d'), "nav": "%0.4f" % i['nav']} for i in acc_data]
        return json.dumps({"data": acc})
    else:
        return jsonify(data="")


@f_app_blueprint.route("/del_acc", methods=['POST', 'GET'])
@login_required
def del_acc():
    """
    删除净值记录
    :by hdhuang
    :return:
    """
    nav_date = request.json['nav_date']
    wind_code = request.json['wind_code']
    acc_record = FUND_NAV.query.filter(and_(FUND_NAV.wind_code == wind_code),
                                       (FUND_NAV.nav_date == nav_date)).first()
    db.session.delete(acc_record)
    db.session.commit()
    del_nav_str = "call proc_delete_fund_nav_by_wind_code(:wind_code, :nav_date)"
    with get_db_session(get_db_engine()) as session:
        logger.info("开始执行存储过程")
        session.execute(del_nav_str, {'wind_code': wind_code,'nav_date':nav_date})
    fof_list = get_all_fof()
    cache.set(key=str(current_user.id), value=fof_list)
    logger.info("用户{}基金列表缓存已更新".format(current_user.username))
    return jsonify(status='ok')


@f_app_blueprint.route('/add_acc', methods=['POST', 'GET'])
def add_acc():
    """
    添加一条净值记录"
    :by hdhuang
    :return:
    """
    post_data = request.json

    acc_record = FUND_NAV.query.filter(and_(FUND_NAV.wind_code == post_data['wind_code']),
                                       (FUND_NAV.nav_date == post_data['nav_date'])).first()
    if acc_record is None:

        acc_record = FUND_NAV(wind_code=request.json['wind_code'], nav_date=request.json['nav_date'],
                              nav=request.json['nav'],
                              nav_acc=request.json['nav_acc'], source_mark=1)
        db.session.add(acc_record)
        db.session.commit()
        sql_str = "call proc_update_fund_info_by_wind_code2(:wind_code, :force_update)"
        replace_nav_str = "call proc_replace_fund_nav_by_wind_code(:wind_code, :nav_date,:force_update)"
        with get_db_session(get_db_engine()) as session:
            logger.info("开始执行存储过程")
            session.execute(sql_str, {'wind_code': request.json['wind_code'], 'force_update': True})
            session.execute(replace_nav_str,{'wind_code':request.json['wind_code'],'nav_date':request.json['nav_date'],'force_update': True})
            fof_list = get_all_fof()
            cache.set(key=str(current_user.id), value=fof_list)
            logger.info("用户{}基金列表缓存已更新".format(current_user.username))
        return jsonify(acc="add")
    else:
        logger.error("这条记录的净值日期已经存在{} {}".format(request.json['wind_code'], request.json['nav_date']))
        post_data = request.json
        acc_record = FUND_NAV.query.filter(and_(FUND_NAV.wind_code == post_data['wind_code']),
                                           (FUND_NAV.nav_date == post_data['nav_date'])).first()
        acc_record.nav_acc = post_data['nav_acc']
        acc_record.nav = post_data['nav']
        db.session.commit()
        sql_str = "call proc_update_fund_info_by_wind_code2(:wind_code, :force_update)"
        replace_nav_str = "call proc_replace_fund_nav_by_wind_code(:wind_code, :nav_date,:force_update)"

        with get_db_session(get_db_engine()) as session:
            logger.info("开始执行存储过程")
            session.execute(sql_str, {'wind_code': request.json['wind_code'], 'force_update': True})
            session.execute(replace_nav_str, {'wind_code': request.json['wind_code'],
                                              'nav_date': request.json['nav_date'], 'force_update': True})
        return jsonify(acc="edit")





@f_app_blueprint.route('/change_acc', methods=['POST', 'GET'])
@login_required
@permission
def change_acc():
    """
    加载修改净值页面
    :param wind_code 基金代码
    :by hdhuang
    :return:
    """
    if request.method == 'GET':
        fof_list = cache.get(str(current_user.id))
        return render_template("change_acc.html", fof_list=fof_list)


@f_app_blueprint.route('/query_acc', methods=['POST', 'GET'])
@login_required
def query_acc():
    """
    查询净值记录,已废弃
    :return:
    """
    if request.method == "POST":
        post_data = request.json
        acc = FUND_NAV.query.filter(and_(FUND_NAV.wind_code == post_data['wind_code']),
                                    (FUND_NAV.nav_date == post_data['date'])).first()
        if acc is not None:
            return jsonify(status="ok", acc=acc.nav, nav_acc=acc.nav_acc)
        else:
            return jsonify(status="error")


@f_app_blueprint.route('/upload_acc/<string:wind_code>', methods=['POST', 'GET'])
@login_required
def upload_acc(wind_code):
    """
    使用上传文件方式更新净值
    :param wind_code 基金代码
    :by hdhuang
    :return:
    """
    if request.method == 'POST':
        if 'file[]' not in request.files:
            return redirect(request.url)
        file = request.files['file[]']
        if file.filename == '':
            return redirect(request.url)
        filename = file.filename
        fof = FoFModel.query.filter_by(wind_code=wind_code).first()
        if fof is None:
            fof_mapping = FUND_ESSENTIAL.query.filter_by(wind_code_s=wind_code).first()
            fof_name = set(fof_mapping.sec_name_s)
        else:
            fof_name = set(fof.sec_name)
        old_name = set(filename)
        fof_name = set(fof_name)
        acc_path = current_app.config['ACC_FOLDER']
        if len(old_name & fof_name) >= 2:
            if file and allowed_file(file.filename):
                if path.exists(acc_path):
                    pass
                else:
                    os.mkdir(acc_path)
                filename = file.filename
                file_path = path.join(current_app.config['ACC_FOLDER'], filename)
                file.save(file_path)
                fund_nav_import_csv.update_fundnav_by_file(wind_code=wind_code, file_path=file_path)
                fof_list = get_all_fof()
                cache.set(key=str(current_user.id), value=fof_list)
                return json.dumps({"files": [{"message": "净值已更新"}]})
        else:
            return json.dumps({"files": [{"error": "源文件格式错误,请检查"}]})
    if request.method == 'GET':
        return json.dumps({"files": [{"message": "净值已更新"}]})


@f_app_blueprint.route('/asset_details/<string:wind_code>',methods=['GET','POST'])
@login_required
def asset_details(wind_code):
    if request.method == 'GET':
        fof_list = cache.get(str(current_user.id))
        return render_template('asset_details.html', fof_list=fof_list,wind_code=wind_code)



@f_app_blueprint.route("/show_batch_asset/<string:wind_code>", methods=['POST', 'GET'])
@login_required
def show_batch_asset(wind_code):
    """
     基金的所有净值记录
    :param wind_code:基金代码
    :by hdhuang
    :return:
    """
    fof_list = cache.get(str(current_user.id))
    asset_list = [{"child": [x for x in i['child']]}
                  for i in fof_list if wind_code == i['primary'].wind_code]


    if len(asset_list) > 0:
        batch_data = []
        for i in asset_list:
            for c in i['child']:
                batch_calc = FUND_NAV_CALC.query.filter_by(wind_code=c['code']).all()
                for b in batch_calc:
                    batch_dict = b.as_dict()
                    batch_dict['name'] = c['name']
                    batch_dict['nav_date'] = batch_dict['nav_date'].strftime('%Y-%m-%d')
                    batch_data.append(batch_dict)
        return jsonify(status='ok',data=batch_data)
    else:
        return jsonify(status='error')


@f_app_blueprint.route('/show_primary_asset/<string:wind_code>',methods=['GET','POST'])
def show_primary_asset(wind_code):
    '''
    查询母基金的全部资产,使用马老师的存储过程返回一个日期
    :param wind_code: 母基金代码
    :return: 所有的资产信息 select * from fund_nav_calc where wind_code = wind_code;
    '''
    next_nav_date_str = "select func_get_next_nav_date(:wind_code)"
    with get_db_session(get_db_engine()) as session:
        sql_return = session.execute(next_nav_date_str, {'wind_code': wind_code})
        tag_date = sql_return.first()[0]
        if tag_date is not None:
            next_date_nav = calc_fof_nav(wind_code,tag_date)
            next_date_nav['db'] = False
            primary_fund = FUND_NAV_CALC.query.filter_by(wind_code=wind_code).all()
            primary_fund = [dict(i.as_dict(),**{"db":True}) for i in primary_fund]
            primary_fund.append(next_date_nav)
            primary_fund = list(map(lambda x:{k:v if k !='nav_date' else v.strftime('%Y-%m-%d') for k,v in x.items()},primary_fund))
            return jsonify(status='ok',data=primary_fund)
        else:
            return jsonify(status='error')


@f_app_blueprint.route('/confirm_asset/<string:wind_code>',methods=['GET','POST'])
def confirm_asset(wind_code):
    """
    资产表确认按钮提交的数据,获取前端提交的数据中的日期,首先在fund info表中查询基金代码和成立时间 True 新基金;
    在trade_date获取提交日期的上一个工作日，检查fund nav calc表中是否存在上一个交易日的数据 True
    :param wind_code:
    :return:
    """
    if request.method == 'POST':
        post_data = request.json
        if post_data['check']:
            return jsonify(status='ok')
        else:
            del post_data['db'],post_data['check']
            post_data['wind_code'] = wind_code
            new_calc_record = FUND_NAV_CALC(**post_data)

            # print(post_data)

            acc_record = FUND_NAV(wind_code=post_data['wind_code'], nav_date=post_data['nav_date'],
                                  nav=post_data['nav'],
                                  nav_acc=post_data['nav'], source_mark=3)
            db.session.add(new_calc_record)
            db.session.add(acc_record)
            #return jsonify(status='ok')
            try:
                db.session.commit()
                sql_str = "call proc_update_fund_info_by_wind_code2(:wind_code, :force_update)"
                replace_nav_str = "call proc_replace_fund_nav_by_wind_code(:wind_code, :nav_date,:force_update)"
                with get_db_session(get_db_engine()) as session:
                    logger.info("开始执行存储过程")
                    session.execute(sql_str, {'wind_code': post_data['wind_code'], 'force_update': True})
                    session.execute(replace_nav_str,
                                    {'wind_code': post_data['wind_code'], 'nav_date': post_data['nav_date'],'force_update':False})
                    fof_list = get_all_fof()
                    cache.set(key=str(current_user.id), value=fof_list)
                    logger.info("用户{}基金列表缓存已更新".format(current_user.username))
                return jsonify(status='ok')
            except exc.IntegrityError:
                logger.error("这条记录的净值日期已经存在{} {}".format(post_data['wind_code'], post_data['nav_date']))
                return jsonify(status='error')



@f_app_blueprint.route('/calendar/<string:wind_code>', methods=['GET', 'POST'])
@login_required
@fund_owner
def calendar(wind_code):
    """
    根据基金代码获取相关的日程
    :param wind_code: 基金代码
    :by hdhuang
    :return:
    """
    if request.method == "GET":
        fof_list = cache.get(str(current_user.id))
        fof = check_code_order(wind_code)
        return render_template("calendar.html", wind_code=wind_code, fof=fof, fof_list=fof_list)
    if request.method == "POST":
        return json.dumps({"status": "ok"})


@f_app_blueprint.route('/query_cal/<string:wind_code>/', methods=['GET', 'POST'])
@login_required
def query_cal(wind_code):
    """
    根据基金代码获取相关的日程
    :param wind_code: 基金代码
    :by hdhuang
    :return:
    """
    start_unix = request.args['start']
    end_unix = request.args['end']
    start = datetime.datetime.fromtimestamp(float(start_unix)).strftime('%Y-%m-%d')
    end = datetime.datetime.fromtimestamp(float(end_unix)).strftime('%Y-%m-%d')
    events = FUND_EVENT.query.filter(and_(FUND_EVENT.wind_code == wind_code,
                                          or_(FUND_EVENT.create_date.between(start, end),
                                              FUND_EVENT.remind_date.between(start, end),
                                              FUND_EVENT.event_date.between(start, end)))).all()
    if len(events) > 0:
        events_list = [{"title": i.event_type, "start": i.remind_date.strftime('%Y-%m-%d'),
                        'end': i.event_date.strftime('%Y-%m-%d'), 'create': i.create_date.strftime('%Y-%m-%d'),
                        "desc": i.description, "id": i.id, "color": i.color, 'Private': i.private,
                        "user": i.create_user, "handle": i.handle_status} for i in events]
        key = ['create', 'start', 'end']
        el = []
        for i in events_list:
            if i['user'] != current_user.username and i['Private'] == True:
                pass
            else:
                for x in key:
                    eo = {}
                    if x == 'create':
                        eo['tag'] = "<i class='fa fa-battery-0'></i>"
                    elif x == "start":
                        eo['tag'] = "<i class='fa fa-battery-2'></i>"
                    elif x == "end":
                        eo['tag'] = "<i class='fa fa-battery-full'></i>"
                    eo['start'] = i[x]
                    eo['id'] = i['id']
                    eo['title'] = i['title']
                    eo['desc'] = i['desc']
                    eo['color'] = i['color']
                    eo['start_time'] = i['start']
                    eo['create_time'] = i['create']
                    eo['end_time'] = i['end']
                    eo['allDay'] = False
                    eo['Private'] = i['Private']
                    eo['handle'] = i['handle']
                    if i['user'] != current_user.username:
                        eo['user'] = i['user']
                    el.append(eo)

        return json.dumps(el)
    else:
        return jsonify(status="empty")


@f_app_blueprint.route('/edit_cal/<string:wind_code>', methods=['GET', 'POST'])
@login_required
def edit_cal(wind_code):
    """
    使用表单为一直基金添加相应的事件
    :param wind_code: 基金代码
    :by hdhuang
    :return: string
    """
    if request.method == 'POST':
        event = FUND_EVENT.query.filter_by(id=request.form['id']).first()
        event.event_type = request.form['title']
        event.remind_date = request.form['start']
        event.description = request.form['description']
        event.color = request.form['color']
        event.wind_code = wind_code
        event.event_date = request.form['end']
        event.create_date = request.form['create']
        event.private = request.form['Private']
        event.create_user = current_user.username
        if request.form['Private'] == 'false':
            event.private = 0
        else:
            event.private = 1
        db.session.commit()
        return "ok"


@f_app_blueprint.route('/add_cal/<string:wind_code>', methods=['GET', 'POST'])
def add_cal(wind_code):
    """
    删除和基金相关的日程
    :param wind_code: 基金代码
    :by hdhuang
    :return: string
    """
    if request.method == 'POST':
        if request.form['Private'] == 'false':
            private = 0
        else:
            private = 1
        event = FUND_EVENT(wind_code=wind_code, event_date=request.form['end'], event_type=request.form['title'],
                           create_date=request.form['create'], remind_date=request.form['start'], private=private,
                           description=request.form['description'], color=request.form['color'],
                           create_user=current_user.username)
        db.session.add(event)
        db.session.commit()
        return "ok"


@f_app_blueprint.route('/del_cal', methods=['GET', 'POST'])
@login_required
def del_cal():
    """
    根据日程id删除对应的日程事件
    :by hdhuang
    :return: string
    """
    event = FUND_EVENT.query.get(request.args['id'])
    db.session.delete(event)
    db.session.commit()
    return "ok"


@f_app_blueprint.route('/all_cal', methods=['GET', 'POST'])
@login_required
def all_cal():
    """
    加载所有日程页面
    :by hdhuang
    :return:
    """
    fof_list = get_all_fof()
    return render_template('allcalendar.html', fof_list=fof_list)


@f_app_blueprint.route('/query_all_cal/')
@login_required
def query_all_cal():
    """
    和用户相关的所有日程
    :by hdhuang
    :return: json
    """
    start_unix = request.args['start']
    end_unix = request.args['end']
    start = datetime.datetime.fromtimestamp(float(start_unix)).strftime('%Y-%m-%d')
    end = datetime.datetime.fromtimestamp(float(end_unix)).strftime('%Y-%m-%d')
    fof_list = current_user.fofs
    events = FUND_EVENT.query.filter(and_(FUND_EVENT.wind_code.in_([i.wind_code for i in fof_list]),
                                          or_(FUND_EVENT.create_date.between(start, end),
                                              FUND_EVENT.remind_date.between(start, end),
                                              FUND_EVENT.event_date.between(start, end)))).all()
    if len(events) > 0:
        events_list = [{"title": i.event_type, "start": i.remind_date.strftime('%Y-%m-%d'),
                        'end': i.event_date.strftime('%Y-%m-%d'), 'create': i.create_date.strftime('%Y-%m-%d'),
                        "desc": i.description, "id": i.id, "color": i.color, 'Private': i.private,
                        "user": i.create_user, "handle": i.handle_status, "name": code_get_name(i.wind_code)} for i in
                       events]
        key = ['create', 'start', 'end']
        el = []
        for i in events_list:
            if i['user'] != current_user.username and i['Private'] == True:
                pass
            else:
                for x in key:
                    eo = {}
                    if x == 'create':
                        eo['tag'] = "<i class='fa fa-battery-0'></i>"
                    elif x == "start":
                        eo['tag'] = "<i class='fa fa-battery-2'></i>"
                    elif x == "end":
                        eo['tag'] = "<i class='fa fa-battery-full'></i>"
                    eo['start'] = i[x]
                    eo['id'] = i['id']
                    eo['title'] = i['title']
                    eo['desc'] = i['desc']
                    eo['color'] = i['color']
                    eo['start_time'] = i['start']
                    eo['create_time'] = i['create']
                    eo['end_time'] = i['end']
                    eo['allDay'] = False
                    eo['Private'] = i['Private']
                    eo['handle'] = i['handle']
                    eo['name'] = i['name']
                    if i['user'] != current_user.username:
                        eo['user'] = i['user']
                    el.append(eo)

        return json.dumps(el)
    else:
        return jsonify(status='ok')


@f_app_blueprint.route('/benchmark/<string:wind_code>', methods=['GET', 'POST'])
@login_required
@fund_owner
def benchmark(wind_code):
    """
    加载配置基金组合，可以动态配置生成组合的各种相信信息，比如策略，金额等信息
    :param wind_code: 基金代码
    :by hdhuang
    :return:
    """
    if request.method == 'GET':
        fof_list = cache.get(str(current_user.id))
        fof = FoFModel.query.filter_by(wind_code=wind_code).first()
        query_child = FOF_FUND_PCT.query.filter_by(wind_code_p=wind_code).first()
        if query_child is not None:
            last_date = FOF_FUND_PCT.query.filter_by(wind_code_p=wind_code).order_by(
                FOF_FUND_PCT.date_adj.desc()).first().date_adj
            child_fof = FOF_FUND_PCT.query.filter(
                and_(FOF_FUND_PCT.wind_code_p == wind_code, FOF_FUND_PCT.date_adj == last_date)).all()
            child = []
            for i in child_fof:
                data = {}
                data['name'] = code_get_name(i.wind_code_s)
                data['date'] = i.date_adj.strftime("%Y-%m-%d")
                data['scale'] = i.invest_scale
                data['code'] = i.wind_code_s
                child.append(data)
        else:
            child = []
        return render_template("benchmark.html", wind_code=wind_code, fof=fof, child=child, fof_list=fof_list)
    if request.method == 'POST':
        post_data = request.json
        stg_list = []
        code_value = []
        for x in post_data:
            result = {}
            c_code = FUND_ESSENTIAL.query.filter_by(wind_code_s=x[1]).first()
            if c_code is not None:
                result['wind_code'] = c_code.wind_code
            else:
                result['wind_code'] = x[1]
            result['value'] = x[2]
            code_value.append(result)
        for i in code_value:
            if i['wind_code'] != 'fh0000':
                last_date = FUND_STG_PCT.query.filter_by(wind_code=i['wind_code']).order_by(
                    FUND_STG_PCT.trade_date.desc()).first().trade_date
                stg = FUND_STG_PCT.query.filter(
                    and_(FUND_STG_PCT.wind_code == i['wind_code'], FUND_STG_PCT.trade_date == last_date)).all()
                stg_pct = [{"stg_name": x.stg_code, "stg_pct": x.stg_pct, "name": x.wind_code} for x in stg]
                stg_list.extend(stg_pct)
        cash = [{"stg_name": "未投现金", 'value': float(i['value']), "name": "fh0000"} for i in code_value if
                i['wind_code'] == 'fh0000']
        stg_pct = []
        for b in code_value:
            pct_temp = [
                {"stg_name": t['stg_name'], "value": float(b['value']) * (t['stg_pct'] / 100), "name": t['name']} for
                t in stg_list if t['name'] == b['wind_code']]
            stg_pct.extend(pct_temp)
        stg_pct.extend(cash)
        df = DataFrame(stg_pct)
        dx = df.groupby(['stg_name'])['value'].sum()
        stg_summary = [{"value": v, "name": k} for k, v in dx.items()]
        stg_name = {t['stg_name'] for t in stg_pct}
        stg_pie = []
        for i in stg_name:
            series = [{"name": code_get_name(x['name']), "value": x['value']} for x in stg_pct if x['stg_name'] == i]
            series_obj = {"name": i, "data": series}
            stg_pie.append(series_obj)
        return jsonify(key=list(stg_name), stg_pie=stg_pie, stg_summary=stg_summary)


@f_app_blueprint.route("/add")
@login_required
def add():
    """
    加载智能生成组合配比页面
    :by hdhuang
    :return:
    """
    fof_list = cache.get(str(current_user.id))
    query = db.session.query(strategy_index_val.index_name.distinct().label("title"))
    exist_name = [row.title for row in query.all()]
    exist_name = [STRATEGY_EN_CN_DIC.get(i) for i in exist_name]
    strategy_list = list(STRATEGY_EN_CN_DIC.values())
    sorted(strategy_list, key=lambda x: len(x))
    chunk_list = [i for i in chunks(strategy_list, 4)]
    not_exist = list(set(strategy_list) - set(exist_name))
    return render_template('append.html', chunk_list=chunk_list, not_exist=not_exist,fof_list=fof_list)


@f_app_blueprint.route('/tab2', methods=['POST'])
def tab2():
    """
    智能生成组合配比页面第二页
    :by hdhuang
    :return:
    """
    data = request.get_json()
    sub_view = {k: get_Value(STRATEGY_EN_CN_DIC, v) if get_Value(STRATEGY_EN_CN_DIC, v) is not None else v for i in
                data['subjective_view'] for k, v in i.items()}
    strategies = [get_Value(STRATEGY_EN_CN_DIC, i) for i in data['strategies']]
    data["subjective_view"] = sub_view
    data["strategies"] = strategies
    pr_data = calc_portfolio_optim_fund(json.dumps(data), JSON_DB)
    pr_data = pr_data['strategyWeight'].to_dict()['Weight']
    legend = list(pr_data.keys())
    trans_dict = STRATEGY_EN_CN_DIC
    result = [{"name": trans_dict.get(k), "value": v} for k, v in pr_data.items()]
    for i in result:
        i['value'] = math.floor(i['value'] * 100)
    result = [i for i in result if i['value'] != 0]
    data['strategies'] = [{"strategy": get_Value(STRATEGY_EN_CN_DIC, i["name"]), "percent": i['value'], 'operator': ''}
                          for i in result]
    mx = max([i['percent'] for i in data['strategies']])
    for i in data['strategies']:
        if i['percent'] == mx:
            i['operator'] = 'largerorequal'
        else:
            i['operator'] = 'smallerorequal'
    session['tab2'] = data
    return json.dumps({'status': 'ok', 'legend': legend, 'result': result})


@f_app_blueprint.route("/tab3", methods=['POST', 'GET'])
@login_required
def tab3():
    """
    智能生成组合配比页面第三页
    :by hdhuang
    :return:
    """
    if request.method == "POST":
        user_select = request.get_json()
        expressions = []
        if user_select.get('years'):
            years = user_select['years']
            start, end = range_years(years[1], years[0])
            expressions.append(FoFModel.fund_setupdate.between(start, end))
        if user_select.get('name'):
            name = user_select.get('name')
            expressions.append(FoFModel.strategy_type == name)
        if user_select.get('rank'):
            rank = user_select.get('rank')
            expressions.append(FoFModel.rank == rank)

        data = FoFModel.query.filter(*expressions).order_by(FoFModel.nav_date_latest)
        data = [{'name': i.sec_name, 'code': i.wind_code,
                 'model': i.strategy_type, 'date': i.fund_setupdate.strftime("%Y-%m-%d")} for i in data]
        return json.dumps(data)


@f_app_blueprint.route('/tab4', methods=['POST'])
def tab4():
    """
    智能生成组合配比页面第四页
    :by hdhuang
    :return:
    """
    if request.method == 'POST':
        json_obj = request.get_json()
        t4_data = []
        for i in json_obj:
            for x in i['fund']:
                fund = FoFModel.query.filter_by(sec_name=x.split(" ")[0]).first()
                t4_data.append({'fund': fund.wind_code, 'strategies': [
                    {'strategy': get_Value(STRATEGY_EN_CN_DIC, i['title']), 'percent': 100}]})
        port_f = dict(session['tab2'])

        port_f['funds'] = t4_data
        raw_calc = c4(json.dumps(port_f), JSON_DB)
        raw_calc_obj = {}

        for k, v in raw_calc.items():
            if k == 'fundWeightA':
                for i in v.values:
                    if str(i[0]) != '0.0':
                        raw_calc_obj = v.to_json()
                        break

            elif k == 'fundWeightB':
                for i in v.values:
                    if str(i[0]) != '0.0':
                        raw_calc_obj = v.to_json()
                        break
            elif k == 'fundWeightC':
                for i in v.values:
                    if str(i[0]) != '0.0':
                        raw_calc_obj = v.to_json()
                        break
            else:
                raw_calc_obj = json.dumps("balance")

        return json.dumps({'status': 'ok', "data": raw_calc_obj})


@f_app_blueprint.route('/manual_add', methods=['POST', 'GET'])
def manual_add():
    """
    手动选择基金生成组合页面,用户可以自主选择基金,同时可以根绝用户添加的基金动态生成组合配置图形
    :by hdhuang
    :return:
    """
    if request.method == 'GET':
        fof_list = get_all_fof()
        return render_template('manual_add.html', fof_list=fof_list)
    elif request.method == 'POST':
        post_data = request.json
        stg_list = []
        code_value = []
        for x in post_data:
            result = {}
            c_code = FUND_ESSENTIAL.query.filter_by(wind_code_s=x[1]).first()
            if c_code is not None:
                result['wind_code'] = c_code.wind_code
            else:
                result['wind_code'] = x[1]
            result['value'] = x[2]
            code_value.append(result)
        for i in code_value:
            if i['wind_code'] != 'fh0000':
                last_date = FUND_STG_PCT.query.filter_by(wind_code=i['wind_code']).order_by(
                    FUND_STG_PCT.trade_date.desc()).first().trade_date
                stg = FUND_STG_PCT.query.filter(
                    and_(FUND_STG_PCT.wind_code == i['wind_code'], FUND_STG_PCT.trade_date == last_date)).all()
                stg_pct = [{"stg_name": x.stg_code, "stg_pct": x.stg_pct, "name": x.wind_code} for x in stg]
                stg_list.extend(stg_pct)
        cash = [{"stg_name": "未投现金", 'value': float(i['value']), "name": "fh0000"} for i in code_value if
                i['wind_code'] == 'fh0000']
        stg_pct = []
        for b in code_value:
            pct_temp = [
                {"stg_name": t['stg_name'], "value": float(b['value']) * (t['stg_pct'] / 100), "name": t['name']} for
                t in stg_list if t['name'] == b['wind_code']]
            stg_pct.extend(pct_temp)
        stg_pct.extend(cash)
        df = DataFrame(stg_pct)
        dx = df.groupby(['stg_name'])['value'].sum()
        stg_summary = [{"value": v, "name": k} for k, v in dx.items()]
        stg_name = {t['stg_name'] for t in stg_pct}
        stg_pie = []
        for i in stg_name:
            series = [{"name": code_get_name(x['name']), "value": x['value']} for x in stg_pct if x['stg_name'] == i]
            series_obj = {"name": i, "data": series}
            stg_pie.append(series_obj)
        full_year = datetime.datetime.now() - datetime.timedelta(days=365)
        wind_code_dict = {i[1]: int(i[2]) for i in post_data}
        ret_dic = calc_index_by_wind_code_dic(wind_code_dict, full_year.strftime("%Y-%m-%d"),
                                              datetime.date.today().strftime('%Y-%m-%d'))
        data = [i for i in ret_dic]
        time_line = [i.strftime("%Y-%m-%d") for i in ret_dic.index]
        line_pie = {"time_line": time_line, 'data': data}
        return jsonify(key=list(stg_name), stg_pie=stg_pie, stg_summary=stg_summary, line_pie=line_pie)


@f_app_blueprint.route('/save_scheme', methods=['POST', 'GET'])
@login_required
def save_scheme():
    """
    保存用户手动配比的组合,立即执行压力测试，
    :param scheme_name 组合名称　当前时间
    :by hdhuang
    :return:
    """
    if request.method == 'POST':
        scheme_name = datetime.datetime.now().strftime("%Y-%m-%d %H.%M.%S")
        scheme = INFO_SCHEME(scheme_name=scheme_name,
                             scheme_setupdate=datetime.datetime.now(),
                             create_user=current_user.id)
        db.session.add(scheme)
        db.session.commit()
        for i in request.json:
            pct_scheme = PCT_SCHEME(scheme_id=scheme.scheme_id, wind_code=i[1], invest_scale=i[2])
            db.session.add(pct_scheme)
            db.session.commit()
        scheme_id = scheme.scheme_id
        task = run_scheme_testing.apply_async(kwargs={"user": current_user.email, 'sid': scheme_id},
                                              task_id=scheme_name)
        return jsonify(status='ok')


@f_app_blueprint.route('/testing_result', methods=['POST', 'GET'])
@login_required
def testing_result():
    """
    压力测试结果查询,
    :by hdhuang
    :return: 所有的压力结果供用户选择
    """
    if request.method == 'GET':
        fof_list = get_all_fof()
        result = INFO_SCHEME.query.all()
        result = [{"scheme_id": i.scheme_id, "scheme_name": i.scheme_name,
                   "create_time": i.scheme_setupdate, 'task': run_scheme_testing.AsyncResult(i.scheme_name).state,
                   "user": UserModel.query.get(i.create_user).username} for i in result]
        return render_template('testing_result.html', fof_list=fof_list, result=result)


@f_app_blueprint.route('/show_testing', methods=['POST', 'GET'])
@login_required
def show_testing():
    """
    在页面展现压力测试结果
    异步任务id和scheme_name相同,压力测试完成后在redis中保存３个key
    :key_name:scheme_id_type
    :by hdhuang
    :return:
    """
    if request.method == 'POST':
        if request.json['schemeName'] == '':
            return jsonify(status='del')
        else:
            scheme_name = request.json['schemeName']
            task = run_scheme_testing.AsyncResult(scheme_name)
            if task.state == 'PENDING':
                return json.dumps({"status": 'pending'})
            elif task.state == 'FAILURE':
                return json.dumps({"status": 'error'})
            else:
                scheme = INFO_SCHEME.query.filter_by(scheme_name=scheme_name).first()
                scheme_fund = PCT_SCHEME.query.filter_by(scheme_id=scheme.scheme_id)
                pct_scheme = [
                    {"fund_name": code_get_name(i.wind_code), "scale": i.invest_scale, "wind_code": i.wind_code} for i
                    in scheme_fund]
                r = get_redis()
                copula = r.get('scheme_{0}_{1}'.format(scheme.scheme_id, 'copula'))
                if copula is not None:
                    copula_obj = json.loads(copula.decode('utf-8'))
                else:
                    copula_obj = ''
                fhs = r.get('scheme_{0}_{1}'.format(scheme.scheme_id, 'fhs_garch'))
                if fhs is not None:
                    fhs_obj = json.loads(fhs.decode('utf-8'))
                else:
                    fhs_obj = ''
                return json.dumps({"copula": copula_obj, 'fhs': fhs_obj, 'status': 'ok', "pct_scheme": pct_scheme})


@f_app_blueprint.route('/del_scheme', methods=['GET', 'POST'])
@login_required
def del_scheme():
    """
    删除压力测试结果,pending状态的压力结果不能删除
    :by hdhuang
    :return:
    """
    if request.method == 'POST':
        logger.warning("删除scheme{}".format(request.json['name']))
        scheme = INFO_SCHEME.query.filter_by(scheme_name=request.json['name']).first()
        pct_scheme = PCT_SCHEME.query.filter_by(scheme_id=scheme.scheme_id).all()
        db.session.delete(scheme)
        [db.session.delete(i) for i in pct_scheme]
        db.session.commit()
        logger.info("scheme{} info pct 已删除".format(request.json['name']))
        r = get_redis(host='127.0.0.1', db=0)
        r.delete('celery-task-meta-' + request.json['name'])
        logger.info("celery {} 已删除".format(request.json['name']))
        return jsonify(status='ok')



@f_app_blueprint.route('/data_show', methods=['GET', 'POST'])
def data_show():
    fof_list = get_all_fof()
    return render_template('data_show.html', fof_list=fof_list)


@f_app_blueprint.route('/data_select', methods=['GET', 'POST'])
def data_select2():
    if request.method == 'POST':
        start = request.form['start']
        end = request.form['end']
        strategy_type_en = request.form['name']
        if start == "" and end == "":
            now = datetime.datetime.now()
            last_year = int(now.year) - 1
            last_months = now.month
            last_day = now.day
            date_from_str = "%s-%s-%s" % (last_year, last_months, last_day)
            date_to_str = time.strftime("%Y-%m-%d", time.localtime())
        else:
            start_time = datetime.datetime.strptime(start, '%Y-%m-%d')
            end_time = datetime.datetime.strptime(end, '%Y-%m-%d')
            date_from_str = start_time.strftime('%Y-%m-%d')
            date_to_str = end_time.strftime('%Y-%m-%d')
        df_rr_df = get_strategy_index_quantile(strategy_type_en, date_from_str, date_to_str,
                                               [0.95, 0.90, 0.75, 0.6, 0.50, 0.4, 0.25, 0.10, 0.05])
        df_rr_df.index = [d.strftime('%Y-%m-%d') if type(d) in (datetime.date, datetime.datetime) else d for d in
                          df_rr_df.index]
        fof_dict = {"min": df_rr_df.min().to_json(), "value": df_rr_df.to_json()}
        return json.dumps(fof_dict)


@f_app_blueprint.route('/invest_corp')
@login_required
@permission
def invest_corp():
    """
    数据库所有的投顾的表格,每种级别投顾的个数
    :param 0,1,2,4
    :by hdhuang
    :return:
    """
    fof_list = get_all_fof()
    invest = Invest_corp.query.all()
    all_invest = [{"id": index, "name": i.name, "alias": i.alias, "review_status": i.review_status,
                   'tot': i.fund_count_tot, 'existing': i.fund_count_existing, "active": i.fund_count_active,
                   'uid': i.mgrcomp_id} for index, i in
                  enumerate(invest)]
    core_invest = query_invest(4)
    observe_invest = query_invest(3)
    archive_invest = query_invest(2)
    all_data = {"core": core_invest, 'all': {"data": all_invest, 'length': len(all_invest)}, "observe": observe_invest,
                "archive": archive_invest, }
    return render_template("f_app/invest_corp.html", fof_list=fof_list, all_data=all_data)


@f_app_blueprint.route('/get_corp')
def get_corp():
    """
    Datatables插件服务器端获取数据方式
    :param 数据中每个列的名称,要和Datatables中列一致
    :by hdhuang
    :return:
    """
    columns = ['mgrcomp_id', 'name', 'alias', 'fund_count_tot', 'fund_count_existing', 'fund_count_active',
               'review_status', ]
    index_column = "mgrcomp_id"
    table = "fund_mgrcomp_info"
    result = DataTablesServer(request, columns=columns, table=table, index=index_column).output_result()
    return json.dumps(result)


@f_app_blueprint.route('/process/<uid>', methods=['GET', 'POST'])
@login_required
def process(uid):
    """
       投顾评估报告
       :param uid:投顾id
       :by hdhuang
       :return:
       """
    if request.method == 'GET':
        fof_list = get_all_fof()
        corp = Invest_corp.query.get(uid)
        return render_template("process.html", fof_list=fof_list, corp=corp)
    if request.method == 'POST':
        comments = request.form['comments']
        file = request.files['file']
        file_name = file.filename
        f_content = file.read()
        file_record = Invest_corp_file(mgrcomp_id=uid, file_type='report', upload_user_id=current_user.id,
                                       upload_datetime=datetime.datetime.now(),
                                       file_name=file_name, file_content=f_content, comments=comments)
        db.session.add(file_record)
        db.session.commit()
        file.close()
        return redirect(url_for('f_app.corp', uid=uid))





@f_app_blueprint.route('/corp_upload_file/<uid>', methods=['GET', 'POST'])
@login_required
def corp_upload_file(uid):
    """
        投顾详情页添加文件
        :param uid:投顾id
        :by zhoushaobo
        :return:
    """
    fof_list = get_all_fof()
    corp = Invest_corp.query.get(uid)
    file_type = FileType.query.filter(FileType.type_name != "评估报告").all()
    file_type = [i.type_name for i in file_type]
    return render_template("corp_upload_file.html", fof_list=fof_list, corp=corp, file_type=file_type)


@f_app_blueprint.route('/append_file_upload/<uid>', methods=['GET', 'POST'])
@login_required
def append_file_upload(uid):
    if request.method == 'POST':
        file = request.files['file']
        file_name = file.filename
        file_type = dict(request.form)
        file_type = file_type['type'][0]
        f_content = file.read()
        file_record = Invest_corp_file(mgrcomp_id=uid, file_type=file_type, upload_user_id=current_user.id,
                                       upload_datetime=datetime.datetime.now(),
                                       file_name=file_name, file_content=f_content)
        db.session.add(file_record)
        db.session.commit()
        file.close()
        return redirect(url_for('f_app.corp', uid=uid))


@f_app_blueprint.route('/corp/<uid>', methods=['GET', 'POST'])
@login_required
@permission
def corp(uid):
    """
    投顾详细信息页面,展示投顾每个产品的历史净值图表,上传文件，修改投顾级别
    :param uid: 投顾id
    :by hdhuang
    :return:
    """
    fof_list = get_all_fof()
    corp = Invest_corp.query.get(uid)
    fof = FoFModel.query.filter_by(mgrcomp_id=uid).all()
    if current_user.is_admin:
        files = Invest_corp_file.query.filter(Invest_corp_file.mgrcomp_id==uid).all()
    elif current_user.is_report:
        files = Invest_corp_file.query.filter(and_(Invest_corp_file.mgrcomp_id == uid,
                                                   Invest_corp_file.upload_user_id == current_user.id)).all()
    report = [{"file_name": i.file_name, "comments": i.comments, "user": UserModel.query.get(i.upload_user_id),
               "upload_time": i.upload_datetime, "fid": i.file_id} for i in files if i.file_type == 'report']
    files = [{"file_name": i.file_name, "user": UserModel.query.get(i.upload_user_id),
              "upload_time": i.upload_datetime, "fid": i.file_id, "file_type": i.file_type} for i in files if
             i.file_type != 'report']
    if len(fof) > 0:
        fof = [{"name": i.sec_name, "alias": i.alias, "wind_code": i.wind_code,"rank":i.rank} for i in fof]
    else:
        fof = None
    return render_template("corp.html", fof_list=fof_list, corp=corp, fof=fof, report=report, files=files)


@f_app_blueprint.route('/get_fof_acc', methods=['POST', 'GET'])
@login_required
def get_fof_acc():
    """
    获取投顾的每一个产品的净值
    :by hdhuang
    :return:
    """
    if request.method == 'POST':
        wind_code = request.json['code']
        time_id = request.json['time_id']
        today = datetime.date.today()
        if time_id == '0':
            start_date = datetime.date(today.year, 1, 1)
        else:
            start_date = datetime.date(today.year - int(time_id), today.month, today.day)
        df_dict = data_handler.get_fof_nav_between(wind_code, start_date, today)
        if df_dict is not None:
            pct_df = df_dict['fund_df']
            data = {}
            result = [{"name": i, "data": np.array(pct_df[i]).tolist()} for i in pct_df.columns]
            data['result'] = [i if i['name'] == '000300.SH' else {'name': code_get_name(i['name']), 'data': i['data']}
                              for i in
                              result]
            time_line = pct_df.index
            data['time_line'] = [i.strftime('%Y-%m-%d') for i in time_line]
            data['data_name'] = [i['name'] for i in data['result']]
            return jsonify(status='ok', data=data)
        else:
            return jsonify(status='error')


@f_app_blueprint.route('/corp_download/<fid>')
@login_required
@permission
def corp_download(fid):
    """
    下载和投顾相关的文件
    :param fid: 文件id
    :by hdhuang
    :return:
    """
    corp_file = Invest_corp_file.query.get(fid)
    file_name = corp_file.file_name
    corp_path = current_app.config['CORP_FOLDER']
    corp_file_path = path.join(corp_path, file_name)
    with open(corp_file_path, 'wb') as f:
        f.write(corp_file.file_content)
    response = make_response(send_file(corp_file_path, as_attachment=True, attachment_filename=file_name))
    basename = os.path.basename(file_name)
    response.headers["Content-Disposition"] = \
        "attachment;" \
        "filename*=UTF-8''{utf_filename}".format(
            utf_filename=quote(basename.encode('utf-8'))
        )
    os.remove(corp_file_path)
    return response


@f_app_blueprint.route('/change_corp_rank', methods=['POST', 'GET'])
@login_required
def change_corp_rank():
    """
    修改投顾级别
    :by hdhuang
    :return:
    """
    if request.method == 'POST':
        corp_id = int(request.json['corp'])
        rank = int(request.json['rank'])
        site = request.json['site']
        company = request.json['company']
        corp = Invest_corp.query.get(corp_id)
        corp.review_status = rank
        corp.address = site
        corp.description = company
        db.session.commit()
        return jsonify(status='ok')

@f_app_blueprint.route('/select_corp_rank', methods=['POST', 'GET'])
@login_required
def select_corp_rank():
    """
    修改投顾评级类型
    :by zhoushaobo
    :return:
    """
    if request.method == 'POST':
        code = request.json['code']
        choose = request.json['choose']
        fund = FoFModel.query.filter_by(wind_code=code).first()
        fund.rank = choose
        db.session.commit()
        return jsonify(status='ok')

@f_app_blueprint.route('/add_corp', methods=['GET', 'POST'])
@login_required
def add_corp():
    """
    添加投顾信息页面,可以让用户手动添加投顾
    :by zhoushaobo
    :return:
    """
    if request.method == 'GET':
        fof_list = cache.get(str(current_user.id))
        return render_template("add_corp.html", fof_list=fof_list)
    if request.method == 'POST':
        name = request.form['name']
        alias = request.form['alias']
        register_capital = request.form['register_capital']
        status = request.form['status']
        site = request.form['site']
        desc = request.form['description']
        corp = Invest_corp(name=name, alias=alias, review_status=int(status), address=site, description=desc,
                           registered_capital=register_capital)
        db.session.add(corp)
        db.session.commit()
        return redirect(url_for('f_app.invest_corp'))



@f_app_blueprint.route('/fund_corp')
@login_required
def fund_corp():
    """
    数据库所有基金展现
    :by zhoushaobo
    :return:
    """
    fof_list = cache.get(str(current_user.id))
    invest = FOF_FUND_PCT.query.all()
    child = []
    for i in invest:
        data = {}
        data['name'] = code_get_name(i.wind_code_s)
        data['date'] = i.date_adj.strftime("%Y-%m-%d")
        data['scale'] = i.invest_scale
        data['code'] = i.wind_code_s
        child.append(data)

    return render_template("f_app/fund_corp.html", fof_list=fof_list, all_data=child)



@f_app_blueprint.route('/noopsyche_add', methods=['POST', 'GET'])
def noopsyche_add():
    """
    用户可以手动选择基金生成组合页面,用户可以自主选择基金,也可以保存智能选择的模式,同时可以根绝用户添加的基金动态生成组合配置图形
    :by zhoushaobo
    :return:
    """
    if request.method == 'GET':
        fof_list = cache.get(str(current_user.id))
        return render_template('noopsyche_add.html', fof_list=fof_list)
    elif request.method == 'POST':
        post_data = request.json
        stg_list = []
        code_value = []
        for x in post_data:
            result = {}
            c_code = FUND_ESSENTIAL.query.filter_by(wind_code_s=x[1]).first()
            if c_code is not None:
                result['wind_code'] = c_code.wind_code
            else:
                result['wind_code'] = x[1]
            result['value'] = x[2]
            code_value.append(result)
        for i in code_value:
            if i['wind_code'] != 'fh0000':
                last_date = FUND_STG_PCT.query.filter_by(wind_code=i['wind_code']).order_by(
                    FUND_STG_PCT.trade_date.desc()).first().trade_date
                stg = FUND_STG_PCT.query.filter(
                    and_(FUND_STG_PCT.wind_code == i['wind_code'], FUND_STG_PCT.trade_date == last_date)).all()
                stg_pct = [{"stg_name": x.stg_code, "stg_pct": x.stg_pct, "name": x.wind_code} for x in stg]
                stg_list.extend(stg_pct)
        cash = [{"stg_name": "未投现金", 'value': float(i['value']), "name": "fh0000"} for i in code_value if
                i['wind_code'] == 'fh0000']
        stg_pct = []
        for b in code_value:
            pct_temp = [
                {"stg_name": t['stg_name'], "value": float(b['value']) * (t['stg_pct'] / 100), "name": t['name']} for
                t in stg_list if t['name'] == b['wind_code']]
            stg_pct.extend(pct_temp)
        stg_pct.extend(cash)
        df = DataFrame(stg_pct)
        dx = df.groupby(['stg_name'])['value'].sum()
        stg_summary = [{"value": v, "name": k} for k, v in dx.items()]
        stg_name = {t['stg_name'] for t in stg_pct}
        stg_pie = []
        for i in stg_name:
            series = [{"name": code_get_name(x['name']), "value": x['value']} for x in stg_pct if x['stg_name'] == i]
            series_obj = {"name": i, "data": series}
            stg_pie.append(series_obj)
        full_year = datetime.datetime.now() - datetime.timedelta(days=365)
        wind_code_dict = {i[1]: int(i[2]) for i in post_data}
        ret_dic = calc_index_by_wind_code_dic(wind_code_dict, full_year.strftime("%Y-%m-%d"),
                                              datetime.date.today().strftime('%Y-%m-%d'))
        data = [i for i in ret_dic]
        time_line = [i.strftime("%Y-%m-%d") for i in ret_dic.index]
        line_pie = {"time_line": time_line, 'data': data}
        return jsonify(key=list(stg_name), stg_pie=stg_pie, stg_summary=stg_summary, line_pie=line_pie)


@f_app_blueprint.route('/batch')
@login_required
@permission
def batch():
    fof_list = cache.get(str(current_user.id))
    return render_template('batch.html', fof_list=fof_list)


@f_app_blueprint.route('/get_batch')
@login_required
def get_batch():
    """
    Datatables插件服务器端获取数据方式
    :param 数据中每个列的名称,要和Datatables中列一致
    :by hdhuang
    :return:
    """
    columns = ['wind_code_s', 'wind_code', 'sec_name_s', 'date_start', 'date_end', 'warning_line',
               'winding_line', ]
    index_column = "wind_code_s"
    table = "fund_essential_info"
    result = DataTablesServer(request, columns=columns, table=table, index=index_column).output_result()
    for i in result['aaData']:
        if i['date_start'] is not None:
            i['date_start'] = i['date_start'].strftime('%Y-%m-%d')
        if i['date_end'] is not None:
            i['date_end'] = i['date_end'].strftime('%Y-%m-%d')
    return json.dumps(result)


@f_app_blueprint.route('/batch_details/<wind_code_s>')
@login_required
def batch_details(wind_code_s):
    fof_list = cache.get(str(current_user.id))
    batch = FUND_ESSENTIAL.query.filter_by(wind_code_s=wind_code_s).first()
    data = batch.to_json()
    return render_template('batch_details.html',batch=data,fof_list=fof_list)

@f_app_blueprint.route('/edit_batch/<wind_code_s>',methods=['GET','POST'])
@login_required
def edit_batch(wind_code_s):
    if request.method == 'GET':
        fof_list = cache.get(str(current_user.id))
        batch = FUND_ESSENTIAL.query.filter_by(wind_code_s=wind_code_s).first()
        data = batch.to_json()
        return render_template('edit_batch.html',fof_list=fof_list,batch=data)
    elif request.method == 'POST':
        data = request.form.to_dict()
        batch = FUND_ESSENTIAL.query.filter_by(wind_code_s=wind_code_s).first()
        for k,v in data.items():
            if k == 'date_end' and len(v) == 0:
                v = None
            setattr(batch,k,v)
        db.session.commit()
        return redirect(url_for('f_app.batch_details',wind_code_s=wind_code_s))



@f_app_blueprint.route('/test')
def test():
    fof_list = cache.get(str(current_user.id))
    fund_list = []
    for i in fof_list:
        for x in i['child']:
            batch = FUND_ESSENTIAL.query.filter_by(wind_code_s=x['code']).first()
            if batch is None:
                fund = FoFModel.query.filter_by(wind_code=x['code']).first()
            else:
                fund = FoFModel.query.filter_by(wind_code=batch.wind_code).first()
            if fund not in fund_list:
                fund_list.append(fund)
    return render_template('test.html',fof_list=fof_list,fund_list=fund_list)



@f_app_blueprint.route('/market_report',methods=['GET','POST'])
@login_required
@permission
def market_report():
    if request.method == 'GET':
        fof_list = cache.get(str(current_user.id))
        return render_template('market_report.html',fof_list=fof_list)
    if request.method == 'POST':
        date_range = request.json
        try:
            file_name,data = gen_report(date_range['start'],date_range['end'])
            charts_dict = data['charts_dict']
            charts = {}
            for k,v in charts_dict.items():
                charts[k] = v['date_idx_quantile_df'].T.to_json()
            table_df = data['table_df']
            return_data = {}
            return_data['charts'] = charts
            return_data['table'] = table_df.to_json()
            text = []
            for i in data['text_dict']:
                text_dict = {}
                for k,v in i.items():
                    if isinstance(v,np.int64):
                        text_dict[k] = int(v)
                    else:
                        text_dict[k] = v
                text.append(text_dict)
            return_data['text'] = text
            return_data['file_name'] = file_name
            return jsonify(status='ok',data=return_data)
        except ZeroDivisionError:
            return jsonify(status='error')


@f_app_blueprint.route('/download_report/')
@login_required
@permission
def download_report():
    data = request.args.to_dict()
    file_path = data['file_name']
    file_name = os.path.split(file_path)[-1]
    response = make_response(send_file(file_path, as_attachment=True, attachment_filename=file_name))
    basename = os.path.basename(file_name)
    response.headers["Content-Disposition"] = \
        "attachment;" \
        "filename*=UTF-8''{utf_filename}".format(
            utf_filename=quote(basename.encode('utf-8'))
        )
    os.remove(file_path)
    return response


@f_app_blueprint.route('/core_info/<wind_code>',methods=['POST','GET'])
@login_required
def core_info(wind_code):
    if request.method == 'GET':
        fof = get_core_info(wind_code)
        core_info = Fund_Core_Info.query.filter_by(wind_code=wind_code).first()
        return  render_template('core_info.html',core_info=core_info,fof=fof)
    if request.method == 'POST':
        data = request.form.to_dict()
        code = Fund_Core_Info.query.filter_by(wind_code=wind_code).first()
        if code is None:
            core_info = Fund_Core_Info(**data)
            db.session.add(core_info)
            db.session.commit()
        else:
            for k, v in data.items():
                setattr(code, k, v)
            db.session.commit()
        return redirect(url_for("f_app.details", wind_code=wind_code))


@f_app_blueprint.route('/import_nav',methods=['GET','POST'])
@login_required
def import_nav():
    if request.method == 'GET':
        fof_list = cache.get(str(current_user.id))
        return render_template("import_nav.html",fof_list=fof_list)

    if request.method == "POST":
        file = request.files['file']
        if file and allowed_file(file.filename):
            file_path = os.path.join(current_app.config['ACC_FOLDER'], file.filename)
            file.save(file_path)
            data_dict, error_list = check_fund_nav_multi(file_path)
            return render_template("acc_file_result.html",data_dict=data_dict,error_list=error_list,file_path=file_path)

@f_app_blueprint.route('/confirm_acc',methods=['POST','GET'])
@login_required
def confirm_acc():
    if request.method == 'POST':
        file_path = request.form['file_path']
        import_fund_nav_multi(file_path=file_path)
        flash("上传成功",category="success")
        os.remove(file_path)
        return redirect(url_for("f_app.home"))

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1] in ALLOWED_EXTENSIONS


@f_app_blueprint.app_errorhandler(404)
def page_not_found(error):
    return render_template('page_404.html'), 404


@f_app_blueprint.app_errorhandler(403)
def page_not_found(error):
    return render_template('page_403.html'), 403


@f_app_blueprint.app_errorhandler(500)
def server_error(error):
    return render_template('page_500.html'), 500


@f_app_blueprint.app_errorhandler(408)
def server_error(error):
    return render_template('page_408.html'), 408


@f_app_blueprint.app_errorhandler(409)
def server_error(error):
    return render_template('page_409.html'), 409
