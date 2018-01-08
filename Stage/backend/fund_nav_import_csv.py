import math
import pandas as pd
import numpy as np
from sqlalchemy.types import String, Date, FLOAT, Integer
from config_fh import get_db_session, get_db_engine, STR_FORMAT_DATE
import os
from fh_tools.fh_utils import str_2_date, get_first, pattern_data_format, try_2_date
import xlrd
import logging

logger = logging.getLogger()


def update_fundnav_by_csv2(file_path, mode='delete_insert'):
    df = pd.read_csv(file_path)
    df.set_index('nav_date_week', inplace=True)
    df_fund = df.unstack().reset_index().dropna()
    col_name_list = list(df_fund.columns)
    df_fund.rename(columns={col_name_list[0]: 'wind_code', col_name_list[1]: 'nav_date', col_name_list[2]: 'nav'},
                   inplace=True)
    df_fund[['trade_date', 'nav_acc']] = df_fund[['nav_date', 'nav']]
    if mode == 'delete_insert':
        df_fund.set_index(['wind_code', 'trade_date'], inplace=True)
        # df_fund['nav_acc'] = df_fund['nav']
        table_name = 'fund_nav_tmp'

        sql_str = 'delete from fund_nav_tmp where wind_code in (%s)' % ("'" + "', '".join(df.columns) + "'")
        engine = get_db_engine()
        with get_db_session(engine) as session:
            session.execute(sql_str)
        df_fund.to_sql(table_name, engine, if_exists='append',
                       dtype={
                           'wind_code': String(20),
                           'trade_date': Date,
                           'nav_date': Date,
                           'nav': FLOAT,
                           'nav_acc': FLOAT,
                       })
    elif mode == 'replace_insert':
        data_list = list(df_fund.T.to_dict().values())
        sql_str = "REPLACE INTO fund_nav_tmp (wind_code, trade_date, nav, nav_acc, nav_date) values (:wind_code, :trade_date, :nav, :nav_acc, :nav_date)"
        with get_db_session() as session:
            session.execute(sql_str, data_list)
            pass
    else:
        raise ValueError('mode="%s" is not available' % mode)


# def fund_nav_df_fillna(fund_nav_df):
#     """
#     为 fund_nav_df 补充nav数据并记录错误日志
#     :param fund_nav_df:
#     :return:
#     """
#     fund_nav_df.dropna(subset=['nav'], inplace=True)
#     fund_nav_df['nav_acc'] = fund_nav_df[['nav', 'nav_acc']].fillna(method='ffill', axis=1)['nav_acc']
#     return fund_nav_df


def update_fundnav_by_file(wind_code, file_path, mode='delete_insert', skip_rows=0, sheet_name=0):
    """
    支持 csv xls xlsx 文件格式导入 fund_nav表 
    :param wind_code: 
    :param file_path: 
    :param mode: 'replace_insert' 更新或插入；
    'delete_insert' 删除时期段内的数据并插入；
    'remove_insert' 移除全部历史数据并插入；
    :return: 
    """
    _, file_extension = os.path.splitext(file_path)
    if file_extension == '.csv':
        fund_nav_df = pd.read_csv(file_path)
    elif file_extension in ('.xls', '.xlsx'):
        fund_nav_df = pd.read_excel(file_path, skiprows=skip_rows, sheetname=sheet_name)
    else:
        raise ValueError('不支持 %s 净值文件类型' % file_extension)
    col_name_list = list(fund_nav_df.columns)
    if len(col_name_list) == 2:
        fund_nav_df['nav_acc'] = fund_nav_df[col_name_list[1]]
        col_name_list = list(fund_nav_df.columns)
    fund_nav_df.rename(columns={col_name_list[0]: 'nav_date', col_name_list[1]: 'nav', col_name_list[2]: 'nav_acc'},
                       inplace=True)
    fund_nav_df.dropna(subset=['nav'], inplace=True)
    fund_nav_df['nav_acc'] = fund_nav_df[['nav', 'nav_acc']].fillna(method='ffill', axis=1)['nav_acc']
    fund_nav_df['wind_code'] = wind_code
    update_fund_nav_df(fund_nav_df, mode=mode)


def update_fund_nav_df(data_df, mode='delete_insert'):
    """
    将净值df 文件更新到数据库。df文件格式 wind_code, nav_date, nav, nav_acc
    :param fund_nav_df: 
    :param mode: 
    :return: 
    """
    data_df['source_mark'] = 2
    for wind_code, fund_nav_df in data_df.groupby('wind_code'):
        # lambda x: datetime.strptime(x, STR_FORMAT_DATE)
        data_str = get_first(fund_nav_df['nav_date'], lambda x: type(x) == str)
        if data_str is not None:
            date_str_format = pattern_data_format(data_str)
            nav_date_s = fund_nav_df['nav_date'].apply(
                lambda x: str_2_date(x, date_str_format=date_str_format))
            fund_nav_df['nav_date'] = nav_date_s
        else:
            nav_date_s = fund_nav_df['nav_date'].apply(try_2_date)
            fund_nav_df['nav_date'] = nav_date_s
        fund_nav_df.dropna(inplace=True)
        if nav_date_s.shape[0] > 0:
            date_min, date_max = try_2_date(nav_date_s.min()), try_2_date(nav_date_s.max())
        else:
            date_min, date_max = None, None
        # 更新数据库
        engine = get_db_engine()
        if mode == 'delete_insert':
            # nav_date_s = fund_nav_df['nav_date']
            if nav_date_s.shape[0] > 0:
                with get_db_session(engine) as session:
                    # 清理历史数据

                    sql_str = 'delete from fund_nav where wind_code = :wind_code and nav_date between :date_frm and :date_to'
                    session.execute(sql_str, [{'wind_code': wind_code,
                                               'date_frm': date_min,
                                               'date_to': date_max}])
                    # 数据插入表
                    table_name = 'fund_nav'
                    fund_nav_df.set_index(['wind_code', 'nav_date'], inplace=True)
                    fund_nav_df.to_sql(table_name, engine, if_exists='append',
                                       dtype={
                                           'wind_code': String(20),
                                           'nav_date': Date,
                                           'nav': FLOAT,
                                           'nav_acc': FLOAT,
                                           'source_mark': Integer
                                       })
                    # 执行存储过程，将相关批次数据统一更新（废弃，各产品归一后净值由单独模块来进行维护）
                    # if date_max is not None:
                    #     sql_str = 'call proc_replace_fund_nav_by_wind_code_until(:wind_code, :nav_date, :force_update)'
                    #     session.execute(sql_str, [{'wind_code': wind_code,
                    #                                'nav_date': date_max,
                    #                                'force_update': True}])

        elif mode == 'remove_insert':
            with get_db_session(engine) as session:
                nav_date_s = fund_nav_df['nav_date']
                # 清理历史数据
                sql_str = 'delete from fund_nav where wind_code = :wind_code'
                session.execute(sql_str, [{'wind_code': wind_code}])
                # 数据插入表
                table_name = 'fund_nav'
                fund_nav_df.set_index(['wind_code', 'nav_date'], inplace=True)
                fund_nav_df.to_sql(table_name, engine, if_exists='append',
                                   dtype={
                                       'wind_code': String(20),
                                       'nav_date': Date,
                                       'nav': FLOAT,
                                       'nav_acc': FLOAT,
                                       'source_mark': Integer
                                   })
                # 执行存储过程，将相关批次数据统一更新（废弃，各产品归一后净值由单独模块来进行维护）
                # if date_max is not None:
                #     sql_str = 'call proc_replace_fund_nav_by_wind_code_until(:wind_code, :nav_date, :force_update)'
                #     session.execute(sql_str, [{'wind_code': wind_code,
                #                                'nav_date': date_max,
                #                                'force_update': True}])
        elif mode == 'replace_insert':
            data_list = list(fund_nav_df.T.to_dict().values())
            with get_db_session() as session:
                sql_str = "REPLACE INTO fund_nav (wind_code, nav_date, nav, nav_acc, source_mark) values (:wind_code, :nav_date, :nav, :nav_acc, :source_mark)"
                session.execute(sql_str, data_list)
                # 执行存储过程，将相关批次数据统一更新（废弃，各产品归一后净值由单独模块来进行维护）
                # if date_max is not None:
                #     sql_str = 'call proc_replace_fund_nav_by_wind_code_until(:wind_code, :nav_date, :force_update)'
                #     session.execute(sql_str, [{'wind_code': wind_code,
                #                                'nav_date': date_max,
                #                                'force_update': True}])
        else:
            raise ValueError('mode="%s" is not available' % mode)

        # 更新 fund_info 表统计信息
        sql_str = "call proc_update_fund_info_by_wind_code2(:wind_code, :force_update)"
        with get_db_session(engine) as session:
            session.execute(sql_str, {'wind_code': wind_code, 'force_update': True})
        logger.info('import fund_nav %d data on %s' % (fund_nav_df.shape[0], wind_code))


def update_fundnav_by_sheet_name(wind_code_sheet_name_dic_list, file_path, mode='replace_insert', skip_rows=0):
    """
    支持 csv xls xlsx 文件格式导入 fund_nav表 
    :param wind_code: 
    :param file_path: 
    :param mode: 'replace_insert' 'replace_insert'
    :return: 
    """
    _, file_extension = os.path.splitext(file_path)
    xls_data = xlrd.open_workbook(file_path)  # 打开xls文件
    name_list = xls_data.sheet_names()
    for wind_code_sheet_name_dic in wind_code_sheet_name_dic_list:
        wind_code = wind_code_sheet_name_dic['wind_code']
        sheet_name = wind_code_sheet_name_dic['sheet_name']
        index_sheet = name_list.index(sheet_name)
        update_fundnav_by_file(wind_code, file_path, mode=mode, skip_rows=skip_rows, sheet_name=index_sheet)


def import_fund_nav_fof1(file_path):
    wind_code_sheet_name_dic_list = [
        {'wind_code': 'FHF-101701', 'sheet_name': 'FHF-101601'},
        {'wind_code': 'XT1614159.XT', 'sheet_name': 'FHF-101601A'},  # 盛世复华汉武1号私募基金
        {'wind_code': 'XT1605537.XT', 'sheet_name': 'FHF-101601B'},  # 杉树欣欣
        {'wind_code': 'FHF-101601C', 'sheet_name': 'FHF-101601C'},  # 九坤量化阿尔法1号私募基金
        {'wind_code': 'XT1614142.XT', 'sheet_name': 'FHF-101601D'},  # 宽投复华1号CTA私募基金
        {'wind_code': 'XT1612348.XT', 'sheet_name': 'FHF-101601E'},  # 开拓者-复华-量化CTA私募基金
    ]
    update_fundnav_by_sheet_name(wind_code_sheet_name_dic_list, file_path, mode='replace_insert', skip_rows=1)


def import_fund_nav_fof2(file_path):
    wind_code_sheet_name_dic_list = [
        {'wind_code': 'FHF-101701', 'sheet_name': 'FHF-101701'},
        {'wind_code': 'FHF-101701C', 'sheet_name': 'FHF-101701C'},  # 因诺天机17号私募基金
        {'wind_code': 'FHF-101701D', 'sheet_name': 'FHF-101701D'},  # 新萌复华1号CTA私募基金
        {'wind_code': 'FHF-101701E', 'sheet_name': 'FHF-101701E'},  # 千象全景1号
    ]
    update_fundnav_by_sheet_name(wind_code_sheet_name_dic_list, file_path, mode='replace_insert', skip_rows=1)
    # update_fundnav_by_sheet_name(wind_code_sheet_name_dic_list, file_path, mode='delete_insert', skip_rows=1)


def check_fund_nav_multi(file_path, ret_df=False):
    """
    检查基金净值文件格式是否有效
    返回error_list, df.to_dict(默认)
    :param file_path: 
    :param ret_df: 
    :return: 
    """
    data_list, data_df, error_dic = [], None, {}
    _, file_extension = os.path.splitext(file_path)
    try:
        if file_extension == '.csv':
            data_df = pd.read_csv(file_path)
        elif file_extension in ('.xls', '.xlsx'):
            data_df = pd.read_excel(file_path)
        else:
            error_dic['file type'] = '不支持 %s 净值文件类型' % file_extension
    except:
        error_dic['file read'] = '文件内容读取失败'
        return data_df if ret_df else data_list, [error_info for _, error_info in error_dic.items()]

    if data_df.shape[0] == 0:
        error_dic['data len'] = '数据表为空'
        return data_df if ret_df else data_list, [error_info for _, error_info in error_dic.items()]

    col_name_list = list(data_df.columns)
    data_list = data_df.to_dict('record')
    # 字段检查
    col_name_list = list(data_df.columns)
    missing_col_list = []
    for col_name in {'基金代码', '基金名称', '日期', '单位净值', '累计净值', '份额分红', '金额分红'}:
        if col_name not in col_name_list:
            missing_col_list.append(col_name)
    if len(missing_col_list) > 0:
        error_dic['data col'] = '当前数据缺少字段 %s' % missing_col_list

    # 基金代码检查
    engine = get_db_engine()
    sql_str = "select wind_code_s, sec_name_s from fund_essential_info"
    with get_db_session(engine) as session:
        wind_code_name_dic = dict(session.execute(sql_str).fetchall())
    for data_dic in data_list:
        wind_code = data_dic['基金代码']
        if wind_code not in wind_code_name_dic:
            error_dic[wind_code] = "%s 不是有效的基金代码" % wind_code
        if wind_code not in wind_code_name_dic:
            error_dic[wind_code + '_code'] = "%s %s 在基金要素表中不存在，请添加相应的基金要素信息或修改成已存在的基金要素" % (wind_code, data_dic['基金名称'])
        elif wind_code_name_dic[wind_code] != data_dic['基金名称']:
            error_dic[wind_code + '_name'] = "%s 与 %s 不匹配" % (wind_code, data_dic['基金名称'])
        try:
            if math.isnan(data_dic['单位净值']):
                error_dic[wind_code + '_nav'] = "%s 基金净值为空或不是有效数字" % wind_code
        except:
            error_dic[wind_code + '_nav'] = "%s 基金净值不是有效数字" % wind_code
        try:
            if math.isnan(data_dic['累计净值']):
                error_dic[wind_code + '_navacc'] = "%s 累计净值为空或不是有效数字" % wind_code
        except:
            error_dic[wind_code + '_navacc'] = "%s 累计净值不是有效数字" % wind_code
        if not isinstance(data_dic['日期'], pd.datetime):
            error_dic[wind_code + '日期'] = "%s 日期%s不是有效日期值" % (wind_code, data_dic['日期'])

    return data_df if ret_df else data_list, [error_info for _, error_info in error_dic.items()]


def import_fund_nav_multi(file_path, mode='delete_insert'):
    """
    根据excel文件 上传基金净值
    文件格式包含表头
    表头格式：基金代码	基金名称	日期	单位净值	累计净值	金额分红  份额分红
    基金代码，基金名称之间需要对应
    :param file_path:
    :param mode:
    :return:
    """
    # 文件检查
    data_df, error_list = check_fund_nav_multi(file_path, ret_df=True)
    if error_list is not None and len(error_list) > 0:
        raise ValueError('\n'.join(error_list))
    imp_data_df = data_df.rename(columns={'基金代码': 'wind_code',
                                          '基金名称': 'sec_name',
                                          '日期': 'nav_date',
                                          '单位净值': 'nav',
                                          '累计净值': 'nav_acc',
                                          '份额分红': 'share',
                                          '金额分红': 'cash'})
    imp_data_df = imp_data_df[['wind_code', 'nav_date', 'nav', 'nav_acc']]
    update_fund_nav_df(imp_data_df, mode=mode)


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s: %(levelname)s [%(name)s] %(message)s')
    # file_path = r'd:\Downloads\nav_file.xlsx'
    # 'XT1522529.XT' 明溪1号
    # file_path = 'd:\Downloads\FOF一期净值.xlsx'
    # replace_insert delete_insert

    # wind_code = 'XT1612348.XT'
    # file_path = r'd:\Works\F复华投资\L路演、访谈、评估报告\开拓者-复华-量化CTA私募基金【每日净值20170428】.xlsx'

    # 文件上传方式更新基金净值
    # wind_code = 'XT1605537.XT'
    # file_path = r'd:\Works\F复华投资\L路演、访谈、评估报告\杉树欣欣 2017-05-16.xlsx'
    # wind_code = 'FHF-XM20170322A'
    # file_path = r'D:\Backup\WeChat Files\WeChat Files\Mr_MarsDog\Files\新萌量化1103.xlsx'
    # update_fundnav_by_file(wind_code, file_path)  # , mode='replace_insert'

    # 鑫隆FOF 一期、二期净值
    # wind_code = 'FHF101601'
    # wind_code = 'FHB101701'

    # file_path = r"Z:\投后管理\产品净值\FOF基金\产品净值\FOF二期净值.xlsx"
    # import_fund_nav_fof2(file_path)

    # file_path = r"Z:\投后管理\产品净值\FOF基金\产品净值\FOF一期净值.xlsx"
    # import_fund_nav_fof1(file_path)

    # excel文件检查
    file_path = r'd:\Works\F复华投资\FOF管理系统\净值计算\基金净值上传用文件2017-11-16.xlsx'
    data_dict, error_list = check_fund_nav_multi(file_path)
    import_fund_nav_multi(file_path)
    # for err_info in error_list:
    #     print(err_info)
