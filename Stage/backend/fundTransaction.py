# uploads file
# .....
import pandas as pd
import logging
from fof_app.models import db, FUND_TRANSACTION, FUND_ESSENTIAL, FOF_FUND_PCT, FoFModel
import os
from sqlalchemy import and_

logger = logging.getLogger()


class Transaction(object):
    """
    关于基基金份额变动，看起来又是没啥卵用的功能
    需要接收一个上传的文件，检查文件的内容是否符合规则，写入数据库
    """

    def __init__(self, file=None):
        """
        :param file:
        """
        self.file = file
        self.column_name = ['wind_code_s', 'fof_name', 'sec_name_s', 'operating_type', 'accounting_date',
                            'request_date',
                            'confirm_date', 'confirm_benchmark', 'share', 'amount', 'description', 'total_amount',
                            'total_cost']
        self.operating_type = ["申购", "赎回", "返费", "现金分红", "分红再投资", "提取业绩报酬", "份额强增", "份额强减"]
        self.positive_type = ["申购", "分红再投资", "份额强增"]
        self.negative_type = ["赎回", "提取业绩报酬", "份额强减"]

    def formatFile(self):
        """
        把路径文件转换成DataFarme
        :return:
        """
        df = pd.read_excel(self.file)
        df = df.astype(object).where(pd.notnull(df), None)
        df.columns = self.column_name
        df = df.drop(['total_amount', 'total_cost'], axis=1)
        logger.info("共计{}条记录".format(len(df.index)))
        df_dict = df.T.to_dict()
        return df_dict

    def checkdfrole(self, df_dict):
        """
        各种蛋疼的条件，简直要吐了!!!!
        :param df_dict:
        :return:
        """
        errors = []
        for i, d in df_dict.items():
            fund = FUND_ESSENTIAL.query.filter(and_(FUND_ESSENTIAL.wind_code_s == d['wind_code_s'],
                                                    FUND_ESSENTIAL.sec_name_s == d['sec_name_s'])).first()
            if fund is None:
                errors.append("请检查第{}行基金要素代码{}".format(i + 1, d['wind_code_s']))
            if d['fof_name'] is None:
                errors.append("请检查第{}行FOF基金名称{}".format(i + 1, d['fof_name']))
            if d['sec_name_s'] is None:
                errors.append("请检查第{}行FOF基金名称{}".format(i + 1, d['sec_name_s']))
            if d['operating_type'] is not None:
                if d['operating_type'] not in self.operating_type:
                    errors.append("请检查第{}行操作类型{}".format(i + 1, d['operating_type']))
            else:
                errors.append("请检查第{}行操作类型不能为空".format(i + 1))
            if d['operating_type'] in ["申购", "赎回"]:
                if d['request_date'] is None:
                    errors.append("请检查第{}行申请日期不能为空".format(i + 1))
            if d['operating_type'] == "赎回" and d['confirm_benchmark'] is None:
                errors.append("请检查第{}行基金净值为空".format(i + 1))
            if d['confirm_date'] is None:
                errors.append("请检查第{}行确日期不能为空".format(i + 1))
            if d['share'] and d['amount'] is not None:
                if d['operating_type'] in self.positive_type and int(d['share']) < 0:
                    errors.append("请检查第{}行{}份额{}有误".format(i + 1, d['operating_type'], d['share']))
                if d['operating_type'] in self.negative_type and int(d['share']) > 0:
                    errors.append("请检查第{}行{}份额{}有误".format(i + 1, d['operating_type'], d['share']))
                if d['operating_type'] == "申购" and int(d['amount']) > 0:
                    errors.append("请检查第{}行{}份额{}有误".format(i + 1, d['operating_type'], d['amount']))
                if d['operating_type'] in ["赎回", "返费", "现金分红"] and int(d['amount']) < 0:
                    errors.append("请检查第{}行{}份额{}有误".format(i + 1, d['operating_type'], d['amount']))
            else:
                errors.append("请检查第{}行确 jiner不能为空".format(i + 1))
        return errors

    def importDate(self, df_dict):
        """
        write data to db
        :return:
        """
        for _, v in df_dict.items():
            record = FUND_TRANSACTION(**v)
            db.session.add(record)
            db.session.commit()


if __name__ == "__main__":
    from fof_app import create_app

    env = os.environ.get('APP_ENV', 'dev')
    flask_app = create_app('fof_app.config.%sConfig' % env.capitalize())
    with flask_app.test_request_context():
        db.init_app(flask_app)
        file = "/home/hd/Downloads/基金交易记录管理导入模板.xlsx"
        x = Transaction(file)
        data = x.formatFile()
        x.importDate(data)
        db.session.remove()
