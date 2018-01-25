import six
import abc
import logging
from sqlalchemy import and_
from fof_app.models import FUND_TRANSACTION, FUND_NAV, FUND_ESSENTIAL, db
from backend.data_handler import get_fund_nav_by_wind_code
from functools import reduce
from pandas import DataFrame
import pandas as pd

logger = logging.getLogger()


@six.add_metaclass(abc.ABCMeta)
class CalcBase(object):
    @classmethod
    def from_operater(cls, name, fund, target):
        for sub_cls in cls.__subclasses__():
            oper = sub_cls.__oper__(cls)
            if name in oper:
                return sub_cls(fund, target)

    @abc.abstractclassmethod
    def calc(self):
        """"""


def query_batch(target):
    """

    :param target:
    :return:
    """
    s = ["分红再投资", "现金分红", "份额强增", "份额强减"]
    result = []
    batch = FUND_ESSENTIAL.query.filter(FUND_ESSENTIAL.wind_code == target.wind_code).all()
    for i in batch:
        recent_record = FUND_TRANSACTION.query.filter(and_(FUND_TRANSACTION.wind_code_s == i.wind_code_s,
                                                           FUND_TRANSACTION.total_share > 0,
                                                           FUND_TRANSACTION.confirm_date == target.nav_date)).order_by(
            FUND_TRANSACTION.confirm_date.desc()).all()
        if len(recent_record) > 1:
            for e in recent_record:
                r_dict = e.as_dict()
                r_dict['market_cap'] = r_dict['total_share'] * target.nav
                logger.info(
                    "批次-{}-当前总份额{},基金{}最新净值{}".format(r_dict['sec_name_s'], r_dict['total_share'], target.wind_code,
                                                      target.nav))
                result.append(r_dict)
            yield result
        else:
            r = FUND_TRANSACTION.query.filter(and_(FUND_TRANSACTION.wind_code_s == i.wind_code_s,
                                                   FUND_TRANSACTION.total_share > 0,
                                                   FUND_TRANSACTION.confirm_date <= target.nav_date)).order_by(
                FUND_TRANSACTION.confirm_date.desc()).first()
            if r is not None:
                r_dict = r.as_dict()
                r_dict['market_cap'] = r_dict['total_share'] * target.nav
                logger.info(
                    "批次-{}-当前总份额{},基金{}最新净值{}".format(r_dict['sec_name_s'], r_dict['total_share'], target.wind_code,
                                                      target.nav))
                yield r_dict


class InitTarget(object):
    def __init__(self, fund, target):

        self.target = target
        self.fund = fund
        self.recent_nav = self._last_nav()
        self.recent_batch_nav = self._last_batch_nav()
        self.normalized_nav = 1.0000
        self.return_data = {"share": self.target['total_share'],
                            "market_cap": self.target['market_cap'],
                            "normalized_nav": self.normalized_nav,
                            "wind_code": self.target['wind_code_s'],
                            "sec_name_s": self.target['sec_name_s'],
                            'operating_type': self.target['operating_type'],
                            "confirm_date": self.target["confirm_date"]}
        self.normal = True if self.fund.nav_date != self.target['confirm_date'] else False

    def _last_nav(self):
        last_nav = FUND_NAV.query.filter(and_(FUND_NAV.wind_code==self.fund.wind_code,
                                              FUND_NAV.nav_date < self.fund.nav_date)).order_by(
            FUND_NAV.nav_date.desc()).first()
        return last_nav

    def _last_batch_nav(self):
        last_nav = FUND_NAV.query.filter(and_(FUND_NAV.wind_code==self.target['wind_code_s'],
                                              FUND_NAV.nav_date < self.fund.nav_date
                                              )).order_by(
            FUND_NAV.nav_date.desc()).first()
        return last_nav

    def normal_cal(self):
        logger.info("use normal cal method")
        if self.recent_batch_nav:
            print(self.fund.nav,"fund_nav")
            print(self.recent_nav.nav,"recent_nav")
            print(self.recent_batch_nav.nav,"recent_batch_nav")
            self.normalized_nav = self.fund.nav / self.recent_nav.nav * self.recent_batch_nav.nav
        logger.info("批次{} [{}] ,{} 归一后净值:{}".format(self.target['sec_name_s'], self.target['operating_type'],
                                                    self.fund.nav_date, self.normalized_nav))
        self.return_data['normalized_nav'] = self.normalized_nav
        return self.return_data


class Normal(CalcBase, InitTarget):
    def calc(self):
        return self.normal_cal()

    def __oper__(self):
        return ["申购", "赎回", "返费"]


class ShareDividends(CalcBase, InitTarget):
    def calc(self):
        if self.normal:
            return self.normal_cal()
        else:
            last_batch = FUND_TRANSACTION.query.filter(and_(
                FUND_TRANSACTION.wind_code_s == self.target['wind_code_s'],FUND_TRANSACTION.confirm_date < self.target['confirm_date'])).order_by(
                FUND_TRANSACTION.confirm_date.desc()).first()
            print(last_batch)
            batch_last_cap = last_batch.total_share * self.recent_nav.nav
            batch_new_cap = self.target['market_cap']
            logger.info("上一条资本记录{},最新资本记录{}".format(batch_last_cap, batch_new_cap))
            self.normalized_nav = batch_new_cap / batch_last_cap * self.recent_batch_nav.nav
            self.return_data['normalized_nav'] = self.normalized_nav
            logger.info("批次{} [{}] ,{} 归一后净值:{}".format(self.target['sec_name_s'], self.target['operating_type'],
                                                        self.fund.nav_date, self.normalized_nav))
            return self.return_data

    def __oper__(self):
        return ["分红再投资"]


class CashDividends(CalcBase, InitTarget):
    def calc(self):
        if self.normal:
            return self.normal_cal()
        else:
            last_batch = FUND_TRANSACTION.query.filter(and_(
                FUND_TRANSACTION.wind_code_s == self.target['wind_code_s'],FUND_TRANSACTION.confirm_date < self.target['confirm_date'])).order_by(
                FUND_TRANSACTION.confirm_date.desc()).first()
            batch_last_cap = last_batch.total_share * self.recent_nav.nav
            self.normalized_nav = (self.target['amount'] + self.target[
                'market_cap']) / batch_last_cap * self.recent_batch_nav.nav
            self.return_data['normalized_nav'] = self.normalized_nav
            logger.info("批次{} [{}] ,{} 归一后净值:{}".format(self.target['sec_name_s'], self.target['operating_type'],
                                                        self.fund.nav_date, self.normalized_nav))
            return self.return_data

    def __oper__(self):
        return ["现金分红"]


class SharePlusMinus(CalcBase, InitTarget):
    def calc(self):
        if self.normal:
            return self.normal_cal()
        else:
            last_batch = FUND_TRANSACTION.query.filter(and_(
                FUND_TRANSACTION.wind_code_s == self.target['wind_code_s'],
                FUND_TRANSACTION.confirm_date < self.target['confirm_date'])).order_by(
                FUND_TRANSACTION.confirm_date.desc()).first()
            batch_last_cap = last_batch.total_share * self.recent_nav.nav
            self.normalized_nav = self.target['market_cap'] / batch_last_cap * self.recent_batch_nav.nav
            self.return_data['normalized_nav'] = self.normalized_nav
            logger.info("批次{} [{}] ,{} 归一后净值:{}".format(self.target['sec_name_s'], self.target['operating_type'],
                                                        self.fund.nav_date, self.normalized_nav))
            return self.return_data

    def __oper__(self):
        return ["份额强增", "份额强减"]


class TaskCash(CalcBase, InitTarget):
    def calc(self):
        if self.normal:
            return self.normal_cal()
        else:
            last_batch = FUND_TRANSACTION.query.filter(and_(
                FUND_TRANSACTION.wind_code_s == self.target['wind_code_s'],
                FUND_TRANSACTION.confirm_date < self.target['confirm_date'])).order_by(
                FUND_TRANSACTION.confirm_date.desc()).first()
            this_cap = self.target['market_cap']
            this_amount = self.target['amount']
            last_nav = last_batch.total_share * self.recent_nav.nav
            last_total_share = last_batch.total_share
            this_share = self.target['share']
            self.normalized_nav = (this_cap - this_amount) / (last_nav * (last_total_share + this_share)) * last_nav
            self.return_data['normalized_nav'] = self.normalized_nav
            logger.info("批次{} [{}] ,{} 归一后净值:{}".format(self.target['sec_name_s'], self.target['operating_type'],
                                                        self.fund.nav_date, self.normalized_nav))
            return self.return_data

    def __oper__(self):
        return ["提取业绩报酬"]


class SpecialCal(object):
    def __init__(self, fund, target):
        self.target = target
        self.fund = fund
        self.wind_code = target[0]['wind_code_s']
        self.last_cap, self.last_share = self._last_batch()

    def calc(self):
        this_cap = self.target[-1]['total_share'] * self.fund.nav
        sum_value = reduce((lambda x, y: x + y), [i['amount'] for i in self.target])
        normalized_nav = (this_cap + sum_value) / self.last_share * self.last_cap
        return {"share": self.target[-1]['total_share'],
                "market_cap": self.target[-1]['market_cap'],
                "normalized_nav": normalized_nav,
                "wind_code": self.target[-1]['wind_code_s'],
                "sec_name_s": self.target[-1]['sec_name_s'],
                'operating_type': self.target[-1]['operating_type'],
                "confirm_date": self.target[-1]['confirm_date']}

    def _last_batch_nav(self):
        last_nav = FUND_NAV.query.filter(FUND_NAV.wind_code==self.wind_code).order_by(
            FUND_NAV.nav_date.desc()).first()
        return last_nav.nav

    def _last_batch(self):
        last_batch = FUND_TRANSACTION.query.filter_by(wind_code_s=self.wind_code).order_by(
            FUND_TRANSACTION.confirm_date.asc()).limit(len(self.target) + 1).first()
        last_share = last_batch.total_share
        last_cap = last_share * self._last_batch_nav()
        return last_cap, last_share


def query_recent_tr(wind_code: str, confirm_date: str) -> list:
    print(wind_code,confirm_date)
    tr_list = []
    tr = FUND_TRANSACTION.query.filter(
        and_(FUND_TRANSACTION.wind_code_s == wind_code, FUND_TRANSACTION.confirm_date == confirm_date)).first()
    if tr is None:
        tr = FUND_TRANSACTION.query.filter(
            and_(FUND_TRANSACTION.wind_code_s == wind_code, FUND_TRANSACTION.confirm_date < confirm_date)).order_by(
            FUND_TRANSACTION.confirm_date.desc()).first()
    tr_list.append(tr)
    tr_list = [i.as_dict() for i in tr_list if i is not None]
    print(tr_list)

    return tr_list

def query_range_tr(wind_code,start,end):
    tr = FUND_TRANSACTION.query.filter(
        and_(FUND_TRANSACTION.wind_code_s == wind_code, FUND_TRANSACTION.confirm_date > start,
             FUND_TRANSACTION.confirm_date <= end)).all()
    if len(tr) > 0:
        tr_list = [i.as_dict() for i in tr]
        return tr_list
    else:
        return None


if __name__ == "__main__":
    from fof_app import create_app
    import os

    env = os.environ.get('APP_ENV', 'dev')
    flask_app = create_app('fof_app.config.%sConfig' % env.capitalize())
    with flask_app.test_request_context():
        db.init_app(flask_app)
        r = FUND_NAV.query.filter_by(wind_code="XT1605537.XT").order_by(FUND_NAV.nav_date.desc()).first()
        prev_nav = FUND_NAV.query.filter(and_(FUND_NAV.wind_code == r.wind_code,
                                              FUND_NAV.nav_date < r.nav_date)).order_by(
            FUND_NAV.nav_date.desc()).first()
        for i in query_batch(r):
            if isinstance(i, list):
                x = SpecialCal(r, i)
            else:
                x = CalcBase.from_operater(i['operating_type'], r, i)
            value = x.calc()
            nav_record = get_fund_nav_by_wind_code(value['wind_code'], limit=5)
            if nav_record is not None:
                nav_record.reset_index(inplace=True)
                batch_acc = nav_record.to_dict(orient='records')
                if len(batch_acc) > 1:
                    start = batch_acc[-2]
                    end = batch_acc[-1]
                elif len(batch_acc) == 1:
                    start = batch_acc[0]
                    end = batch_acc[0]
                print([i['nav_date'] for i in batch_acc])
                for x in batch_acc:
                    tr = query_recent_tr(value['wind_code'],x['nav_date'])
                    print(x['nav_date'],tr)
                    #     pass
                # acc = [{"nav_acc": "%0.4f" % z['nav_acc'], "pct": "%0.4f" % z['pct'],
                #         "sec_name": i['sec_name_s'],
                #         "wind_code": i['wind_code_s'],
                #         "nav_date": z['nav_date'].strftime('%Y-%m-%d'), "nav": "%0.4f" % z['nav']} for z in
                #        batch_acc]
            # print(acc)
