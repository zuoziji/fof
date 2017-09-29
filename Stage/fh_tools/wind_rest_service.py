import pandas as pd
import requests
import json
from datetime import datetime, date
STR_FORMAT_DATE = '%Y-%m-%d'
STR_FORMAT_DATETIME_WIND = '%Y-%m-%d %H:%M:%S'  # 2017-03-06 00:00:00.005000
UN_AVAILABLE_DATETIME = datetime.strptime('1900-01-01', STR_FORMAT_DATE)
UN_AVAILABLE_DATE = UN_AVAILABLE_DATETIME.date()
def format_datetime_to_str(dt):
    if dt is None:
        return None
    dt_type = type(dt)
    if dt_type == str:
        return dt
    elif dt_type == date:
        if dt > UN_AVAILABLE_DATE:
            return dt.strftime(STR_FORMAT_DATE)
        else:
            return None
    elif dt_type == datetime:
        if dt > UN_AVAILABLE_DATETIME:
            return dt.strftime(STR_FORMAT_DATE)
        else:
            return None
    else:
        return dt
class APIError(Exception):
    def __init__(self, status):
        self.status = status
    def __str__(self):
        return "APIError:status={}".format(self.status)
class WindRest:
    def __init__(self, url_str):
        self.url = url_str
        self.header = {'Content-Type': 'application/json'}
    def _url(self, path: str) -> str:
        return self.url + path
    def public_post(self, path: str, req_data: str) -> list:
        # print('self._url(path):', self._url(path))
        ret_data = requests.post(self._url(path), data=req_data, headers=self.header)
        ret_dic = ret_data.json()
        if ret_data.status_code != 200:
            raise APIError('POST / {} {}'.format(ret_data.status_code, str(ret_dic)))
        else:
            return ret_data.status_code, ret_dic
    def wset(self, table_name, options):
        path = 'wset/'
        req_data_dic = {"table_name": table_name, "options": options}
        req_data = json.dumps(req_data_dic)
        _, json_dic = self.public_post(path, req_data)
        df = pd.DataFrame(json_dic).T
        return df
    def wss(self, codes, fields, options=""):
        path = 'wss/'
        req_data_dic = {"codes": codes, "fields": fields, "options": options}
        req_data = json.dumps(req_data_dic)
        _, json_dic = self.public_post(path, req_data)
        df = pd.DataFrame(json_dic).T
        return df
    def wsd(self, codes, fields, begin_time, end_time, options=""):
        path = 'wsd/'
        req_data_dic = {"codes": codes, "fields": fields,
                        "begin_time": format_datetime_to_str(begin_time),
                        "end_time": format_datetime_to_str(end_time),
                        "options": options}
        req_data = json.dumps(req_data_dic)
        _, json_dic = self.public_post(path, req_data)
        df = pd.DataFrame(json_dic).T
        return df
    def tdaysoffset(self, offset, begin_time, options=""):
        path = 'tdaysoffset/'
        req_data_dic = {"offset": offset,
                        "begin_time": format_datetime_to_str(begin_time),
                        "options": options}
        req_data = json.dumps(req_data_dic)
        _, json_dic = self.public_post(path, req_data)
        date_str = json_dic['Date']
        return date_str
if __name__ == "__main__":
    url_str = "http://10.0.5.110:5000/wind/"
    # url_str = "http://10.0.3.66:5000/wind/"
    rest = WindRest(url_str)
    # data_df = rest.wset(table_name="sectorconstituent", options="date=2017-03-21;sectorid=1000023121000000")
    data_df = rest.wss(codes="QHZG160525.OF", fields="fund_setupdate,fund_maturitydate,fund_mgrcomp,fund_existingyear,fund_ptmyear,fund_type,fund_fundmanager")
    # data_df = rest.wsd("600123.SH", "close,pct_chg", "2017-01-04", "2017-02-28", "PriceAdj=F")
    print(data_df)
    # date_str = rest.tdaysoffset(1, '2017-3-31')
    # print(date_str)