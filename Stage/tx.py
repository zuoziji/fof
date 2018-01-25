from bs4 import BeautifulSoup
import requests
from urllib3 import request
url = "http://guba.eastmoney.com/list,000725.html"

t = requests.get(url)
soup = BeautifulSoup(t.text,"lxml")
mydivs = soup.find(attrs={"data-pager":True})
l = mydivs['data-pager']

l = l.split('|')
print(l[1])

"""select -sum(amount) from fund_transaction where wind_code_s='XT1605537.XT_1' and confirm_date<='2018-1-16' """

"""update fund_transaction, 
(select wind_code_s, -sum(amount) sum_cost from fund_transaction where wind_code_s='XT1605537.XT_1' and confirm_date<='2018-01-13')  calc_cost
set total_cost = sum_cost
where fund_transaction.wind_code_s = calc_cost.wind_code_s
and confirm_date = '2018-01-13'"""\


