import requests
from bs4 import BeautifulSoup
url =  'http://cwzx.shdjt.com/more.asp?gdmc=%D6%D0%D1%EB%BB%E3%BD%F0%D7%CA%B2%FA%B9%DC%C0%ED%D3%D0%CF%DE%D4%F0%C8%CE%B9%AB%CB%BE'
header = {
"Referer":"http://cwzx.shdjt.com/cwcx.asp?idid=184000"
}
data = requests.get(url,headers=header)
data.encoding = 'gb2312'
html = data.text
s = BeautifulSoup(html)
for i in  s.find_all('tr',{"height":"25"}):
    td = i.find_all('td')
    td_data = [ i.text for i in td]
    print(td_data)