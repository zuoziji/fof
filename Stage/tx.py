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
