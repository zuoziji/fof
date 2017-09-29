import pandas as pd
from pandas import DataFrame,Series
import json
# from werkzeug.contrib.cache import RedisCache
#
# cache = RedisCache(host="10.0.5.107", db=3,default_timeout=0)
#
#
#
#
# # key_list = cache.get('13')
# #
# # for i in key_list:
# #     print(i['primary'])
# #     print(i['child'])

#
# import pdfkit
#
# pdfkit.from_url('http://localhost:5000/f_app/batch', 'out.pdf')
#pdfkit.from_file('test.html', 'out.pdf')
#
# from pandas import DataFrame,Series
# import pandas as pd
# x = [{'value': 1231.0, 'name': 'FHF-101601C'},
#      {'value': 2000.0, 'name': 'XT1605537.XT'},
#      {'value': 930.0, 'name': 'FHF-101701D'},
#      {'value': 2222.0, 'name': 'FHF-101602'},
#      {'value': 1200.0, 'name': 'XT1518876.XT'},
#      {'value': 900.0, 'name': 'XT1521987.XT'},
#      {'value': 1900.0, 'name': 'XT1605537.XT'},{'value': 1900.0, 'name': 'XT1605537.XT'}]
# #
# # fund_set = { i['name'] for i in x }
# # for i in fund_set:
# #     data_dict = {}
# #     for t in x:
# #         value = 0
# #         if t['name'] == i:
# #             print(t['value'],i)
#
#
# df = DataFrame(x)
# g1 = df.groupby(["name"]).sum()
# print(g1['value'].to_dict())
#
# from random import choice
# from string import ascii_uppercase as uc, digits as dg
# #
# # print(dg+uc)
# #
# # # for i in range(200):
# # #     part1 = ''.join(choice(uc) for j in range(3))    # 三个大写的英文
# # #     part2 = ''.join(choice(dg) for j in range(3))    # 三个随机数字
# # #     part3 = ''.join(choice(dg + uc) for j in range(10))    # 十个随机大写字母或者数字
# # #     print(part1 + part2 + part3 + '\n')
# for i in range(10):
#     code = ''.join(choice(dg+uc) for x in range(8))
#     print(code )
