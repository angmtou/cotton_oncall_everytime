# -*- coding: utf-8 -*-

"""
    writen by:     mua
    ver:     1.0
    date:     2017/06
    func：      catch  cftc ctotton on call data
    data from ： https://www.cftc.gov/MarketReports/CottonOnCall/index.htm

"""

import pandas as pd
import cx_Oracle

from datetime import datetime
from datetime import date
from sqlalchemy import create_engine

import os
# os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'
# os.environ['NLS_LANG']='AMERICAN_AMERICA.UTF8'      本地：IMPLIFIED CHINESE_CHINA.UTF8
os.environ['NLS_LANG']='SIMPLIFIED CHINESE_CHINA.UTF8'

import re

# 处理默认首页
url2="https://www.cftc.gov/MarketReports/CottonOnCall/index.htm"


data1=pd.read_html(io=url2)
print "\n\n\n-------------------BEGIN-------------------------------------\n\n"


data2 =pd.read_html(url2)[1]
data2

# data3=data2.loc[1:,:]
data3=data2.loc[1:14,:]

oncallstr=data3.iloc[0,4].split()[2]



oncalldate=datetime.strptime(oncallstr,'%m/%d/%Y')


data4=data3[2:]


data4.loc[:,7]=oncalldate
data4.loc[:,8]=datetime.now()
data4.loc[:,1:5]=data4.loc[:,1:6].astype(int)

# col1=["basedon",  "unfixcall"  , "changedsales" , "unfixpur" , "changedpur", "closedprice"   , "changedice"    , "CLOSEDATE"]
col1=["basedon",  "unfixcall"  , "changedsales" , "unfixpur" , "changedpur", "closedprice"   , "changedice"    , "CLOSEDATE",'inserttime']

data4.columns=col1
print data4.dtypes
print data4




# change to your oracle env ,if not error to oracle
engine = create_engine('oracle+cx_oracle://xxx:xxx@192.xxx/xx')
cnx = engine.connect()
data4.to_sql('ods_oncal',engine,if_exists='append',index=False)
cnx.close()  

print 'success\n'

print oncalldate

print "\n\n\n-------------------END-------------------------------------\n\n"