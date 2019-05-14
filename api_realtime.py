# -*- coding: utf-8 -*-
"""
Created on Tue Apr  2 10:43:57 2019

@author: ldh
"""

# api_realtime.py
import os
import json
import datetime as dt
import yaml
import pandas as pd
import redis

#with open('etc.yaml','r') as f: # 本地测试用语句
#    etc = yaml.load(f)
with open(os.path.join(os.path.split(__file__)[0],'etc.yaml'),'r') as f:
    etc = yaml.load(f)
    
redis_etc = etc['redis_realtime_min']
pool = redis.ConnectionPool(host = redis_etc['host'],
                port = redis_etc['port'], 
                db = redis_etc['db'],
                password = redis_etc['password'])


def get_min_realtime(stock_code):
    '''
    获取股票1分钟实时行情。
    
    Parameters
    ----------
    stock_code
        股票代码
        
    Returns
    -------
    DataFrame
        若当日无数据,则返回None
    '''       
    r = redis.Redis(connection_pool = pool) 
    data = r.hgetall(':'.join(['KLine','1Min',stock_code,dt.datetime.today().strftime('%Y%m%d')]))
    df = pd.DataFrame.from_records([json.loads(val) for key,val in data.items()])
    return df.sort_values('Time',ascending = True)
        


            



