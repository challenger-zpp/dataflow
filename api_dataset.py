# -*- coding: utf-8 -*-
"""
Created on Wed Apr 10 17:02:20 2019

@author: ldh
"""

# api_dataset.py

import os
import sqlalchemy
import yaml
import pandas as pd

#with open('etc.yaml','r') as f: # 本地测试用语句
#    etc = yaml.load(f)
with open(os.path.join(os.path.split(__file__)[0],'etc.yaml'),'r') as f:
    etc = yaml.load(f)
    
sql_etc = etc['sql_wind']    

def get_engine():
    
    engine=sqlalchemy.create_engine('mssql+pymssql://%s:%s@%s/%s?charset=cp936'%(sql_etc['user'], sql_etc['password'], sql_etc['host'],sql_etc['db']))#,encoding='utf-8',convert_unicode=True)
    return engine


def get_raw_data(sql_str):
    engine = get_engine()
    raw_data = pd.read_sql(sql_str,engine)
    engine.dispose()
    return raw_data  
  
def get_dataset(data_set,date):
    '''
    获取数据集.
    
    Parameters
    ----------
    data_set
        数据集,'A'为全A
    date
        获取日期 yyyymmdd

        
    Returns
    -------
    DataFrame
        若无数据,则返回None    
    '''
    
    if data_set == 'A':
        sql_str = '''
        SELECT
        S_INFO_WINDCODE as wind_code,
        S_INFO_CODE as stock_code,
        S_INFO_NAME as stock_name,
        S_INFO_LISTDATE as list_date
        FROM AShareDescription
        WHERE S_INFO_LISTDATE <= '%s' AND ( S_INFO_DELISTDATE > '%s' OR S_INFO_DELISTDATE is NULL)
        '''%(date,date)
    
    quote_data = get_raw_data(sql_str)
    return quote_data

if __name__ == '__main__':
    all_a = get_dataset('A','20190409')