# -*- coding: utf-8 -*-
"""
Created on Wed Apr  3 10:05:55 2019

@author: guosen
"""

import pymssql
import yaml
import pandas as pd

with open('etc.yaml','r') as f:
    etc = yaml.load(f)
    
sql_etc = etc['sql_dailyquote']


def get_raw_data(sql_str):
    conn = pymssql.connect(sql_etc['host'], sql_etc['user'], sql_etc['password'], sql_etc['db'],charset='cp936') 
    raw_data = pd.read_sql(sql_str,conn)
    conn.close()
    return raw_data


def get_daily_quote(stock_code,start_date,end_date):
    '''
    获取股票日线行情。
    
    Parameters
    ----------
    stock_code
        股票代码
    star_date
        开始日期 yyyymmdd
    endd_ate
        结束日期 yyyymmdd
        
    Returns
    -------
    DataFrame
        若无数据,则返回None
    '''       
    
    
    sql_str='''
    SELECT 
    TradeDate,StockCode,[Open],[Close],High,Low,Vol,Amount,AdjFactor,TradeStatus 
    FROM BasicData.dbo.DailyQuote 
    WHERE StockCode='%s' and TradeDate>='%s' and TradeDate<='%s'
    '''%(stock_code,start_date,end_date)
    
    quote_data= get_raw_data(sql_str)
    quote_data=quote_data.sort_values('TradeDate',ascending = True)
    
    return quote_data