# -*- coding: utf-8 -*-
"""
Created on Wed Apr  3 10:05:55 2019

@author: guosen
"""

import pymssql
import datetime as dt
import yaml
import pandas as pd

with open('etc.yaml','r') as f:
    etc = yaml.load(f)
sql_etc = etc['sql_dailyquote']


def get_sql_conn(sqlstr):
    conn=pymssql.connect(sql_etc['host'], sql_etc['user'], sql_etc['password'], sql_etc['db']) 
    rawdata=pd.read_sql(sqlstr,conn)
    conn.close()
    return rawdata


def get_daily_quote(stock_code,startdate,enddate):
    '''
    获取股票日线行情。
    
    Parameters
    ----------
    stock_code
        股票代码
    stardate
        开始日期 yyyymmdd
    enddate
        结束日期 yyyymmdd
        
    Returns
    -------
    DataFrame
        若无数据,则返回None
    '''       
    
    
    sqlstr='''
    select TradeDate,StockCode,Open,Close,High,Low,Vol,Amount,AdjFactor,TradeStatus 
    FROM BasicData.dbo.DailyQuote 
    where StockCode='%s' and TradeDate>='%s' and TradeDate<='%s'
    '''%(stock_code,startdate,enddate)
    
    quotedata=get_sql_conn(sqlstr)
    quotedata=quotedata.sort_values('TradeDate',ascending = True)
    
    return quotedata