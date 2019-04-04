# -*- coding: utf-8 -*-
"""
Created on Wed Apr  3 10:05:55 2019

@author: guosen
"""
import os
import sqlalchemy
import yaml
import pandas as pd


with open(os.path.join(os.path.split(__file__)[0],'etc.yaml'),'r') as f:
    etc = yaml.load(f)
    
sql_etc = etc['sql_dailyquote']


def get_engine():
    engine=sqlalchemy.create_engine('mssql+pymssql://%s:%s@%s/%s'%(sql_etc['user'], sql_etc['password'], sql_etc['host'],sql_etc['db']))
    return engine


def get_raw_data(sql_str):
    engine = get_engine()
    raw_data = pd.read_sql(sql_str,engine)
    engine.dispose()
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
    if isinstance(start_date,int):
       start_date=str(start_date)
    if isinstance(end_date,int):
       end_date=str(end_date)
    
    sql_str='''
    SELECT 
    TradeDate,StockCode,[Open],[Close],High,Low,Vol,Amount,AdjFactor,TradeStatus 
    FROM BasicData.dbo.DailyQuote 
    WHERE StockCode='%s' and TradeDate>='%s' and TradeDate<='%s'
    '''%(stock_code,start_date,end_date)
    
    quote_data= get_raw_data(sql_str)
    quote_data=quote_data.sort_values('TradeDate',ascending = True)
    
    return quote_data

def get_daily_quote_index(index_code,start_date,end_date):
    '''
    获取指数日线行情。
    
    Parameters
    ----------
    stock_code
        指数代码
    star_date
        开始日期 yyyymmdd
    endd_ate
        结束日期 yyyymmdd
        
    Returns
    -------
    DataFrame
        若无数据,则返回None
    '''       
    if isinstance(start_date,int):
       start_date=str(start_date)
    if isinstance(end_date,int):
       end_date=str(end_date)
    
    
    sql_str='''
    SELECT 
    TradeDate,StockCode,[Open],[Close],High,Low,Vol,Amount  
    FROM BasicData.dbo.DailyQuote_Index  
    WHERE StockCode='%s' and TradeDate>='%s' and TradeDate<='%s'
    '''%(index_code,start_date,end_date)
    
    quote_data= get_raw_data(sql_str)
    quote_data=quote_data.sort_values('TradeDate',ascending = True)
    
    return quote_data

def get_daily_quote_SWInd(index_code,start_date,end_date):
    '''
    获取申万一级行业日线行情。
    
    Parameters
    ----------
    stock_code
        指数代码
    star_date
        开始日期 yyyymmdd
    endd_ate
        结束日期 yyyymmdd
        
    Returns
    -------
    DataFrame
        若无数据,则返回None
    '''       
    if isinstance(start_date,int):
       start_date=str(start_date)
    if isinstance(end_date,int):
       end_date=str(end_date)
    
    
    sql_str='''
    SELECT 
    TradeDate,StockCode,[Open],[Close],High,Low,Vol,Amount  
    FROM BasicData.dbo.DailyQuote_SWInd  
    WHERE StockCode='%s' and TradeDate>='%s' and TradeDate<='%s'
    '''%(index_code,start_date,end_date)
    
    quote_data= get_raw_data(sql_str)
    quote_data=quote_data.sort_values('TradeDate',ascending = True)
    
    return quote_data
