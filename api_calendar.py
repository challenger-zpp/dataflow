# -*- coding: utf-8 -*-
"""
Created on Tue May 14 09:13:19 2019

@author: ldh
"""

# api_calendar.py

import os
import pandas as pd
import yaml
import sqlalchemy

#with open('etc.yaml','r') as f: # 本地测试用语句
#    etc = yaml.load(f)
with open(os.path.join(os.path.split(__file__)[0],'etc.yaml'),'r') as f:
    etc = yaml.load(f)
    
sql_etc = etc['sql_calendar']
engine = sqlalchemy.create_engine('mssql+pymssql://%s:%s@%s/%s'%(sql_etc['user'], sql_etc['password'], sql_etc['host'],sql_etc['db']))

class Calendar():
    
    def __init__(self,start_date = '20100101',end_date = '20200101'):
        self.start_date = start_date
        self.end_date = end_date
        self.trade_calendar = None
        self.natural_calendar = None

        self._fetch_calendar()
        
    def _fetch_calendar(self):
        
        trade_calendar = pd.read_sql('''
                                     SELECT trade_date FROM trade_calendar
                                     WHERE trade_date >= '{start_date}' AND
                                     trade_date <= '{end_date}'
                                     '''.format(start_date = self.start_date,
                                     end_date = self.end_date),
                                     engine)
        self.trade_calendar = trade_calendar['trade_date']                                        
        self.natural_calendar = pd.Series(pd.date_range(start=self.start_date,
                                              end=self.end_date))
        
    def move(self,date,n = None,days = None,months = None,years = None):
        '''
        根据交易日历移动date.
        
        Parameters
        ----------
        date
            datetime or str like YYYYMMDD,YYYY-MM-DD
        n
            int/float,根据日历移动的数量
        days
            int/float,移动天数,默认为None
        months
            int/float,移动月数,默认为None
        years
            int/float,移动年数,默认为None
            
        Returns
        --------
        datetime
        
        Notes
        -------
        正为未来,负为过去
        目前仅支持n.
        '''
        ind = self.trade_calendar[self.trade_calendar == date].index[0]
        
        if n:
            t_ind = ind + n
            return self.trade_calendar.iat[t_ind].to_pydatetime()
        
        if days:
            pass
        
        if months:
            pass
        
        if years:
            pass    
        
    def move_natural(self,date,n,direction = '+'):
        '''
        根据自然日历移动日期,返回按照要求的最近的交易日。
        
        Parameters
        -----------
        date
            datetime
        n
            int,根据日历移动的数量
        direction
            str,'+'代表自然日的正向交易日,'-'则是自然日的负向交易日
            
        Returns
        --------
        datetime
        
        Notes
        -------
        正为未来,负为过去        
        '''
        ind = self.natural_calendar[self.natural_calendar == date].index[0]        
        t_ind = ind + n
        t_date = self.natural_calendar[t_ind]
        
        the_date = self.trade_calendar.loc[self.trade_calendar == t_date]
        if len(the_date) != 0:
            return t_date.to_pydatetime()
        else:
            if direction == '+':
                dates = self.trade_calendar.loc[self.trade_calendar >= t_date]
                return dates.tolist()[0].to_pydatetime()
            elif direction == '-':
                dates = self.trade_calendar.loc[self.trade_calendar <= t_date]
                return dates.tolist()[0].to_pydatetime()
        
if __name__ == '__main__':
    a = Calendar('20010101','20180515')