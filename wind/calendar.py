# -*- coding: utf-8 -*-
"""
Created on Thu Nov 30 14:39:12 2017

@author: ldh
"""

# calendar.py

import os
import pickle
import pandas as pd

class Calendar():
    
    def __init__(self,start_date = '20100101',end_date = '20200101',
                 exchange = None,path = None):
        self.start_date = start_date
        self.end_date = end_date
        self.exchange = exchange
        self.path = path
        self.trade_calendar = None
        self.natural_calendar = None

        if self.path is not None:
            self.loads()
        else:
            self._fetch_calendar()
        
    def _fetch_calendar(self):
        from .wind_api import get_tdays
        if self.exchange:
            self.trade_calendar = get_tdays(self.start_date,self.end_date,
                                  TradingCalendar = self.exchange)
        else:
            self.trade_calendar = get_tdays(self.start_date,self.end_date)
            
        self.natural_calendar = pd.Series(pd.date_range(start=self.start_date,
                                              end=self.end_date))
        
    def move(self,date,n = None,days = None,months = None,years = None):
        '''
        根据交易日历移动date.
        
        Parameters
        ----------
        date
            datetime
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
        
    def persist(self,path):
        '''
        保存日历到指定路径.
        
        Parameters
        ----------
        path
            保存路径
        '''
        self.path = path
        status = {'start_date':self.start_date,
                 'end_date':self.end_date,
                 'path':self.path}
        self.trade_calendar.to_excel(os.path.join(self.path,'calendar_data.xlsx'))
        with open(os.path.join(self.path,'calendar_status.pkl'),'wb') as f:
            pickle.dump(status,f)
            
    def loads(self):
        '''
        从指定路径读取日历。
        
        Parameters
        ----------
        path
            读取路径        
        '''
        self.trade_calendar = pd.read_excel(os.path.join(self.path,'calendar_data.xlsx'))[0]
        with open(os.path.join(self.path,'calendar_status.pkl'),'rb') as f:
            status = pickle.load(f)        
        self.start_date = status['start_date']
        self.end_date = status['end_date']
        self.path = status['path']
        
if __name__ == '__main__':
    a = Calendar('20010101','20180515')
    