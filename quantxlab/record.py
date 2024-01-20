import pickle
import os
import datetime
import requests
from io import StringIO
import pandas as pd
import numpy as np

class HistoryRecorder(object):
    
    def __init__(self, name='history.pkl'):
        self.name = name
        self.open_file()
       
    def open_file(self):
        if self.name in os.listdir():
            self.history = pickle.load(open(self.name, 'rb'))
        else:
            self.history = {'equality': [], 'date': [], 'position': []}
    
    def add(self, date, position):
        self.open_file()
        if self.history['date'] and date == self.history['date'][-1]:
            print('你的輸入日期好像不太對喔！', date, '這天已經存過檔囉！')
            return
        
        ddate = pd.to_datetime(date)
        self.history['equality'].append(self.__calculate_balance(ddate, position))        
        self.history['date'].append(date)
        self.history['position'].append(position)
        pickle.dump(self.history, open(self.name, 'wb'))
    
    def remove(self, date):
        self.open_file()
        if date not in self.history['date']:
            print('沒辦法刪除，因為找不到：', date)
        remove_index = self.history['date'].index(date)
        del self.history['date'][remove_index]
        del self.history['position'][remove_index]
        del self.history['equality'][remove_index]
        pickle.dump(self.history, open(self.name, 'wb'))
        
    def plot_equality(self):
        self.open_file()
        if len(self.history['date']) == 0:
            print('沒有歷史資料')
            return
        elif len(self.history['date']) == 1:
            print('只有一天：', self.history['date'][0], ' 您的總資產為', self.history['equality'][0])
            print('不畫圖（兩天以上才會畫圖）')
            return
        print('權益曲線（資產歷史紀錄）')
        pd.Series(self.history['equality'], index=pd.to_datetime(self.history['date'])).plot()
        
    def __calculate_balance(self, date, position):
        df = self.__crawler(date.year, date.month, date.day)
        stock_list1 = df['證券代號'].map(lambda v: v in list(position.keys()))
        stock_list2 = df['證券名稱'].map(lambda v: v in list(position.keys()))
        print('股票概況：')
        print(df[stock_list1 | stock_list2])
        v1 = (df[stock_list1]['證券代號'].map(lambda v: position[v]).astype(float) * df[stock_list1]['收盤價'].astype(float) * 1000).sum()
        v2 = (df[stock_list2]['證券名稱'].map(lambda v: position[v]).astype(float) * df[stock_list2]['收盤價'].astype(float) * 1000).sum()
        ret = v1 + v2
        if '帳戶餘額+交割金額' in position:
            ret += position['帳戶餘額+交割金額']
        return ret
    
    def __crawler(self, year, month, day):
        year_str = str(year)
        month_str = '0' + str(month) if month < 10 else str(month)
        day_str = '0' + str(day) if day < 10 else str(day)

        datestr = year_str + month_str + day_str
        
        print('開始下載股價:', datestr)
        r = requests.post('http://www.twse.com.tw/exchangeReport/MI_INDEX?response=csv&date=' + datestr + '&type=ALL')

        df = pd.read_csv(StringIO("\n".join([i.translate({ord(c): None for c in ' '}) 
                                         for i in r.text.split('\n') 
                                         if len(i.split('",')) == 17 and i[0] != '='])), header=0)
        print('下載成功')
        return df