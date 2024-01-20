import unittest
import random
import datetime
from data import Data

class DataTestCase(unittest.TestCase):
    def setUp(self):
        self.data = Data()
        self.table = self.data.get('收盤價', 1000000)
        
    def compare(self, start_date, n):
        self.data.date = start_date
        t1 = self.data.get('收盤價', n).dropna(how='all', axis=1)
        t2 = self.table[:start_date].iloc[-n:].dropna(how='all', axis=1)
        
        self.assertEqual(set(t1.columns), set(t2.columns))
        self.assertEqual(set(t1.index), set(t2.index))
        
        c1 = (t1 == t2).sum().sum() + t1.isnull().sum().sum()
        c2 = len(t1.columns)*len(t1)
        self.assertEqual(c1, c2)
        
    def test_length(self):
        self.data.date = datetime.date(2018,3,1)
        for n in range(1, 20):
            self.assertEqual(len(self.data.get('收盤價', n)), n)
            
    def test_final_date(self):        
        for d in self.table.index[-20:]:
            self.data.date = d
            t = self.data.get('收盤價', 2)
            self.assertEqual(t.index[-1], d)
    
    def test_same(self):
        for i in range(1, 20):
            cnt = random.randint(0, len(self.table))
            self.compare(self.table.index[cnt], i)
            
    def test_cache(self):
        data1 = Data()
        data2 = Data()
        data1.cache = False
        data2.cache = True
        
        for i in range(1, 20):
            cnt = random.randint(0, len(self.table))
            
            t1 = data1.get('收盤價', i)
            t2 = data2.get('收盤價', i)
            
            self.assertEqual(set(t1.columns), set(t2.columns))
            self.assertEqual(set(t1.index), set(t2.index))

            c1 = (t1 == t2).sum().sum() + t1.isnull().sum().sum()
            c2 = len(t1.columns)*len(t1)
            self.assertEqual(c1, c2)
        
            
if __name__ == '__main__':
    unittest.main()