import datetime
import pandas as pd
import os
from quantxlab.data import Data
from quantxlab.strategies.QuantX_strategy import QuantxStrategy


def generate_monthly_seasonal_change_date(yearStart=2023, monthStart=1, yearEnd=2023, monthEnd=12):
    monthly_revenue_holdDays = []
    for y in range(yearStart, yearEnd + 1):
        if y == yearStart:
            ms = monthStart
        else:
            ms = 1
        if y == yearEnd:
            me = monthEnd
        else:
            me = 12
        for m in range(ms, me + 1):
            if m == 3:
                monthly_revenue_holdDays.append(datetime.date(y, 3, 11))
                monthly_revenue_holdDays.append(datetime.date(y, 4, 1))
            elif m == 5:
                monthly_revenue_holdDays.append(datetime.date(y, 5, 16))
            elif m == 8:
                monthly_revenue_holdDays.append(datetime.date(y, 8, 15))
            elif m == 11:
                monthly_revenue_holdDays.append(datetime.date(y, 11, 15))
            else:
                monthly_revenue_holdDays.append(datetime.date(y, m, 11))
    return monthly_revenue_holdDays


data = Data()

startTime = datetime.datetime.now()

# 回測

print("======== Backtesting Begin ========")

# 設定pd讓輸出不要省略
# 顯示所有列
pd.set_option('display.max_columns', None)
# 顯示所有行
pd.set_option('display.max_rows', None)
# 設置value的顯示長度为100，默認为50
pd.set_option('max_colwidth', 100)

# 設定回測參數
# 起始日期
yearStart = 2021
monthStart = 9
dayStart = 1
# 結束日期
yearEnd = 2023
monthEnd = 12
dayEnd = 5
# QuantX Strategy的換手日必須特別設定來回測
holdDays = [generate_monthly_seasonal_change_date(yearStart, monthStart, yearEnd, monthEnd)]
# 起始資金
funds = [80000000]
# path to save the result figures
file_saving_route = './result/QuantX_6/'
if not os.path.exists(file_saving_route):
    os.makedirs(file_saving_route)

# 設定策略
strategy = QuantxStrategy(data, params={"account_for": 6})

for fund in funds:
    # 不同換手長度回測
    for holdDay in holdDays:
        print(f"======== {yearStart}/{monthStart}/{dayStart} ~ {yearEnd}/{monthEnd}/{dayEnd} fund: {fund} ========")
        strategy.backtest(path_record=file_saving_route, start_date=datetime.date(yearStart, monthStart, dayStart),
                          end_date=datetime.date(yearEnd, monthEnd, dayEnd), hold_days=holdDay, capital=fund)

endTime = datetime.datetime.now()

print("Process time:", endTime - startTime)