import pandas as pd

# 1. 最近股價波動越小越好（最大值/最小值 由小排到大）
# 2. 「月營收年增率」越高越好（由大排到小）
# 3. 前一個月的「月營收年增率」越高越好（由大排到小）
# 4. ROE年增率越大越好
# 接下來針對每檔股票，將這四種不同的排名累加，再取一次排名，找前十名來做投資！

def strategy(data):
    close = data.get("收盤價", 120)

    condition1 = close.max() / close.min()
    rev = data.get("當月營收", 14)
    condition2 = (rev.iloc[-1] / rev.iloc[-13])
    condition3 = (rev.iloc[-2] / rev.iloc[-14])

    稅後淨利 = data.get('本期淨利（淨損）', 5)
    # 股東權益，有兩個名稱，有些公司叫做權益總計，有些叫做權益總額，所以得把它們抓出來
    權益總計 = data.get('權益總計', 5)
    權益總額 = data.get('權益總額', 5)
    # 並且把它們合併起來
    權益總計.fillna(權益總額, inplace=True)
    roe = 稅後淨利 / 權益總計
    condition4 = roe.iloc[-1] / roe.iloc[-5]

    select_stock = (condition1.rank() + # 該數值越小越好
        condition2.rank(ascending=False) + # 該值越大越好
        condition3.rank(ascending=False) + # 該值越大越好
        condition4.rank(ascending=False)   # 該值越大越好
    ).rank() <= 10

    return select_stock[select_stock]