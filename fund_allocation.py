import datetime as dt
import pandas_datareader as web

upperBound = 16000
lowerBound = 12000
# base 最少要下多少
base = 0.3

try:
    todate = dt.datetime.now()
    todate = todate.strftime("%Y-%m-%d")

    df = web.DataReader(name='^TWII', data_source='yahoo', start=todate, end=todate)
except:
    # get yesterday's close price
    try:
        todate = dt.datetime.now() - dt.timedelta(days=1)
        todate = todate.strftime("%Y-%m-%d")

        df = web.DataReader(name='^TWII', data_source='yahoo', start=todate, end=todate)
    except:
        # get close price of the day before yesterday
        try:
            todate = dt.datetime.now() - dt.timedelta(days=2)
            todate = todate.strftime("%Y-%m-%d")

            df = web.DataReader(name='^TWII', data_source='yahoo', start=todate, end=todate)
        except:
            pass
currentPrice = 13250

if currentPrice > upperBound:
    print("All time high, sell everything.")
    pass
elif currentPrice < lowerBound:
    print("All time low, buy everything.")
else:
    foundRatio = 0.5 + (1 - ((currentPrice - lowerBound) / (upperBound - lowerBound))) * 0.5
    print("We should use", round(foundRatio, 4) * 100, "% of our money to buy stocks.")
