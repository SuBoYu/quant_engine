import os
import requests
import shioaji as sj
from dotenv import load_dotenv
from quantxlab.data import Data
from datetime import datetime
from dateutil.relativedelta import relativedelta
from quantxlab.strategies.QuantX_strategy import strategy


def re_score_hoilding(apiKeySecretDict: dict, linebot: list, test=True):

    stockListPosition = []
    for i in range(len(apiKeySecretDict["key"])):
        print("This is the " + str(i) + "th account")
        api = sj.Shioaji()
        api.login(api_key=apiKeySecretDict['key'][i],
                  secret_key=apiKeySecretDict['secret'][i])

        list_positions = api.list_positions(api.stock_account)

        for position in list_positions:
            if str(position.code) == "8069":
                continue
            #print(position.code)
            stockListPosition.append(str(position.code))

    # 策略清單
    day = datetime.now().date()
    # 如果是每月的5~9號就採用那個月10號的月報，其餘用上一次的月報
    # 季報也是從季報開出來前五天就採用最新的季報，其餘用上一次的季報
    if 4 < day.day < 10:
        diff = 10 - day.day
        day += relativedelta(days=diff)
    if day.month == 3 and 26 < day.day < 31:
        diff = 31 - day.day
        day += relativedelta(days=diff)

    if day.month == 5 and 10 < day.day < 15:
        diff = 15 - day.day
        day += relativedelta(days=diff)

    if day.month == 8 and 9 < day.day < 14:
        diff = 14 - day.day
        day += relativedelta(days=diff)
    if day.month == 11 and 9 < day.day < 14:
        diff = 14 - day.day
        day += relativedelta(days=diff)

    data = Data(day)

    #if selectall => return all stock in the market
    #else => select stock in the position
    stockListAll = strategy(data, earlySellMode = True)

    message = "\nReScore Holding: \n"

    Res = []
    for i in range(len(stockListPosition)):
        try:
            Res.append(str(stockListPosition[i])+" : "+str(stockListAll[stockListPosition[i]]))
        except:
            #print("Missing",stockListPosition[i], "in rescoring.")
            message += "Missing "+str(stockListPosition[i])+" in rescoring.\n"


    #sort by value
    Res.sort(key=lambda x: x.split(":")[1], reverse=True)

    for stock in Res:
        message += stock+ "\n"

    print(message)

    if not test:
        for token in linebot:
            line_notify_message(token, message)
        
def line_notify_message(token, msg):
    headers = {
        "Authorization": "Bearer " + token,
        "Content-Type": "application/x-www-form-urlencoded"
    }

    payload = {'message': msg}
    r = requests.post("https://notify-api.line.me/api/notify", headers=headers, params=payload)

    return r.status_code

if __name__ == '__main__':

    Test = True

    load_dotenv()

    dev_token = os.getenv("dev_token")

    apiKeySecretDict = {"key": [], "secret": []}

    apiKeySecretDict["key"].append(os.getenv("api_key_Tony"))
    apiKeySecretDict["secret"].append(os.getenv("secret_key_Tony"))

    apiKeySecretDict["key"].append(os.getenv("api_key_Jason"))
    apiKeySecretDict["secret"].append(os.getenv("secret_key_Jason"))

    re_score_hoilding(apiKeySecretDict, [dev_token], test = Test)