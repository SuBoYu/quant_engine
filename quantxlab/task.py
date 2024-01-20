import os
import sqlite3
import pytz
from datetime import datetime, date, timedelta
from dateutil.relativedelta import relativedelta
from .crawler import table_latest_date, date_range, update_table, crawl_price, month_range, crawl_monthly_report, \
    season_range, crawl_finance_statement_by_date, crawl_benchmark_return, crawl_tw_total_pmi, crawl_tw_total_nmi, \
    crawl_tw_business_indicator, crawl_margin_balance, crawl_margin_transactions
import requests
from .data import Data
from .utlis import portfolio
from .shared_list import indu, indu_id
from quantxlab.strategies.QuantX_strategy import QuantxStrategy
from dotenv import load_dotenv
import shioaji as sj
import pandas as pd
from .shared_list import stock_not_for_quantx

desired_timezone = pytz.timezone('Asia/Taipei')

def crawl_data():
    conn = sqlite3.connect(os.path.join('data', "data.db"))
    fromd = table_latest_date(conn, "price")
    tod = datetime.now(desired_timezone).date()
    # update price table
    dates = date_range(fromd, tod)
    if len(dates) == 0:
        print('price no data to parse')
    else:
        update_table(conn, 'price', crawl_price, dates)

    # update monthly revenue table
    fromd = table_latest_date(conn, "monthly_revenue")
    tod = datetime.now(desired_timezone).date()
    if fromd.date() >= tod:
        fromd = fromd - relativedelta(months=1)
        tod = fromd
    # 過了10號還是爬一下上個月的月報
    else:
        fromd = fromd - relativedelta(months=1)
    dates = month_range(fromd, tod)
    if len(dates) == 0:
        print('monthly_revenue no data to parse')
    else:
        update_table(conn, 'monthly_revenue', crawl_monthly_report, dates)

    # update seasonal revenue table 季報更新時間就是3/1, 4/26, 7/26, 10/24
    tod = datetime.now(desired_timezone).date()
    dates = []
    if (tod.month == 4 and 26 <= tod.day <= 30) or (tod.month == 5 and 1 <= tod.day <= 15):
        dates = [datetime(tod.year, 5, 15).date()]
    elif (tod.month == 7 and 26 <= tod.day <= 31) or (tod.month == 8 and 1 <= tod.day <= 14):
        dates = [datetime(tod.year, 8, 14).date()]
    elif (tod.month == 10 and 24 <= tod.day <= 31) or tod.month == 11 and 1 <= tod.day <= 20:
        dates = [datetime(tod.year, 11, 14).date()]
    elif tod.month == 3 and 1 <= tod.day <= 31:
        dates = [datetime(tod.year, 3, 31).date()]
    if len(dates) == 0:
        print('finance_statement no data to parse')
    else:
        update_table(conn, 'finance_statement', crawl_finance_statement_by_date, dates)

    # update benchmark_return
    fromd = table_latest_date(conn, "benchmark_return")
    fromd = fromd.replace(day=1)
    tod = datetime.now(desired_timezone).date()
    dates = month_range(fromd, tod)
    if len(dates) == 0:
        print('benchmark_return no data to parse')
    else:
        update_table(conn, 'benchmark_return', crawl_benchmark_return, dates)

    # update tw_total_pmi tw_total_nmi
    # tw_total_pmi
    fromd = table_latest_date(conn, "tw_total_pmi")
    fromd = fromd - relativedelta(months=1)
    tod = datetime.now(desired_timezone).date()
    dates = month_range(fromd, tod)
    if len(dates) == 0:
        print('tw_total_pmi no data to parse')
    else:
        update_table(conn, "tw_total_pmi", crawl_tw_total_pmi, dates)

    # tw_total_nmi
    fromd = table_latest_date(conn, "tw_total_nmi")
    fromd = fromd - relativedelta(months=1)
    tod = datetime.now(desired_timezone).date()
    dates = month_range(fromd, tod)
    if len(dates) == 0:
        print('tw_total_nmi no data to parse')
    else:
        update_table(conn, "tw_total_nmi", crawl_tw_total_nmi, dates)

    # update tw_business_indicator
    fromd = table_latest_date(conn, "tw_business_indicator")
    fromd = fromd - relativedelta(months=1)
    tod = datetime.now(desired_timezone).date()
    dates = month_range(fromd, tod)
    if len(dates) == 0:
        print('tw_business_indicator no data to parse')
    else:
        update_table(conn, "tw_business_indicator", crawl_tw_business_indicator, dates)

    # update margin_balance
    fromd = table_latest_date(conn, "margin_balance")
    tod = datetime.now(desired_timezone).date()
    # update price table
    dates = date_range(fromd, tod)
    if len(dates) == 0:
        print('margin_balance no data to parse')
    else:
        update_table(conn, 'margin_balance', crawl_margin_balance, dates)

    # update margin_transactions
    fromd = table_latest_date(conn, "margin_transactions")
    tod = datetime.now(desired_timezone).date()
    # update price table
    dates = date_range(fromd, tod)
    if len(dates) == 0:
        print('margin_transactions no data to parse')
    else:
        update_table(conn, 'margin_transactions', crawl_margin_transactions, dates)

    return


def check_crawl_datanum(data: Data):
    price = data.get('price', '收盤價', 1)
    priceLatestDate = price.index[-1]
    priceDataNum = price.shape[1]

    monReport = data.get('monthly_revenue', "上月比較增減(%)", 1)
    monReportNum = monReport.shape[1]
    monReportLatestDate = monReport.index[-1]

    seasonReport = data.get('balance_sheet', "普通股股本", 1)
    seasonReportNum = seasonReport.shape[1]
    seasonReportLatestDate = seasonReport.index[-1]

    return priceLatestDate, priceDataNum, monReportLatestDate, monReportNum, seasonReportLatestDate, seasonReportNum


def calculate_account_financial_num(apiKeySecretDict: dict) -> dict:
    year = datetime.now(desired_timezone).year

    revenue_holdDays = []

    for m in range(1, 13):
        if m == 3:
            revenue_holdDays.append(date(year, 3, 12))
            revenue_holdDays.append(date(year, 4, 1))
        elif m == 5:
            revenue_holdDays.append(date(year, 5, 16))
        elif m == 8:
            revenue_holdDays.append(date(year, 8, 15))
        elif m == 11:
            revenue_holdDays.append(date(year, 11, 15))
        else:
            revenue_holdDays.append(date(year, m, 11))

    today = datetime.now(desired_timezone).strftime('%Y-%m-%d')
    # turn the today into datetime.date type
    today = datetime.strptime(today, '%Y-%m-%d').date()

    # check which interval of today in monthly_revenue_holdDays
    for i in range(len(revenue_holdDays)):
        if today <= revenue_holdDays[i]:
            begin = revenue_holdDays[i-1]
            break

    if today > date(year, 12, 11):
        begin = date(year, 12, 11)
    if today <= date(year, 1, 11):
        begin = date(year-1, 12, 11)

    begin = (begin + timedelta(days=1)).strftime('%Y-%m-%d')

    print("begin: ", begin)

    today = datetime.now(desired_timezone).strftime('%Y-%m-%d')

    accountBalance = 0
    totalCost = 0
    uCost = 0
    uPnl = 0
    rCost = 0
    rPnl = 0
    deliveryPayment = 0

    unRealizeddict = dict()
    realizeddict = dict()

    for i in range(len(apiKeySecretDict['key'])):
        print("This is the " + str(i) + "th account")
        api = sj.Shioaji()
        api.login(api_key=apiKeySecretDict['key'][i],
                  secret_key=apiKeySecretDict['secret'][i])

        accountBalance += api.account_balance().acc_balance
        # print("accountBalance: " + str(accountBalance))

        # 購買紀錄
        # .code => stock_id
        # .pnl  => profit and loss
        list_positions = api.list_positions(api.stock_account)

        noRPnl = False
        try:
            rPnl_list = api.list_profit_loss(api.stock_account, begin, today)
            rPnl_list = pd.DataFrame(pnl.__dict__ for pnl in rPnl_list)
            rPnlGrouped = rPnl_list.groupby('code').sum(numeric_only=False)
            rPnlGrouped = rPnlGrouped.reset_index()

        except:
            # set rPnlGrouped to be empty if there is no deliveryPayment
            rPnlGrouped = pd.DataFrame(columns=['code', 'quantity', 'price', 'pnl'])
            noRPnl = True

        # deliveryPayment
        settlements = api.settlements(api.stock_account)

        # T = 1,2 (T+1, T+2)
        for i in range(1, len(settlements)):
            deliveryPayment += settlements[i].amount

        # stock not for quantx deliverpayment ignore
        stock_not_for_quantx_deliverpayment = 0


        # Unrealized PnL
        for position in list_positions:
            if str(position.code) in stock_not_for_quantx:
                continue
            uPnl += position.pnl
            u_position_cost = position.quantity * position.price * 1000
            uCost += u_position_cost
            if str(position.code) not in unRealizeddict:
                unRealizeddict[str(position.code)] = [position.pnl, u_position_cost]
            else:
                unRealizeddict[str(position.code)][0] += position.pnl
                unRealizeddict[str(position.code)][1] += u_position_cost

        # Realized PnL
        if not noRPnl:
            for row in rPnlGrouped.iterrows():
                if row[1]["code"] in stock_not_for_quantx:
                    continue
                # 計算買入成本
                r_position_cost = 0
                for each_sell_row in rPnl_list[rPnl_list["code"] == row[1]["code"]].iterrows():
                    profitloss_detail = api.list_profit_loss_detail(api.stock_account, each_sell_row[1]["id"])
                    for each_detail in profitloss_detail:
                        r_position_cost += each_detail["cost"]
                if str(row[1]['code']) not in realizeddict:
                    realizeddict[str(row[1]['code'])] = [row[1]['pnl'], r_position_cost]
                else:
                    realizeddict[str(row[1]['code'])][0] += row[1]['pnl']
                    realizeddict[str(row[1]['code'])][1] += r_position_cost

                rPnl += row[1]['pnl']
                rCost += r_position_cost

        api.logout()

    uNetWorth = uPnl + uCost
    totalPnl = uPnl + rPnl
    totalCost = uCost + rCost
    accountBalance = accountBalance + deliveryPayment
    netWorth = uNetWorth + accountBalance

    # remove dNetWorth
    return {"netWorth": netWorth, "totalPnl": totalPnl, "accountBalance": accountBalance, "totalCost": totalCost,
            "uNetWorth": uNetWorth, "uPnl": uPnl, "rPnl": rPnl, "unRealizeddict": unRealizeddict, "realizeddict": realizeddict,}


def re_evaluate_holding(apiKeySecretDict: dict, data, earlySellMonth, earlySellSeason) -> str:
    stockListPosition = []
    for i in range(len(apiKeySecretDict["key"])):
        print("This is the " + str(i) + "th account")
        api = sj.Shioaji()
        api.login(api_key=apiKeySecretDict['key'][i],
                  secret_key=apiKeySecretDict['secret'][i])

        list_positions = api.list_positions(api.stock_account)

        for position in list_positions:
            if str(position.code) in stock_not_for_quantx:
                continue
            stockListPosition.append(str(position.code))

    params = {"earlySellMonth":earlySellMonth, "earlySellSeason":earlySellSeason}
    strategy = QuantxStrategy(data, params)
    stockListAll = strategy.run()

    if earlySellMonth is True:
        message = "\nReEvaluate Holding based on monthly report: \n"
    if earlySellSeason is True:
        message = "\nReEvaluate Holding based on seasonal report: \n"

    Res = []
    for i in range(len(stockListPosition)):
        try:
            Res.append(str(stockListPosition[i]) + " : " + str(stockListAll[stockListPosition[i]]))
        except:
            message += "Missing " + str(stockListPosition[i]) + " in reevaluate.\n"

    # sort by value
    Res.sort(key=lambda x: x.split(":")[1], reverse=True)

    for stock in Res:
        message += stock + "\n"

    return message

def net_worth_ds(apiKeySecretDict: dict, test=True):
    financial_num = calculate_account_financial_num(apiKeySecretDict)

    message = "\nUnrealized Pnl:" + "\n"

    for k, v in financial_num["unRealizeddict"].items():
        message += (str(k) + " : " + str(v[0]) + "  " + str(
                    round((float(v[0]) / float(v[1])) * 100, 2)) + "%\n")

    message += "Total Unrealized Pnl: " + str(financial_num["uPnl"]) + "\n"

    message += "Realized Pnl:" + "\n"

    for k, v in financial_num["realizeddict"].items():
        message += (str(k) + " : " + str(v[0]) + "  " + str(
                    round((float(v[0]) / float(v[1])) * 100, 2)) + "%\n")

    message += "Total Realized Pnl: " + str(financial_num["rPnl"]) + "\n"

    totalPnl = financial_num["totalPnl"]
    roiOfAlgo = round(totalPnl * 100 / financial_num["totalCost"], 2)
    roiOfAllFund = round(totalPnl * 100 / (financial_num["netWorth"]), 2)
    remainCash = financial_num["accountBalance"]
    netWorth = financial_num["netWorth"]

    message += "Total Pnl : " + str(totalPnl) + "\n"
    # Total Cost隱藏起來避免混淆
    # message += "Total Cost : " + str(financial_num["totalCost"]) + "\n"
    message += "ROI of Algo : " + str(roiOfAlgo) + "%\n"
    message += "ROI of all fund : " + str(roiOfAllFund) + "%\n"
    message += "Remain Cash : " + str(remainCash) + "\n"
    message += "Net Worth : " + str(netWorth) + "\n"
    print(message)

    def get_last_date_from_csv(filename):
        with open(filename, 'r') as file:
            last_line = None
            for line in file:
                last_line = line
            if last_line:
                return last_line.strip().split(',')[-1]
        return None

    today = datetime.now(desired_timezone).strftime('%Y-%m-%d')
    # Data need to be record in the ActualPerformance.csv
    data_to_append = [str(totalPnl), str(roiOfAlgo) + "%", str(roiOfAllFund) + "%", str(financial_num["uPnl"]),str(financial_num["rPnl"]),str(remainCash), str(netWorth),
                      str(today)]
    current_date = data_to_append[-1]

    if not os.path.isfile('ActualPerformance.csv'):
        print("No file found, create a new one")
        # Define the column name
        column_name = ['total_pnl', 'roi_algo', 'roi_fund','Unrealized_Pnl','Realized_Pnl','remain_cash', 'net_worth', 'date']
        df = pd.DataFrame(columns=column_name)
        df.to_csv('ActualPerformance.csv', index=False)
    else:
        df = pd.read_csv('ActualPerformance.csv')

    if get_last_date_from_csv('ActualPerformance.csv') == current_date:
        df.iloc[-1] = data_to_append
        print('Today\'s data has been updated.')
    else:
        df = df.append(pd.Series(data_to_append, index=df.columns), ignore_index=True)
        print('Today\'s data has been added.')

    df.to_csv('ActualPerformance.csv', index=False)

    if not test:
        return message

    print("Done")

    return

def net_worth_linenotify(apiKeySecretDict: dict, linebot: list, test=True):
    financial_num = calculate_account_financial_num(apiKeySecretDict)

    message = "\nUnrealized Pnl:" + "\n"

    for k, v in financial_num["unRealizeddict"].items():
        message += (str(k) + " : " + str(v[0]) + "  " + str(
                    round((float(v[0]) / float(v[1])) * 100, 2)) + "%\n")

    message += "Total Unrealized Pnl: " + str(financial_num["uPnl"]) + "\n"

    message += "Realized Pnl:" + "\n"

    for k, v in financial_num["realizeddict"].items():
        message += (str(k) + " : " + str(v[0]) + "  " + str(
                    round((float(v[0]) / float(v[1])) * 100, 2)) + "%\n")

    message += "Total Realized Pnl: " + str(financial_num["rPnl"]) + "\n"

    totalPnl = financial_num["totalPnl"]
    roiOfAlgo = round(totalPnl * 100 / financial_num["totalCost"], 2)
    roiOfAllFund = round(totalPnl * 100 / (financial_num["netWorth"]), 2)
    remainCash = financial_num["accountBalance"]
    netWorth = financial_num["netWorth"]

    message += "Total Pnl : " + str(totalPnl) + "\n"
    # Total Cost隱藏起來避免混淆
    # message += "Total Cost : " + str(financial_num["totalCost"]) + "\n"
    message += "ROI of Algo : " + str(roiOfAlgo) + "%\n"
    message += "ROI of all fund : " + str(roiOfAllFund) + "%\n"
    message += "Remain Cash : " + str(remainCash) + "\n"
    message += "Net Worth : " + str(netWorth) + "\n"
    print(message)

    def get_last_date_from_csv(filename):
        with open(filename, 'r') as file:
            last_line = None
            for line in file:
                last_line = line
            if last_line:
                return last_line.strip().split(',')[-1]
        return None

    today = datetime.now(desired_timezone).strftime('%Y-%m-%d')
    # Data need to be record in the ActualPerformance.csv
    data_to_append = [str(totalPnl), str(roiOfAlgo) + "%", str(roiOfAllFund) + "%", str(financial_num["uPnl"]),str(financial_num["rPnl"]),str(remainCash), str(netWorth),
                      str(today)]
    current_date = data_to_append[-1]

    if not os.path.isfile('ActualPerformance.csv'):
        print("No file found, create a new one")
        # Define the column name
        column_name = ['total_pnl', 'roi_algo', 'roi_fund','Unrealized_Pnl','Realized_Pnl','remain_cash', 'net_worth', 'date']
        df = pd.DataFrame(columns=column_name)
        df.to_csv('ActualPerformance.csv', index=False)
    else:
        df = pd.read_csv('ActualPerformance.csv')

    if get_last_date_from_csv('ActualPerformance.csv') == current_date:
        df.iloc[-1] = data_to_append
        print('Today\'s data has been updated.')
    else:
        df = df.append(pd.Series(data_to_append, index=df.columns), ignore_index=True)
        print('Today\'s data has been added.')

    df.to_csv('ActualPerformance.csv', index=False)

    if not test:
        for token in linebot:
            line_notify_message(token, message)

    print("Done")

    return


def line_notify_message(token, msg):
    headers = {
        "Authorization": "Bearer " + token,
        "Content-Type": "application/x-www-form-urlencoded"
    }

    payload = {'message': msg}
    r = requests.post("https://notify-api.line.me/api/notify", headers=headers, params=payload)

    return r.status_code

def stock_list_quantx_ds(pool: str, apiKeySecretDict: dict, investpercentage: float = 0.9, test=True):
    return_message = list()
    # 策略清單
    earlySellMonth = False
    earlySellSeason = False
    day = datetime.now(desired_timezone).date()
    # 如果是每月的1~9號就採用那個月10號的月報，其餘用上一次的月報
    # 季報也是從季報開出來前20天就採用最新的季報(3/1, 4/26, 7/26, 10/26)，其餘用上一次的季報
    # monthly_revenue early sell
    if 1 <= day.day < 10:
        diff = 10 - day.day
        day += relativedelta(days=diff)
    # early_sell month 可能10號星期五，所以往後推2天也要通知early_sell
    if 1 <= day.day <= 12:
        earlySellMonth = True
    # seasonal_report early sell
    if day.month == 3 and 1 <= day.day < 31:
        day = datetime(day.year, 3, 31).date()
        earlySellSeason = True
    if (day.month == 4 and 26 <= day.day <= 30) or (day.month == 5 and 1 <= day.day <= 15):
        day = datetime(day.year, 5, 15).date()
        earlySellSeason = True
    if (day.month == 7 and 26 <= day.day <= 31) or (day.month == 8 and 1 <= day.day <= 14):
        day = datetime(day.year, 8, 14).date()
        earlySellSeason = True
    if (day.month == 10 and 24 <= day.day <= 31) or (day.month == 11 and 1 <= day.day <= 14):
        day = datetime(day.year, 11, 14).date()
        earlySellSeason = True

    data = Data(day)

    print("Begin...")

    message = "\n{} price data num: {}\n{} monthly report data num: {}\n{} seasonal report data num: {}".format(
        *check_crawl_datanum(data))

    # early sale reevaluate

    if earlySellMonth:
        message += "\nEarly Sell Month\n"
        message += re_evaluate_holding(apiKeySecretDict, data, earlySellMonth, False)
    if earlySellSeason:
        message += "\nEarly Sell Season\n"
        message += re_evaluate_holding(apiKeySecretDict, data, False, earlySellSeason)

    netWorthQuantXPool = calculate_account_financial_num(apiKeySecretDict)["netWorth"]

    # 選股清單
    strategy = QuantxStrategy(data, params=dict())
    stock_list = strategy.run(capital=netWorthQuantXPool)
    # 要notify的info
    message += "\n" + datetime.now(desired_timezone).strftime('%Y-%m-%d') + "'s stock list:"
    eps = data.get('income_sheet', "基本每股盈餘合計", 1)
    for i in range(len(stock_list)):
        # 計算該產業季報eps開了幾%
        indu_k = eps.reindex(columns=indu_id[indu[str(stock_list.index[i])][1]]).dropna(axis=1)
        seasonal_report_announcement_percentage = int(
            len(indu_k.T) / len(indu_id[indu[str(stock_list.index[i])][1]]) * 100)

        message += "\n" + str(i + 1) + ". " + str(stock_list.index[i]) + " " + str(stock_list[i]) + " " + \
                   indu[str(stock_list.index[i])][0] + " " + indu[str(stock_list.index[i])][1] + " SRAP: " + \
                   str(seasonal_report_announcement_percentage) + "%"

    return_message.append(message)

    if pool == "main":
        # QuantX pool
        # 計算可投入資金
        netWorth = netWorthQuantXPool
        invest_money = netWorthQuantXPool
        total_invest_money = netWorthQuantXPool
        pre_total_invest_money = 0
        while total_invest_money <= netWorth:
            p, total_invest_money = portfolio(stock_list.index, invest_money, data)
            if pre_total_invest_money == total_invest_money:
                invest_money += 50000
                continue
            # 要notify的info
            message = "\n" + "下注比例 : {}% : ".format(round(total_invest_money / netWorth * 100))

            message += "\n\n" + "networth : " + str(netWorth)

            message += "\n\n" + "actual investing money : " + str(total_invest_money) + "\n\n"

            message += "預計投資部位:"
            for i, v in p.iteritems():
                message += "\n" + str(i) + " " + indu[str(i)][0] + " 投資 " + str(v) + "張"

            print(message)

            if not test:
                if netWorth * investpercentage < total_invest_money <= netWorth:
                    return_message.append(message)

            print("Done")

            pre_total_invest_money = total_invest_money
            invest_money += 50000
    elif pool == "boyd":
        # boyD pool
        # 計算可投入資金
        netWorth = 800000
        invest_money = netWorth
        total_invest_money = netWorth
        pre_total_invest_money = 0
        while total_invest_money <= 800000:
            p, total_invest_money = portfolio(stock_list.index, invest_money, data)
            if pre_total_invest_money == total_invest_money:
                invest_money += 50000
                continue
            # 要notify的info
            message = "\n" + "下注比例 : {}% : ".format(round(total_invest_money / netWorth * 100))

            message += "\n\n" + "networth : " + str(netWorth)

            message += "\n\n" + "actual investing money : " + str(total_invest_money) + "\n\n"

            message += "預計投資部位:"
            for i, v in p.iteritems():
                message += "\n" + str(i) + " " + indu[str(i)][0] + " 投資 " + str(v) + "張"

            print(message)

            if not test:
                if netWorth * investpercentage < total_invest_money <= netWorth:
                    return_message.append(message)

            print("Done")

            pre_total_invest_money = total_invest_money
            invest_money += 50000

    return return_message


def stock_list_linenotify(apiKeySecretDict: dict, linebot: list, investpercentage: float = 0.9, test=True):
    # 策略清單
    earlySellMonth = False
    earlySellSeason = False
    day = datetime.now(desired_timezone).date()
    # 如果是每月的1~9號就採用那個月10號的月報，其餘用上一次的月報
    # 季報也是從季報開出來前20天就採用最新的季報(3/1, 4/26, 7/26, 10/26)，其餘用上一次的季報
    # monthly_revenue early sell
    if 1 <= day.day < 10:
        diff = 10 - day.day
        day += relativedelta(days=diff)
    # early_sell month 可能10號星期五，所以往後推2天也要通知early_sell
    if 1 <= day.day <= 12:
        earlySellMonth = True
    # seasonal_report early sell
    if day.month == 3 and 1 <= day.day < 31:
        day = datetime(day.year, 3, 31).date()
        earlySellSeason = True
    if (day.month == 4 and 26 <= day.day <= 30) or (day.month == 5 and 1 <= day.day <= 15):
        day = datetime(day.year, 5, 15).date()
        earlySellSeason = True
    if (day.month == 7 and 26 <= day.day <= 31) or (day.month == 8 and 1 <= day.day <= 14):
        day = datetime(day.year, 8, 14).date()
        earlySellSeason = True
    if (day.month == 10 and 24 <= day.day <= 31) or (day.month == 11 and 1 <= day.day <= 14):
        day = datetime(day.year, 11, 14).date()
        earlySellSeason = True

    data = Data(day)

    print("Begin...")

    message = "\n{} price data num: {}\n{} monthly report data num: {}\n{} seasonal report data num: {}".format(
        *check_crawl_datanum(data))

    # early sale reevaluate

    if earlySellMonth:
        message += "\nEarly Sell Month\n"
        message += re_evaluate_holding(apiKeySecretDict, data, earlySellMonth, False)
    if earlySellSeason:
        message += "\nEarly Sell Season\n"
        message += re_evaluate_holding(apiKeySecretDict, data, False, earlySellSeason)

    netWorthQuantXPool = calculate_account_financial_num(apiKeySecretDict)["netWorth"]

    # 選股清單
    strategy = QuantxStrategy(data, params=dict())
    stock_list = strategy.run(capital=netWorthQuantXPool)
    # 要notify的info
    message += "\n" + datetime.now(desired_timezone).strftime('%Y-%m-%d') + "'s stock list:"
    eps = data.get('income_sheet', "基本每股盈餘合計", 1)
    for i in range(len(stock_list)):
        # 計算該產業季報eps開了幾%
        indu_k = eps.reindex(columns=indu_id[indu[str(stock_list.index[i])][1]]).dropna(axis=1)
        seasonal_report_announcement_percentage = int(
            len(indu_k.T) / len(indu_id[indu[str(stock_list.index[i])][1]]) * 100)

        message += "\n" + str(i + 1) + ". " + str(stock_list.index[i]) + " " + str(stock_list[i]) + " " + \
                   indu[str(stock_list.index[i])][0] + " " + indu[str(stock_list.index[i])][1] + " SRAP: " + \
                   str(seasonal_report_announcement_percentage) + "%"

    print(message)

    if not test:
        for token in linebot:
            line_notify_message(token, message)

    # boyD pool
    # 計算可投入資金
    # BoyD Pool linebot通知
    netWorth = 800000
    invest_money = netWorth
    total_invest_money = netWorth
    pre_total_invest_money = 0
    while total_invest_money <= 800000:
        p, total_invest_money = portfolio(stock_list.index, invest_money, data)
        if pre_total_invest_money == total_invest_money:
            invest_money += 50000
            continue
        # 要notify的info
        message = "\n" + "下注比例 : {}% : ".format(round(total_invest_money / netWorth * 100))

        message += "\n\n" + "networth : " + str(netWorth)

        message += "\n\n" + "actual investing money : " + str(total_invest_money) + "\n\n"

        message += "預計投資部位:"
        for i, v in p.iteritems():
            message += "\n" + str(i) + " " + indu[str(i)][0] + " 投資 " + str(v) + "張"

        print(message)

        if not test:
            if netWorth * investpercentage < total_invest_money <= netWorth:
                line_notify_message(linebot[0], message)

        print("Done")

        pre_total_invest_money = total_invest_money
        invest_money += 50000

    netWorth = netWorthQuantXPool
    invest_money = netWorthQuantXPool
    total_invest_money = netWorthQuantXPool
    pre_total_invest_money = 0

    # QuantX pool
    # 計算可投入資金
    # QuantX Pool linebot通知
    while total_invest_money <= netWorth:
        p, total_invest_money = portfolio(stock_list.index, invest_money, data)
        if pre_total_invest_money == total_invest_money:
            invest_money += 50000
            continue
        # 要notify的info
        message = "\n" + "下注比例 : {}% : ".format(round(total_invest_money / netWorth * 100))

        message += "\n\n" + "networth : " + str(netWorth)

        message += "\n\n" + "actual investing money : " + str(total_invest_money) + "\n\n"

        message += "預計投資部位:"
        for i, v in p.iteritems():
            message += "\n" + str(i) + " " + indu[str(i)][0] + " 投資 " + str(v) + "張"

        print(message)

        if not test:
            if netWorth * investpercentage < total_invest_money <= netWorth:
                line_notify_message(linebot[1], message)

        print("Done")

        pre_total_invest_money = total_invest_money
        invest_money += 50000


# If using this file to run, remember to remove "." while importing in this file and crawl.py,
# or simply use django to run this file. 
# if __name__ == '__main__':
def daily_task(test=False):
    # 爬蟲
    # crawl_data()

    # 跑策略清單並通知群組
    load_dotenv()

    # dev_token = os.getenv("dev_token")
    # trading_engine_token = os.getenv("trading_engine_token")
    # boyd_token = os.getenv("boyd_token")
    #
    # stock_list_token = [boyd_token, dev_token]
    #
    # net_worth_token = [dev_token, trading_engine_token]
    stock_list_token = []

    apiKeySecretDict = {"key": [], "secret": []}

    apiKeySecretDict["key"].append(os.getenv("api_key_Tony"))
    apiKeySecretDict["secret"].append(os.getenv("secret_key_Tony"))

    apiKeySecretDict["key"].append(os.getenv("api_key_Jason"))
    apiKeySecretDict["secret"].append(os.getenv("secret_key_Jason"))

    stock_list_linenotify(apiKeySecretDict, stock_list_token, test=test)

    # 計算績效
    # net_worth_linenotify(apiKeySecretDict, net_worth_token, test=test)

    return
