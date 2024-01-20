import warnings
import pandas as pd
import numpy as np
import math
import datetime
import matplotlib.pyplot as plt
from ..shared_list import indu_id
from .TW_Macro_filter import tw_macro_filter
from .strategy import Strategy
from ..analysis_tools import *

warnings.filterwarnings("ignore", category=FutureWarning)

def pb_ratio(data):
    股東權益 = data.get('balance_sheet', '歸屬於母公司業主之權益合計', 1)
    股本 = data.get('balance_sheet', '普通股股本', 1)
    price = data.get('price', '收盤價', 1)

    股東權益.index = price.index
    股本.index = price.index

    # print(股本.index)

    # 算法跟剛剛非常類似，只是寫得比較快，不用往下深究啦XD，
    # 首先上面的公式，可以變成 股價淨值比 = 股價 / (股東權益/股本)/10
    # 然後因為 price 的頻率跟 股本 不一樣，所以必須

    pb = price / (股東權益 / 股本) / 10

    return pb
    # reindex  method="ffill向後填充"


def cash_dividend_to_price(data):
    現金股利 = -data.get('cash_flows', "發放現金股利", 20)

    現金股利.fillna(value=0, inplace=True)
    k = 現金股利.sum()
    k = k.to_frame()
    k = pd.DataFrame(k.values.reshape(1, -1), index=[現金股利.index[-1]], columns=現金股利.columns)
    股本 = data.get('balance_sheet', '普通股股本', 1)
    price = data.get('price', '收盤價', 1)
    每股現金股利 = (k / (股本 / 10))
    每股現金股利.index = ["殖利率"]
    price.index = ["殖利率"]
    return 每股現金股利 / price


def check_columns_month(x):
    return x.loc[["上月增減", "同期增減", "累積增減"]].isnull().all()


def check_columns_season(x):
    return x.loc[["現金流量", "營業利益", "本期淨利", "流動比率", "負債比率"]].isnull().all()


class MfsStrategy(Strategy):

    def __init__(self, data, params):
        """
        - parameters:
            - params: dict
                {"earlySellMonth": False,
                "earlySellSeason": False,
                "macro_filter": False,
                "reverse": False,
                "account_for": 1,
                "stop_loss": None,
                "stop_profit": None}
        """
        self.data = data
        self.params = params
        self.params["earlySellMonth"] = self.params['earlySellMonth'] if 'earlySellMonth' in self.params.keys() else False
        self.params["earlySellSeason"] = self.params['earlySellSeason']if 'earlySellSeason' in self.params.keys() else False
        self.params["macro_filter"] = self.params['macro_filter'] if 'macro_filter' in self.params.keys() else False
        self.params["reverse"] = self.params['reverse'] if 'reverse' in self.params.keys() else False
        self.params["account_for"] = self.params['account_for'] if 'account_for' in self.params.keys() else 6
        self.params["stop_loss"] = self.params['stop_loss'] if 'stop_loss' in self.params.keys() else None
        self.params["stop_profit"] = self.params['stop_profit'] if 'stop_profit' in self.params.keys() else None

    def get_type(self) -> str:
        return "QuantX_strategy"

    def get_params(self) -> dict:
        return self.params

    def backtest(self, path_record, start_date, end_date, hold_days, capital=10000000, weight='average', benchmark_id="0050"):
        """
        - parameters:
            - path_record: str
                path to save the result
            - start_date: datetime.date
                start date of backtest
            - end_date: datetime.date
                end date of backtest
            - hold_days: int or list
                hold days of backtest
            - capital: int
                initial capital
            - weight: str
                weight of stocks in portfolio, 'average' or 'price'
            - benchmark_id: str
                benchmark of backtest
        """
        # weight configuration check
        if weight != 'average' and weight != 'price':
            print('Backtest stop, weight should be "average" or "price", find', weight, 'instead')

        # get price data in order backtest
        self.data.date = end_date
        close_price = self.data.get('price', '收盤價', (end_date - start_date).days)
        highest_price = self.data.get('price', '最高價', (end_date - start_date).days)
        lowest_price = self.data.get('price', '最低價', (end_date - start_date).days)

        # start from 1 TWD at start_date,
        end = 1
        date = start_date

        # record some history
        principal = capital
        win_rate_count = 0
        monthly_return = []
        equality = pd.Series()
        nstock = {}
        transactions = pd.DataFrame()
        maxreturn = -10000
        minreturn = 10000
        # 紀錄換手報酬
        changerecord_list = []

        # record average MAE and GMFE for each edge_ratio window and each stocks
        all_mae_average = []
        all_gmfe_average = []

        def date_iter_periodicity(start_date, end_date, hold_days):
            date = start_date
            while date < end_date:
                yield (date), (date + datetime.timedelta(hold_days))
                date += datetime.timedelta(hold_days)

        def date_iter_specify_dates(start_date, end_date, hold_days):
            dlist = [start_date] + hold_days + [end_date]
            if dlist[0] == dlist[1]:
                dlist = dlist[1:]
            if dlist[0] > dlist[1]:
                dlist = [dlist[0]] + dlist[2:]
            if dlist[-1] == dlist[-2]:
                dlist = dlist[:-1]
            if dlist[-1] < dlist[-2]:
                dlist = dlist[:-2] + [dlist[-1]]
            for sdate, edate in zip(dlist, dlist[1:]):
                yield (sdate), (edate)

        if isinstance(hold_days, int):
            dates = date_iter_periodicity(start_date, end_date, hold_days)
        elif isinstance(hold_days, list):
            dates = date_iter_specify_dates(start_date, end_date, hold_days)
        else:
            print('the type of hold_dates should be list or int.')
            return None

        for sdate, edate in dates:

            # select stocks at date
            self.data.date = sdate - datetime.timedelta(days=1)
            if self.params["reverse"]:
                stocks = self.run(capital=principal)
            else:
                stocks = self.run(capital=principal)
            mfs_score = stocks
            print("principal: ", principal)
            stocks, total_invest_money = self.portfolio(stocks.index, principal)
            mfs_score = mfs_score[stocks.index]
            principal -= total_invest_money

            # hold the stocks for hold_days day
            s = close_price[stocks.index & close_price.columns][sdate:edate]
            h = highest_price[stocks.index & highest_price.columns][sdate:edate]
            l = lowest_price[stocks.index & lowest_price.columns][sdate:edate]

            if s.empty:
                s = pd.Series(1, index=pd.date_range(sdate, edate))
            else:
                if self.params["stop_loss"] != None:
                    below_stop = ((s / s.bfill().iloc[0]) - 1) * 100 < -np.abs(self.params["stop_loss"])
                    below_stop = (below_stop.cumsum() > 0).shift(1).fillna(False)
                    s[below_stop] = np.nan
                    h[below_stop] = np.nan
                    l[below_stop] = np.nan

                if self.params["stop_profit"] != None:
                    above_stop = ((s / s.bfill().iloc[0]) - 1) * 100 > np.abs(self.params["stop_profit"])
                    above_stop = (above_stop.cumsum() > 0).shift(2).fillna(False)
                    s[above_stop] = np.nan

                s.dropna(axis=1, how='all', inplace=True)
                h.dropna(axis=1, how='all', inplace=True)
                l.dropna(axis=1, how='all', inplace=True)

                # record transactions
                bprice = s.bfill().iloc[0]
                sprice = s.apply(lambda s: s.dropna().iloc[-1])
                if self.params["stop_loss"] != None:
                    stop_loss_price = bprice * (1 - self.params["stop_loss"] / 100)
                    hprice = h.apply(lambda h: h.dropna().iloc[-1])
                    lprice = l.apply(lambda l: l.dropna().iloc[-1])
                    # 出場價
                    # 止損價比最高價高，代表跳空，就取最高價合最低價平均
                    # 止損價比最低價低，代表沒有觸發止損
                    # 止損價在最高最低之間採用止損價
                    stop_loss_price[stop_loss_price > hprice] = (hprice[stop_loss_price > hprice] + lprice[
                        stop_loss_price > hprice]) / 2
                    stop_loss_price[stop_loss_price < lprice] = sprice[stop_loss_price < lprice]
                    sprice = stop_loss_price
                    # s裡的出場點位還沒修正，要把全部的出場點位都修正成上面的有止損時的sprice，目前還是都拿原本的sprice 收盤價
                    for stock_id in sprice.index:
                        last_valid_index = s[stock_id].last_valid_index()
                        s.loc[last_valid_index, stock_id] = sprice[stock_id]

                temp_s = s.copy()

                s.ffill(inplace=True)

                mae_average = dict()
                gmfe_average = dict()
                # get average MAE and BMFE for each edge_ratio and each stock
                for edge_ratio_window in range(1, (edate - sdate).days + 1):
                    mae, bmfe, gmfe = get_MAE_BMFE_GMFE(s, edge_ratio_window)

                    # get MAE, BMFE and GMFE of each stock in hold_days
                    if edge_ratio_window == (edate - sdate).days:
                        stocks_MAE = mae
                        stocks_BMFE = bmfe
                        stocks_GMFE = gmfe

                    mae = round(mae.mean(), 2)
                    gmfe = round(gmfe.mean(), 2)

                    mae_average[edge_ratio_window] = mae
                    gmfe_average[edge_ratio_window] = gmfe

                all_mae_average.append(pd.Series(mae_average))
                all_gmfe_average.append(pd.Series(gmfe_average))

                # 每支股票的報酬率  type = df
                transactions = transactions.append(pd.DataFrame({
                    'mfs_score': mfs_score,
                    'buy_price': bprice,
                    'sell_price': sprice,
                    'lowest_price': s.min(),
                    'highest_price': s.max(),
                    'buy_date': pd.Series(s.index[0], index=s.columns),
                    'sell_date': temp_s.apply(lambda temp_s: temp_s.dropna().index[-1]),
                    'profit(%)': (sprice / bprice - 1) * 100,
                    'MAE': stocks_MAE,
                    'BMFE': stocks_BMFE,
                    'GMFE': stocks_GMFE
                }))

                # calculate equality
                # normalize and average the price of each stocks
                if weight == 'average':
                    s = s / s.bfill().iloc[0]
                s = s.mean(axis=1)
                s = s / s.bfill()[0]

            # print some log
            change_record = str(sdate) + '-' + str(edate) + " 報酬率:" + str(
                round(s.iloc[-1] / s.iloc[0] * 100 - 100, 2)) + '% '
            # For 策略勝率計算
            if (round(s.iloc[-1] / s.iloc[0] * 100 - 100, 2) > 0):
                win_rate_count += 1
            # For 夏普率計算
            monthly_return.append(round(s.iloc[-1] / s.iloc[0] * 100 - 100, 2))
            # 起始資金變動
            R = round(s.iloc[-1] / s.iloc[0] * 100 - 100, 2)
            # 考慮實際投資跟沒投資的本金
            total_invest_money = total_invest_money * (1 + R / 100)
            principal += total_invest_money

            change_record += "本金:" + str(principal)

            # save in change_recordlist
            changerecord_list.append(change_record)

            print(change_record)

            maxreturn = max(maxreturn, s.iloc[-1] / s.iloc[0] * 100 - 100)
            minreturn = min(minreturn, s.iloc[-1] / s.iloc[0] * 100 - 100)

            # plot backtest result
            ((s * end - 1) * 100).plot()
            # 隨時間的淨值紀錄  type = series
            # 有複利效果
            # 為了避免這期end_date跟下期start_date的淨值重複計算
            equality = equality.append(s[:-1] * end)
            end = (s / s[0] * end).iloc[-1]

            if math.isnan(end):
                end = 1

            # add nstock history
            nstock[sdate] = len(stocks)

        maxreturn = "每次換手最大報酬 : " + str(round(maxreturn, 2)) + "%"
        minreturn = "每次換手最小報酬 : " + str(round(minreturn, 2)) + "%"

        extrareturn_list = [maxreturn, minreturn]

        print(maxreturn)
        print(minreturn)

        benchmark = close_price[benchmark_id][start_date:end_date].iloc[1:]

        # MDD
        mdd = pd.DataFrame((equality / equality.cummax() - 1) * 100, columns=["MDD"]).reset_index(drop=True)
        # 策略勝率
        win_rate = round(win_rate_count / len(changerecord_list) * 100, 2)
        changerecord_list.append("win_rate:" + str(win_rate) + "%")
        # 夏普比率 （Rx標的平均報酬率－Rf無風險利率）/ σx標準差。
        risk_free_rate = 0.005
        avg_montly_return = sum(monthly_return) / len(monthly_return)
        std_monthly_return = np.std(monthly_return)
        sharpe_ratio = round(avg_montly_return / std_monthly_return * (12 ** 0.5), 2)
        changerecord_list.append("sharpe_ratio:" + str(sharpe_ratio))
        # 年化報酬
        start_year = start_date.year
        end_year = end_date.year
        total_years = end_year - start_year + round((((end_date - datetime.date(end_date.year, 1, 1)).days + 1) / 365),
                                                    2)

        yearly_roi_fund = round(pow(principal / capital, 1 / total_years) * 100 - 100, 2)
        changerecord_list.append("yearly_roi_fund:" + str(yearly_roi_fund) + "%")

        yearly_roi_algo = round(pow(equality[-1], 1 / total_years) * 100 - 100, 2)
        changerecord_list.append("yearly_roi_algo:" + str(yearly_roi_algo) + "%")
        # 儲存第一張圖片 報酬
        # bechmark (thanks to Markk1227)
        ((benchmark / benchmark[0] - 1) * 100).plot(color=(0.8, 0.8, 0.8))
        plt.ylabel('Return On Investment (%)')
        plt.grid(linestyle='-.')

        if isinstance(hold_days, int):
            path_record += str(start_date) + "~" + str(end_date) + "_" + str(hold_days) + "_" + str(
                capital) + str(self.params["stop_loss"])
        else:
            path_record += str(start_date) + "~" + str(end_date) + "_specific_date_" + str(capital) + "_" + str(
                self.params["stop_loss"]) + "_"
        plt.savefig(path_record + 'return.png')
        plt.close()
        # plt.show()

        # 儲存第二張圖片 回撤
        ((benchmark / benchmark.cummax() - 1) * 100).plot(legend=True, color=(0.8, 0.8, 0.8))
        ((equality / equality.cummax() - 1) * 100).plot(legend=True)
        plt.ylabel('Drawdown (%)')
        plt.grid(linestyle='-.')
        plt.savefig(path_record + 'drawdown.png')
        plt.close()
        # plt.show()

        # 儲存第三個CSV 將所有交易紀錄存成csv
        changerecord_df = pd.DataFrame(changerecord_list, columns=["Change Return"])
        extrareturn_df = pd.DataFrame(extrareturn_list, columns=["Extra Change"])
        equality = pd.DataFrame(equality, columns=["Net Worth"])

        equality = equality.rename_axis("Date").reset_index()

        transactions = transactions.rename_axis("stock_id").reset_index()
        print(equality)
        print(transactions)

        final = []
        # 橫向合併
        final = pd.concat([changerecord_df, extrareturn_df, equality, mdd, transactions], ignore_index=True, axis=1)
        final.columns = ["Change Record", "Extra Change", "Date", "Net Worth", "mdd", "stock_id", "mfs_score",
                         "buy_price",
                         "sell_price", "lowest_price", "highest_price", "buy_date", "sell_date", "profit(%)", "MAE",
                         "BMFE",
                         "GMFE"]

        # plot bubble chart
        plot_return_chart(final, path_record, show_fig=True)
        plot_mae_bmfe_bubble_chart(final, path_record, show_fig=True)
        plot_mae_gmfe_bubble_chart(final, path_record, show_fig=True)
        plot_mae_return_bubble_chart(final, path_record, show_fig=True)
        plot_edge_ratio_chart(all_mae_average, all_gmfe_average, path_record, show_fig=True)

        final.to_csv(path_record + '.csv', encoding="utf_8_sig")

    def run(self, capital=1000000):
        """
        - parameters:
            - capital: int
                initial capital
        """

        price = self.data.get('price', '收盤價', 1)

        # ------------------營收成長性------------------
        # 月營收月增率
        mr = self.data.get('monthly_revenue', "上月比較增減(%)", 1)
        # 月營收年增率
        yr = self.data.get('monthly_revenue', "去年同月增減(%)", 1)
        # 累計營收年增率
        cyr = self.data.get('monthly_revenue', "前期比較增減(%)", 1)

        for i in range(mr.shape[1]):
            if (mr.iloc[0][i]) > 0:
                mr.iloc[0][i] = 5
            else:
                mr.iloc[0][i] = 0

        for i in range(yr.shape[1]):
            if (yr.iloc[0][i]) > 0:
                yr.iloc[0][i] = 5
            else:
                yr.iloc[0][i] = 0

        for i in range(cyr.shape[1]):
            if (cyr.iloc[0][i]) > 0:
                cyr.iloc[0][i] = 10
            else:
                cyr.iloc[0][i] = 0

        mr.index = ["上月增減"]
        yr.index = ["同期增減"]
        cyr.index = ["累積增減"]

        # ------------------獲利成長性------------------

        # 毛利率
        gp = self.data.get('income_sheet', "營業毛利（毛損）", 5) / self.data.get('income_sheet', "營業收入合計", 5)

        # 毛利率季增率
        gps = gp.iloc[-1] / gp.iloc[-2] - 1
        # 毛利率年增率
        gpy = gp.iloc[-1] / gp.iloc[-5] - 1

        # 營業利益
        bi = self.data.get('income_sheet', "營業利益（損失）", 5)

        # 營業利益季增
        bis = bi.iloc[-1] / bi.iloc[-2] - 1

        # 營業利益年增
        biy = bi.iloc[-1] / bi.iloc[-5] - 1

        gps = gps.to_frame(name="毛利季增")
        gpy = gpy.to_frame(name="毛利年增")
        bis = bis.to_frame(name="利益季增")
        biy = biy.to_frame(name="利益年增")
        gps = gps.T
        gpy = gpy.T
        bis = bis.T
        biy = biy.T

        for i in range(gps.shape[1]):
            if (gps.iloc[0][i]) > 0:
                gps.iloc[0][i] = 5
            else:
                gps.iloc[0][i] = 0

        for i in range(gpy.shape[1]):
            if (gpy.iloc[0][i]) > 0:
                gpy.iloc[0][i] = 5
            else:
                gpy.iloc[0][i] = 0

        for i in range(bis.shape[1]):
            if (bis.iloc[0][i]) > 0:
                bis.iloc[0][i] = 5
            else:
                bis.iloc[0][i] = 0

        for i in range(biy.shape[1]):
            if (biy.iloc[0][i]) > 0:
                biy.iloc[0][i] = 5
            else:
                biy.iloc[0][i] = 0

        # ------------------穩定性------------------

        # 營業活動現金流量
        ca = self.data.get('cash_flows', "營業活動之淨現金流入（流出）", 1)
        # 營業利益營業活動之淨現金流入（流出）
        op = self.data.get('income_sheet', "營業利益（損失）", 1)
        # 本期淨利
        np = self.data.get('income_sheet', "本期淨利（淨損）", 1)

        # 現金股利
        # cd = data.get('cash_flows', "發放現金股利",20)

        for i in range(ca.shape[1]):
            if (ca.iloc[0][i]) > 0:
                ca.iloc[0][i] = 5
            else:
                ca.iloc[0][i] = 0

        for i in range(op.shape[1]):
            if (op.iloc[0][i]) > 0:
                op.iloc[0][i] = 5
            else:
                op.iloc[0][i] = 0

        for i in range(np.shape[1]):
            if (np.iloc[0][i]) > 0:
                np.iloc[0][i] = 5
            else:
                np.iloc[0][i] = 0

        # 5年現金股利不使用
        # cd = cd.fillna(value=0)
        #
        # for i in range(ca.shape[1]):
        #     if cd.iloc[0][i] or cd.iloc[1][i] or cd.iloc[2][i] or cd.iloc[3][i]:
        #         cd.loc[21] = 5
        #     else:
        #         cd.loc[21] = 0
        #
        # cd = cd.drop(cd.index[0:20])

        ca.index = ["現金流量"]
        op.index = ["營業利益"]
        np.index = ["本期淨利"]

        final = pd.concat([mr, yr, cyr, gps, gpy, bis, biy, ca, op, np])

        # ------------------安全性------------------
        # 流動比率
        流動資產 = self.data.get('balance_sheet', "流動資產合計", 1)
        流動負債 = self.data.get('balance_sheet', "流動負債合計", 1)

        流動比率 = 流動資產 / 流動負債

        流動比率[流動比率 > 1] = 5
        流動比率[流動比率 <= 1] = 0
        流動比率.index = ["流動比率"]

        # 負債比率
        資產總計 = self.data.get('balance_sheet', "資產總計", 1)
        負債總計 = self.data.get('balance_sheet', "負債總計", 1)

        負債比率 = 負債總計 / 資產總計
        負債比率[負債比率 < 0.5] = 0.4
        負債比率[負債比率 >= 0.5] = 0
        負債比率[負債比率 == 0.4] = 5
        負債比率.index = ["負債比率"]

        # ------------------價值性------------------
        # 本益比 針對各產業去調整本益比的篩選方式
        price.index = ["PE"]
        eps = self.data.get('income_sheet', "基本每股盈餘合計", 1)
        eps.index = ["PE"]
        本益比 = price / eps
        本益比[本益比 < 0] = float("inf")

        本益比 = 本益比.sort_values("PE", axis=1)

        c = 0
        for indu_cat in indu_id.keys():
            indu_list = indu_id[indu_cat]
            # 把過去沒有的indu_list的公司代號丟掉，reindex會把過去沒有的公司塞nan
            indu_k = 本益比.reindex(columns=indu_list).dropna(axis=1).sort_values("PE", axis=1).loc["PE"]

            part = int(len(indu_k) / 16)

            for i in range(0, 16):
                if i == 15:
                    indu_k.iloc[i * part:] = 15 - i
                else:
                    indu_k.iloc[i * part:(i + 1) * part] = 15 - i

            if c == 0:
                k = indu_k
            else:
                k = k.append(indu_k)
            c += 1
        k = pd.DataFrame(k, columns=['PE'])
        本益比 = k.T

        # 股價淨值比
        pb = pb_ratio(self.data)
        pb.index = ["PB"]
        k = pb.loc["PB"]
        # 用中位數去填充Nan
        k.fillna(value=k.median(), inplace=True)

        pb = pb.sort_values("PB", axis=1)
        k = pb.loc["PB"]
        part = int(len(k) / 6)
        for i in range(0, 6):
            if i == 5:
                k.iloc[i * part:] = 5 - i
            else:
                k.iloc[i * part:(i + 1) * part] = 5 - i

        # 現金股利殖利率

        cdtp = cash_dividend_to_price(self.data)
        cdtp = cdtp.sort_values("殖利率", axis=1)
        k = cdtp.loc["殖利率"]
        part = int(len(k) / 11)
        for i in range(0, 11):
            if i == 10:
                k.iloc[i * part:] = i
            else:
                k.iloc[i * part:(i + 1) * part] = i

        # ------------------全部數據合併------------------
        res = pd.concat([final, 流動比率, 負債比率, 本益比, pb, cdtp], axis=0)

        # ------------------將月報分數皆為Nan的排掉-----------------------
        if self.params["earlySellMonth"]:
            cols_to_drop = res.apply(check_columns_month, axis=0)
            res = res.loc[:, ~cols_to_drop]
        # ------------------將季報分數皆為Nan的排掉-----------------------
        if self.params["earlySellSeason"]:
            cols_to_drop = res.apply(check_columns_season, axis=0)
            res = res.loc[:, ~cols_to_drop]

        # ------------------stockrank 選股排序------------------
        stockrank = res.sum().sort_values(axis=0, ascending=False)

        # ------------------earlySell Module------------------
        if self.params["earlySellMonth"]:
            print("Early Sell Month:")
            print("Only", len(stockrank), "stocks left.")
            return stockrank
        if self.params["earlySellSeason"]:
            print("Early Sell Season:")
            print("Only", len(stockrank), "stocks left.")
            return stockrank

        stockrank = stockrank[stockrank.index[:10]]

        return stockrank
