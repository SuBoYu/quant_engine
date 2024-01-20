import warnings
import pandas as pd
from .industry import industry_dict
import talib

warnings.filterwarnings("ignore", category=FutureWarning)

# ADL 均線指標
def get_adl(data, benchmark=1, ma_range=[5, 60]):
    df = data.get('price', '收盤價', 61)
    fluctuation = df / df.shift()
    fluctuation = fluctuation.dropna(how='all')
    fluctuation = fluctuation.T

    dataset = []
    for i in range(len(fluctuation.columns)):
        daily_stat = fluctuation.iloc[:, i].dropna()
        stats = {'ups': (daily_stat > benchmark).sum(), 'downs': (daily_stat < benchmark).sum()}
        dataset.append(stats)
    ad_line_df = pd.DataFrame(dataset, index=fluctuation.columns)
    ad_line_df['net'] = ad_line_df['ups'] - ad_line_df['downs']
    ad_line_df['ADL'] = ad_line_df['net'].cumsum()
    for i in ma_range:
        s = f'ADL_MA{i}'
        ad_line_df[s] = ad_line_df['ADL'].rolling(i).sum() / i
    ad_line_df = ad_line_df.dropna()
    ad_line_df = ad_line_df[[c for c in ad_line_df.columns if 'ADL' in c]]
    ad_line_df['ind'] = ad_line_df['ADL_MA5'] - ad_line_df['ADL_MA60']
    return ad_line_df.astype(float)

# 大盤融資維持率
def margin_position(data, short_par=5, long_par=30):
    上市融資交易金額 = data.get('margin_balance', '上市融資交易金額', 30)
    上櫃融資交易金額 = data.get('margin_balance', '上櫃融資交易金額', 30)
    融資今日餘額 = data.get('margin_transactions', '融資餘額', 30)
    close = data.get('price', '收盤價', 30)
    融資總餘額 = 上市融資交易金額 + 上櫃融資交易金額
    融資餘額市值 = (融資今日餘額 * close * 1000).sum(axis=1)[融資今日餘額.index]
    mt_rate = (融資餘額市值 / 融資總餘額)
    mt_rate = mt_rate.dropna()
    short_ma = mt_rate.rolling(short_par).mean()
    long_ma = mt_rate.rolling(long_par).mean()
    entry = short_ma >= long_ma
    return mt_rate, entry.astype(float)


# 大盤多空排列家數均線指標
def ls_order_position(data, short=5, mid=10, long=30):
    close = data.get('price', '收盤價', 30)
    short_ma = close.rolling(short).mean()
    mid_ma = close.rolling(mid).mean()
    long_ma = close.rolling(long).mean()
    long_order = (short_ma.iloc[-1, :] >= mid_ma.iloc[-1, :]) & (mid_ma.iloc[-1, :] >= long_ma.iloc[-1, :])
    long_order = long_order.sum()
    short_order = (short_ma.iloc[-1, :] < mid_ma.iloc[-1, :]) & (mid_ma.iloc[-1, :] < long_ma.iloc[-1, :])
    short_order = short_order.sum()
    entry = long_order - short_order
    return entry.astype(float)


# 台灣50 MACD週線
def get_macd_0050(data):
    df = data.get("price", "收盤價", 34)["0050"]
    macd, signal, hist = talib.MACD(df, fastperiod=12, slowperiod=26, signalperiod=9)
    return hist[-1]


# 大盤股價淨值比
def get_market_pb(data):
    capital = data.get('balance_sheet', '普通股股本', 1).iloc[-1, :]
    close = data.get('price', '收盤價', 1).iloc[-1, :]
    market_value = (capital * close).sum() / 10
    權益總計 = data.get('balance_sheet', '歸屬於母公司業主之權益合計', 1).iloc[-1, :]
    market_pb = 權益總計.sum()
    result = market_value / market_pb
    return result


def tw_macro_filter(data):
    # 大盤指數與月線交叉情況, 目前指數 > 均線20日
    benchmark_return_all = data.get('benchmark_return', '發行量加權股價報酬指數', 20)
    ma20_long_all = (benchmark_return_all > benchmark_return_all.rolling(20).mean())[-1] * 1

    # tw_total_pmi
    tw_total_pmi_all = data.get('tw_total_pmi', '製造業PMI', 1)
    tw_total_pmi_all = (tw_total_pmi_all > 50)[-1] * 1.5

    # tw_total_nmi
    tw_total_nmi_all = data.get('tw_total_nmi', '臺灣非製造業NMI(%)', 1)
    tw_total_nmi_all = (tw_total_nmi_all > 50)[-1] * 1

    # tw_total_pmi_future
    tw_total_pmi_future_all = data.get('tw_total_pmi', '未來六個月展望(%)', 1)
    tw_total_pmi_future_all = (tw_total_pmi_future_all > 50)[-1] * 1.5

    # tw_business_policy_ind
    tw_business_policy_ind_all = data.get('tw_business_indicator', '景氣對策信號(分)', 12)
    tw_business_policy_ind_all = (tw_business_policy_ind_all.rolling(3).mean() > tw_business_policy_ind_all.rolling(12).mean())[-1] * 2

    # ls_order_sig
    ls_order_sig_all = (ls_order_position(data) > 0).astype(float) * 1.5

    # macd_0050
    macd_0050_all = (get_macd_0050(data) > 0).astype(float)

    # market_pb
    market_pb_all = get_market_pb(data)
    market_pb_all = (market_pb_all >= 2) * -1 + (market_pb_all <= 1.4) * 1

    #adl
    adls_all = get_adl(data)
    adls_all = (adls_all['ind'] > 0).astype(float) * 1.5

    # mt_rate
    mt_rate_all, mt_rate_sig_all = margin_position(data)
    mt_rate_all = (mt_rate_all[-1] >= 1.8) * -1 + (mt_rate_all[-1] <= 1.4) * 2

    inds = pd.DataFrame({
        'benchmark_ma20': ma20_long_all,
        'tw_total_pmi': tw_total_pmi_all,
        'tw_total_nmi': tw_total_nmi_all,
        'tw_total_pmi_future': tw_total_pmi_future_all,
        'tw_business_policy_ind': tw_business_policy_ind_all,
        'ls_order_sig': ls_order_sig_all,
        'macd_0050': macd_0050_all,
        'market_pb': market_pb_all,
        'adls': adls_all,
        'mt_rate': mt_rate_all,
    })

    score = inds.astype(float).ffill().sum(axis=1)

    position = pd.DataFrame({
        'long': score >= 4,
        'short': score < 4
    })

    return position
