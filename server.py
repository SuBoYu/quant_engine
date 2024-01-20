import dash
import dash_core_components as dcc
import dash_html_components as html
import pandas as pd
import datetime
from quantxlab.data import Data
import sys

# ----------------------
# get all strategies
# ----------------------
import os
snames = [py for py in os.listdir('quantxlab/strategies') if py[-3:] == '.py' and py != 'strategy.py']
strategies = {}
for s in snames:
    print('strategies.' + s[:-3])
    strategies[s[:-3]] = getattr(__import__('strategies.' + s[:-3]), s[:-3]).strategy


# ----------------------
# dataframe to html
# ----------------------
def generate_table(dataframe, max_rows=10):
    """
    將 dataframe 讀進來，轉換成 html 中的 table
    """
    table_value = []
    
    # 對於每個rows
    for i in range(min(len(dataframe), max_rows)):
        
        row = []
        
        # rows 中的每個元素
        for col in dataframe.columns:
            
            # 假如此 column 的最後兩個字為「漲跌」則根據正負顯示顏色
            if col[-2:] == '漲跌':
                color = 'red' if dataframe.iloc[i][col] >= 0 else 'green'
                row.append(html.Td(dataframe.iloc[i][col], style={'color': color}))
            else:
                row.append(html.Td(dataframe.iloc[i][col]))
                
        table_value.append(html.Tr(row))
   
    
    return html.Table(
        # Table Header
        [html.Tr([html.Th(col) for col in dataframe.columns])] +

        # Table Body
        table_value,
        className='table table-striped',
    )

# ------------------------
# simulation for strategy
# ------------------------
def simulation(strategy, data, date):
    
    """
    給定一個策略，還有日期，我們產生策略當天的股票清單
    並且產生股票從該日期到最近期的漲跌幅狀況（dataframe）
    並output出 dataframe 、 曲線圖 還有最近期晚上的日期
    """
    
    # record the original date for data
    org_date = data.date
    
    # get the stock list on the "date"
    data.date = date
    slist = strategy(data).index
    
    # select a subset of price
    data.date = datetime.datetime.now().date()
    ndays = (datetime.datetime.now().date() - date).days
    prices = data.get('收盤價', ndays+10)
    prices = prices[slist][date+datetime.timedelta(days=1):]
    
    df = pd.DataFrame()
    if not prices.empty:
        
        # 製作 dataframe
        buy_price = prices.iloc[0]
        current_price = prices.iloc[-1]
        yesterday_price = prices.iloc[-2]
        today_gain = (prices.iloc[-1] / prices.iloc[-2] - 1)*100
        total_gain = (prices.iloc[-1] / prices.iloc[0] - 1)*100

        df = pd.DataFrame({
            '買入股價': buy_price,
            '今日股價': current_price,
            '昨日股價': yesterday_price,
            '今日漲跌': today_gain,
            '至今漲跌': total_gain,
        })
        df = df[['買入股價', '昨日股價', '今日股價', '今日漲跌', '至今漲跌']]
        
        # equality
        eq = (prices/prices.bfill().iloc[0]).mean(axis=1)
        last_day = prices.index[-1]
    else:
        # 製作 dataframe
        prices = data.get('收盤價', ndays+10)
        
        df = pd.DataFrame({
            '今日收盤': prices[slist].iloc[-1]
        })
        
        # equality
        eq = pd.Series(1, index=pd.to_datetime([prices.index[-1]]))
        last_day = prices.index[-1]
        
    data.date = org_date
    return df, eq, str(last_day).split()[0] + '晚上的狀況'



# ----------------------
# Dash start 樣式表
# ----------------------

app = dash.Dash()

# ----------------------
# CSS style setting
# ----------------------

# bootstrap CSS
app.css.append_css({"external_url": "https://maxcdn.bootstrapcdn.com/bootstrap/3.3.7/css/bootstrap.min.css"})

# Dash CSS
app.css.append_css({"external_url": "https://codepen.io/chriddyp/pen/bWLwgP.css"})

# Loading screen CSS
app.css.append_css({"external_url": "https://codepen.io/chriddyp/pen/brPBPO.css"})

# ----------------------
# HTML layout
# ----------------------

app.layout = html.Div(children=[
    
    # 網頁標題
    html.H1(children='策略監控台'),
    html.Br(),
    
    # 選擇策略
    html.H4(children='策略名稱'),
    dcc.Dropdown(
        id='strategy-picker',
        options=[{'label': name, 'value': name} for name, func in strategies.items()],
    ),
    html.Br(),
    
    # 選擇日期
    html.H4(children='選股日期'),
    dcc.DatePickerSingle(
        id='date-picker',
        min_date_allowed=datetime.datetime(2014, 8, 1),
        max_date_allowed=datetime.datetime(2200, 1, 1),
        initial_visible_month=datetime.datetime.now(),
        #date=datetime.datetime.now(),
    ),
    html.Br(),
    html.Br(),
    
    # 顯示結果
    html.Div(id='table'),
], style={'width':'80%', 'margin':'10%'})

# ----------------------
# 使用者互動的邏輯
# ----------------------

@app.callback(
    # 此 function 產生的 output 會於 table 顯示
    dash.dependencies.Output(component_id='table', component_property='children'),
    
    # 此 function 的 input 是「策略選單」跟「日期選單」
    [dash.dependencies.Input(component_id='strategy-picker', component_property='value'),
     dash.dependencies.Input('date-picker', 'date')]
)
def update_output_div(input_value, date):
    
    """
    根據所選策略、所選時間，用simulation函式來產生 dataframe equality 和標題
    並且顯示於網頁中的 table 上
    """
    
    # 還沒選擇
    if date is None or input_value is None:
        return html.H4(children='請選擇上方的策略與日期')
    try:
        # 監測開始
        print('start simulation')
        date = datetime.datetime.strptime(date.split()[0], '%Y-%m-%d')
        data = Data()
        
        # 根據所選策略、所選時間，用simulation函式來產生 dataframe equality 和標題
        df, eq, s = simulation(strategies[input_value], data, date.date())
        print('end simulation')
        df.index.name = '股票代號'
        
        # 產生 html 
        return html.Div(children=[html.H3(s), html.Br(), dcc.Graph(
            id='example-graph-2',
            
            # 畫圖
            figure={
                'data': [
                    {'x': eq.index, 'y': eq, 'type': 'line', 'name': 'SF'},
                ],
                'layout': {
                }
            }
            # 用 generate_table 將 dataframe 轉成 HTML table 
        ),  generate_table(df.reset_index().round(2), len(df))
        ])
    except:
        errorlog = "Unexpected error: " + str(sys.exc_info())
        return html.H4(children='遇到了一些問題喔！' + errorlog)



if __name__ == '__main__':
    app.run_server(debug=True, processes=1)