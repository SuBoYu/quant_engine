import pandas as pd
import plotly.express as px


# Description: get MAE, BMFE, GMFE
# Parameter:
## stocks_price: backtest result DataFrame
## hold_days: stocks holding days
def get_MAE_BMFE_GMFE(stocks_price, hold_days):
    stocks_price = stocks_price.iloc[:hold_days + 1]
    stocks_price = stocks_price.apply(pd.to_numeric, errors='coerce')
    buy_price = stocks_price.iloc[0]
    stocks_price_diff = stocks_price.iloc[1:] - buy_price

    # MAE
    stocks_MAE = ((stocks_price_diff / buy_price) * 100).min().apply(lambda x: abs(x) if x < 0 else 0)
    stocks_MAE_date = stocks_price_diff.idxmin()

    # GMFE
    stocks_GMFE = ((stocks_price_diff / buy_price) * 100).max().apply(lambda x: x if x >= 0 else 0)

    # BMFE
    stocks_BMFE = pd.Series()
    for stock_id in stocks_price.columns:
        # get all dates before MAE
        date_before_mae = [date for date in stocks_price_diff.index if date < stocks_MAE_date[stock_id]]

        # get stock's BMFE
        stock_BMFE = ((stocks_price_diff / buy_price) * 100).loc[date_before_mae][stock_id].max()
        stock_BMFE = stock_BMFE if stock_BMFE >= 0 else 0
        stocks_BMFE = pd.concat([stocks_BMFE, pd.Series({stock_id: stock_BMFE})])

    return stocks_MAE, stocks_BMFE, stocks_GMFE


# Description: plot edge ratio
# Parameter:
## all_mae_average: [pd.Series, pd.Series, ...]
## all_gmfe_average: [pd.Series, pd.Series, ...]
## file_saving_route: path to save file
## show_fig: to show the figure or not
def plot_edge_ratio_chart(all_mae_average, all_gmfe_average, file_saving_route, show_fig=False):
    longest_mae_len = len(max(all_mae_average, key=len))
    longest_gmfe_len = len(max(all_gmfe_average, key=len))

    # fill the length of the mae and gmfe to the max mae or gmfe length
    for i in range(len(all_mae_average)):
        mae = all_mae_average[i]
        if len(mae) != longest_mae_len:
            last_value = mae.iloc[-1]
            fill_mae_series = pd.Series([last_value] * (longest_mae_len - len(mae)))
            mae = pd.concat([mae, fill_mae_series], ignore_index=True)
            all_mae_average[i] = mae

    for i in range(len(all_gmfe_average)):
        gmfe = all_gmfe_average[i]
        if len(gmfe) != longest_gmfe_len:
            last_value = gmfe.iloc[-1]
            fill_gmfe_series = pd.Series([last_value] * (longest_gmfe_len - len(gmfe)))
            gmfe = pd.concat([gmfe, fill_gmfe_series], ignore_index=True)
            all_gmfe_average[i] = gmfe

    # calculate edge ratio
    mae_mean_df = pd.concat(all_mae_average, axis=1).transpose().mean().apply(lambda x: 0.1 if x == 0 else x)
    gmfe_mean_df = pd.concat(all_gmfe_average, axis=1).transpose().mean()
    edge_ratio = round(gmfe_mean_df / mae_mean_df, 2)

    fig = px.line(edge_ratio, x=edge_ratio.index, y=edge_ratio.values)
    fig.update_layout(
        title={
            'text': 'Edge Ratio',
            'x': 0.5,
            'y': 0.96,
            'xanchor': 'center',
            'yanchor': 'top'
        },
        title_font={
            'size': 25
        },
        xaxis_title='Time Scale',
        yaxis_title='Edge Ratio'
    )

    fig.write_image(file_saving_route + 'edge_ratio.png')
    if show_fig:
        fig.show()


def plot_return_chart(report, file_saving_route, show_fig=False):
    win_ratio = round((report['profit(%)'].dropna() > 0).sum() / len(report['profit(%)'].dropna()) * 100, 2)
    df = pd.DataFrame({
        'stock_id': report['stock_id'].dropna(),
        'buy_date': report['buy_date'].dropna().apply(lambda x: x.date()),
        'profit': [float(x) for x in report['profit(%)'].dropna()],
        'profit_or_loss': report['profit(%)'].dropna().apply(lambda x: 'profit' if x > 0 else 'loss')
    })
    colors = {'profit': '#f50c1c', 'loss': '#409c46'}

    fig = px.histogram(df, x="profit", color='profit_or_loss', color_discrete_map=colors)
    profit_mean = round(df['profit'].mean(), 2)
    fig.add_vline(x=profit_mean, line_width=2, line_dash="dash", line_color="green",
                  annotation_position="top right",
                  annotation_text=f'  avg:{profit_mean}%',
                  row=1, col=1)
    fig.update_layout(
        title={
            'text': f'Win Ratio:{win_ratio}%',
            'x': 0.5,
            'y': 0.96,
            'xanchor': 'center',
            'yanchor': 'top'
        },
        title_font={
            'size': 25
        },
        xaxis_title='Return(%)',
        yaxis_title='count',
        legend={
            'title': 'Profit & Loss'
        }
    )

    fig.write_image(file_saving_route + 'return_histogram.png')
    if show_fig:
        fig.show()


# Description: plot MAE and BMFE bubble chart
# Parameter:
## report: backtest result DataFrame
## file_saving_route: path to save file
## show_fig: to show the figure or not
def plot_mae_bmfe_bubble_chart(report, file_saving_route, show_fig=False):
    df = pd.DataFrame({
        'stock_id': report['stock_id'].dropna(),
        'buy_date': report['buy_date'].dropna().apply(lambda x: x.date()),
        'MAE': report['MAE'].dropna(),
        'BMFE': report['BMFE'].dropna(),
        'profit': [round(abs(float(x)), 2) for x in report['profit(%)'].dropna()],
        'profit_or_loss': report['profit(%)'].dropna().apply(lambda x: 'profit' if x > 0 else 'loss')
    })
    colors = {'profit': '#f50c1c', 'loss': '#409c46'}

    fig = px.scatter(df, x='MAE', y='BMFE', size='profit', hover_name='stock_id',
                     hover_data='buy_date', color='profit_or_loss', color_discrete_map=colors)
    fig.update_layout(
        title={
            'text': 'BMFE/MAE',
            'x': 0.5,
            'y': 0.96,
            'xanchor': 'center',
            'yanchor': 'top'
        },
        title_font={
            'size': 25
        },
        xaxis_title='MAE(%)',
        yaxis_title='BMFE(%)',
        legend={
            'title': 'Profit & Loss'
        }
    )

    fig.write_image(file_saving_route + 'mae_bmfe.png')
    if show_fig:
        fig.show()


# Description: plot MAE and GMFE bubble chart
# Parameter:
## report: backtest result DataFrame
## file_saving_route: path to save file
## show_fig: to show the figure or not
def plot_mae_gmfe_bubble_chart(report, file_saving_route, show_fig=False):
    df = pd.DataFrame({
        'stock_id': report['stock_id'].dropna(),
        'buy_date': report['buy_date'].dropna().apply(lambda x: x.date()),
        'MAE': report['MAE'].dropna(),
        'GMFE': report['GMFE'].dropna(),
        'profit': [round(abs(float(x)), 2) for x in report['profit(%)'].dropna()],
        'profit_or_loss': report['profit(%)'].dropna().apply(lambda x: 'profit' if x > 0 else 'loss')
    })
    colors = {'profit': '#f50c1c', 'loss': '#409c46'}

    fig = px.scatter(df, x='MAE', y='GMFE', size='profit', hover_name='stock_id',
                     hover_data='buy_date', color='profit_or_loss', color_discrete_map=colors)
    fig.update_layout(
        title={
            'text': 'GMFE/MAE',
            'x': 0.5,
            'y': 0.96,
            'xanchor': 'center',
            'yanchor': 'top'
        },
        title_font={
            'size': 25
        },
        xaxis_title='MAE(%)',
        yaxis_title='GMFE(%)',
        legend={
            'title': 'Profit & Loss'
        }
    )

    fig.write_image(file_saving_route + 'mae_gmfe.png')
    if show_fig:
        fig.show()


# Description: plot MAE and Return bubble chart
# Parameter:
## report: backtest result DataFrame
## file_saving_route: path to save file
## show_fig: to show the figure or not
def plot_mae_return_bubble_chart(report, file_saving_route, show_fig=False):
    df = pd.DataFrame({
        'stock_id': report['stock_id'].dropna(),
        'buy_date': report['buy_date'].dropna().apply(lambda x: x.date()),
        'MAE': report['MAE'].dropna(),
        'profit': [round(float(x), 2) for x in report['profit(%)'].dropna()],
        'profit_abs': [round(abs(float(x)), 2) for x in report['profit(%)'].dropna()],
        'profit_or_loss': report['profit(%)'].dropna().apply(lambda x: 'profit' if x > 0 else 'loss')
    })
    colors = {'profit': '#f50c1c', 'loss': '#409c46'}

    fig = px.scatter(df, x='profit', y='MAE', size='profit_abs', hover_name='stock_id',
                     hover_data='buy_date', color='profit_or_loss', color_discrete_map=colors)

    stats = {g[0]: g[1].describe().to_dict() for g in df.groupby('profit_or_loss')}

    # plot horizontal line to display the 3rd Quartile
    for pl, color in colors.items():
        y = stats[pl]['MAE']['75%']
        fig.add_hline(y=round(y, 2), line_width=2, line_dash="dash", annotation_text=f'Q3: {round(y, 2)}',
                      line_color=color)

    fig.update_layout(
        title={
            'text': 'MAE/Return',
            'x': 0.5,
            'y': 0.96,
            'xanchor': 'center',
            'yanchor': 'top'
        },
        title_font={
            'size': 25
        },
        xaxis_title='Return(%)',
        yaxis_title='MAE(%)',
        legend={
            'title': 'Profit & Loss'
        }
    )

    fig.write_image(file_saving_route + 'mae_return.png')
    if show_fig:
        fig.show()