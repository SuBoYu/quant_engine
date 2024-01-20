import numpy as np

def portfolio(stock_list, money, data, lowest_fee=20, discount=0.6, add_cost=10):
    close_price = data.get('price', '收盤價', 1)
    stock_list = close_price.iloc[-1][stock_list].transpose()
    print('estimate price according to', close_price.index[-1])

    print('initial number of stock', len(stock_list))
    while (money / len(stock_list)) < (lowest_fee - add_cost) * 1000 / 1.425 / discount:
        stock_list = stock_list[stock_list != stock_list.max()]
    print('after considering fee', len(stock_list))

    while True:
        invest_amount = (money / len(stock_list))
        ret = np.floor(invest_amount / stock_list / 1000)

        if (ret == 0).any():
            stock_list = stock_list[stock_list != stock_list.max()]
        else:
            break

    print('after considering 1000 share', len(stock_list))

    return ret, (ret * stock_list * 1000).sum()