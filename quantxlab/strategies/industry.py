import requests
import pandas as pd
import copy
import random
import time
from requests.exceptions import ReadTimeout
from requests.exceptions import ConnectionError
from fake_useragent import UserAgent

def generate_random_header():
    ua = UserAgent()
    user_agent = ua.random
    headers = {'Accept': '*/*', 'Connection': 'keep-alive',
               'User-Agent': user_agent}
    return headers


def industry_dict():
    #上市類股
    url = "https://isin.twse.com.tw/isin/class_main.jsp?owncode=&stockname=&isincode=&market=1&issuetype=1&industry_code=&Page=1&chklike=Y"

    for i in range(10):
        try:
            headers = generate_random_header()
            response = requests.get(url, headers=headers)
            break
        except (ConnectionError, ReadTimeout) as error:
            print(error)
            print('失敗，10秒後重試')
            time.sleep(10)

    listed = pd.read_html(response.text)[0]
    listed.columns = listed.iloc[0,:]
    listed = listed[["有價證券代號","有價證券名稱","市場別","產業別","公開發行/上市(櫃)/發行日"]]
    listed = listed.iloc[1:]

    id = listed["有價證券代號"]

    name = listed["有價證券名稱"] 

    indu = listed["產業別"]

    id = id.tolist()
    name = name.tolist()
    indu =  indu.tolist()

    #宣告一字典，key為股票代號，value為名稱與產業別
    indudic = {}

    #宣告一字典，key為產業類別，value股票代號
    indu_id = {}

    for i in range(listed.shape[0]):
        temp = []     #股票名稱、產業類別
        temp.append(name[i])
        temp.append(indu[i])
        indudic[id[i]] = temp
        if indu[i] not in indu_id.keys():
            indu_id[indu[i]] = [id[i]]
        else:
            indu_id[indu[i]].append(id[i])

    #上櫃股票
    url = "https://isin.twse.com.tw/isin/class_main.jsp?owncode=&stockname=&isincode=&market=2&issuetype=4&industry_code=&Page=1&chklike=Y"

    response = requests.get(url)
    listed = pd.read_html(response.text)[0]
    listed.columns = listed.iloc[0,:]
    listed = listed[["有價證券代號","有價證券名稱","市場別","產業別","公開發行/上市(櫃)/發行日"]]
    listed = listed.iloc[1:]

    id = listed["有價證券代號"]

    name = listed["有價證券名稱"] 

    indu = listed["產業別"]

    id = id.tolist()
    name = name.tolist()
    indu =  indu.tolist()

    for i in range(listed.shape[0]):
        temp = []     #股票名稱、產業類別
        temp.append(name[i])
        temp.append(indu[i])
        indudic[id[i]] = temp
        if indu[i] not in indu_id.keys():
            indu_id[indu[i]] = [id[i]]
        else:
            indu_id[indu[i]].append(id[i])
    delkey = []
    for item in indu_id.keys():
        if len(indu_id[item]) < 16:
            temp = indu_id[item]
            for i in temp:
                indudic[i][1] = "其他業"
            indu_id["其他業"] += temp
            delkey.append(item)
    for i in delkey:
        del indu_id[i]


    return indudic, indu_id

#計算公司種類與數量
if __name__ == "__main__":
    
    print("Begin...")
    
    #dict key = sotck_id ； value = [公司名稱, 產業類別]
    indu = industry_dict()
    
    indus = []
    #放進所有產業類別
    for i in indu.values():
        indus.append(i[1])
    
    con = []
    
    #篩出總數小於6的產業list
    for item in set(indus):
        if indus.count(item) < 6:
            con.append(item)
    
    #總數小於6的產業set(不重複)
    con = set(con)
    
    #產業總數小於6的股票代號
    conid = []
    for i in indu.keys():
        if indu[i][1] in con:
            conid.append(i)

    #刪除上述股票代號
    for stock_id in conid:
        del indu[stock_id]
    
    #重新加進去並將key設為 "其他"
    for i in range(len(conid)):
        temp = []              
        temp.append(conid[i])   #股票名稱
        temp.append("其他業")     #產業類別
        indu[conid[i]] = temp

    indus = []
    #放進所有產業類別
    for i in indu.values():
        indus.append(i[1])

    for item in set(indus):
        print(item, indus.count(item))
    