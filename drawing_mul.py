import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

y1 = 0
y2 = 0

for y1 in range(2021,2022):
    for y2 in range(2021,2022):
        if y2 >= y1:
            print("===========This is", y1,"to",y2,"===========")
            path5 = "C:/Users/user/桌面/Quant-investment/side_project/result/5/" + str(y1) + "~" + str(y2) +"/" + str(y1) +"-01-01~"+ str(y2) +"-08-16_5.csv"
            path10 = "C:/Users/user/桌面/Quant-investment/side_project/result/5/" + str(y1) + "~" + str(y2) +"/" + str(y1) +"-01-01~"+ str(y2) +"-08-16_10.csv"
            path20 = "C:/Users/user/桌面/Quant-investment/side_project/result/5/" + str(y1) + "~" + str(y2) +"/" + str(y1) +"-01-01~"+ str(y2) +"-08-16_20.csv"
            path30 = "C:/Users/user/桌面/Quant-investment/side_project/result/5/" + str(y1) + "~" + str(y2) +"/" + str(y1) +"-01-01~"+ str(y2) +"-08-16_30.csv"
            path40 = "C:/Users/user/桌面/Quant-investment/side_project/result/5/" + str(y1) + "~" + str(y2) +"/" + str(y1) +"-01-01~"+ str(y2) +"-08-16_40.csv"
            path50 = "C:/Users/user/桌面/Quant-investment/side_project/result/5/" + str(y1) + "~" + str(y2) +"/" + str(y1) +"-01-01~"+ str(y2) +"-08-16_50.csv"
            path60 = "C:/Users/user/桌面/Quant-investment/side_project/result/5/" + str(y1) + "~" + str(y2) +"/" + str(y1) +"-01-01~"+ str(y2) +"-08-16_60.csv"
            df5 = pd.read_csv(path5)
            df10 = pd.read_csv(path10)
            df20 = pd.read_csv(path20)
            df30 = pd.read_csv(path30)
            df40 = pd.read_csv(path40)
            df50 = pd.read_csv(path50)
            df60 = pd.read_csv(path60)

            #使用月份當做X軸資料
            date = df5["Date"]
            date = date.to_list()
            for i in range(len(date)):
                if type(date[i]) == float:
                    stop = i
                    break
            for i in range(len(date)-stop):
                date.pop()

            value5 = df5["Net Worth"]
            value5 = value5.to_list()
            for i in range(len(value5)):
                if np.isnan(value5[i]):
                    stop = i
                    break

            for i in range(len(value5)-stop):
                value5.pop()

            value10 = df10["Net Worth"]
            value10 = value10.to_list()
            for i in range(len(value10)):
                if np.isnan(value10[i]):
                    stop = i
                    break
            for i in range(len(value10)-stop):
                value10.pop()

            value20 = df20["Net Worth"]
            value20 = value20.to_list()
            for i in range(len(value20)):
                if np.isnan(value20[i]):
                    stop = i
                    break
            for i in range(len(value20)-stop):
                value20.pop()

            value30 = df30["Net Worth"]
            value30 = value30.to_list()
            for i in range(len(value30)):
                if np.isnan(value30[i]):
                    stop = i
                    break
            for i in range(len(value30)-stop):
                value30.pop()

            value40 = df40["Net Worth"]
            value40 = value40.to_list()
            for i in range(len(value40)):
                if np.isnan(value40[i]):
                    stop = i
                    break
            for i in range(len(value40)-stop):
                value40.pop()

            value50 = df50["Net Worth"]
            value50 = value50.to_list()
            for i in range(len(value50)):
                if np.isnan(value50[i]):
                    stop = i
                    break
            for i in range(len(value50)-stop):
                value50.pop()

            value60 = df60["Net Worth"]
            value60 = value60.to_list()
            for i in range(len(value60)):
                if np.isnan(value60[i]):
                    stop = i
                    break
            for i in range(len(value60)-stop):
                value60.pop()

            sma = min(len(date),len(value5),len(value10),len(value20),len(value30),len(value40),len(value50),len(value60))

            for i in range(len(date)-sma):
                date.pop(0)
            for i in range(len(value5)-sma):
                value5.pop(0)
            for i in range(len(value10)-sma):
                value10.pop(0)
            for i in range(len(value20)-sma):
                value20.pop(0)
            for i in range(len(value30)-sma):
                value30.pop(0)
            for i in range(len(value40)-sma):
                value40.pop(0)
            for i in range(len(value50)-sma):
                value50.pop(0)
            for i in range(len(value60)-sma):
                value60.pop(0)

            # 設定圖片大小為長15、寬10
            plt.figure(figsize=(12,8),dpi=100,linewidth = 2)

            # 把資料放進來並指定對應的X軸、Y軸的資料，用方形做標記(s-)，並指定線條顏色為紅色，使用label標記線條含意
            plt.plot(date,value5,color = 'red', label="5")
            plt.plot(date,value10,color = 'orange', label="10")
            plt.plot(date,value20,color = 'yellow', label="20")
            plt.plot(date,value30,color = 'green', label="30")
            plt.plot(date,value40,color = 'blue', label="40")
            plt.plot(date,value50,color = 'purple', label="50")
            plt.plot(date,value60,color = 'black', label="60")

            # 設定圖片標題，以及指定字型設定，x代表與圖案最左側的距離，y代表與圖片的距離
            plt.title("Net Value", x=0.5, y=1.03)

            # 设置刻度字体大小
            plt.xticks(fontsize=20)
            plt.yticks(fontsize=20)

            # 標示x軸(labelpad代表與圖片的距離)
            plt.xlabel("month", fontsize=30, labelpad = 15)
            # 標示y軸(labelpad代表與圖片的距離)
            plt.ylabel("Worth", fontsize=30, labelpad = 20)

            plt.legend(loc = "best", fontsize=20)

            path_record = "C:/Users/user/桌面/Quant-investment/side_project/result/plt/5/"

            path1 = path_record + str(y1)+ "_" + str(y2)+'.png'
            plt.savefig(path1)
            plt.close()
    
print("Done")