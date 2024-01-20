# from quantxlab.task import daily_task
#
# daily_task(test=True)

from quantxlab.crawler import test
test()

# from quantxlab.data import Data
# data = Data()
# print(data)
# from quantxlab.crawler import test
# test()


# import pandas as pd
# #创建一个示例DataFrame
# data = {'      Column Name 1': [1, 2, 3],
#         ' Column Name 2': [4, 5, 6]}
# df = pd.DataFrame(data)
# # 删除列名中的空格并替换为下划线
# df.rename(columns=lambda x: x.replace(' ', ''), inplace=True)
# #打印结果
# print(df)