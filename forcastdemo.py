import pandas as pd
import pymysql
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn import linear_model

# 打开数据库连接
db = pymysql.connect("rm-uf6871zn4f8aq9vpvro.mysql.rds.aliyuncs.com", "user", "AvtarGroup1234", "job_data")
# 使用cursor()方法获取操作游标
cursor = db.cursor()
# SQL 查询语句
sql = "select result from tb_analyze_time_salary"
x_list = []
y_list = []
try:
    # 执行SQL语句
    cursor.execute(sql)
    # 获取所有记录列表
    results = cursor.fetchall()
    list = results[0]
    x_list = []
    y_list = []
    for i in list:
        # string转换成dict的方法
        list_ds = eval(i)
        for j in list_ds:
            x_list.append(j['date'])
            y_list.append(j['salary'])
except:
   print ("Error: unable to fetch data")

tmp = []
now = 0
for date in x_list:
    split_str = date.split('-')
    month = int(split_str[0])
    day = int(split_str[1])
    time = month*30+day
    now = time + 1
    tmp.append(time)

x_list = tmp
# Pandas将列表（List）转换为数据框（Dataframe）
dic = {'x_list' : x_list,'y_list' : y_list}
aa=pd.DataFrame(dic)
aa.head()
X = np.array(aa[['x_list']])
Y = np.array(aa[['y_list']])

# 划分数据集
x_train, x_test, y_train, y_test = train_test_split(X, Y, test_size = 0.2, random_state=0)

#将训练集代入到线性回归模型中训练(岭回归)
model = linear_model.LinearRegression()
model.fit (x_train,y_train)

# #线性回归模型的斜率
# model.coef_
# #线性回归模型的截距
# model.intercept_
# #判定系数R Square
# model.score(x_train,y_train)

#输入自变量预测因变量
#新版的sklearn中，所有的数据都应该是二维矩阵，哪怕它只是单独一行或一列（比如前面做预测时，仅仅只用了一个样本数据），所以需要使用.reshape(1,-1)进行转换
now = np.array(now).reshape(1, -1)
# 预测薪水(保留两位小数)
predict_salary = model.predict(now)
print(type(predict_salary))
predict_salary = round(float(predict_salary),2)
# 将预测值插入数据库
sql = "select result from tb_analyze_time_salary"
# 获取游标
cursor = db.cursor()
# SQL 更新语句
sql = "update tb_analyze_time_salary set forecast =  '%.2f' where id = 1"%(predict_salary)
try:
    cursor.execute(sql)
    db.commit()
except:
    print("insert error")

# 关闭数据库连接
db.close()



# #将测试集的自变量代入到模型预测因变量
# list(model.predict(x_test))
#
# #显示测试集的因变量
# list(y_test)
#
# #计算误差平方和
# ((y_test - model.predict(x_test)) **2).sum()