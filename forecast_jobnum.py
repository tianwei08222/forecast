import pandas as pd
import pymysql
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn import linear_model
import json

class ForecastAllJobnum:
    x_list = []
    y_list = []
    month = [31,28,31,30,31,30,31,31,30,31,30,31]
    db = pymysql.connect("rm-uf6871zn4f8aq9vpvro.mysql.rds.aliyuncs.com", "user", "Group1234", "job_data")
    def get_data(self):
        # 使用cursor()方法获取操作游标
        cursor = self.db.cursor()
        # SQL 查询语句
        sql = "select result from tb_forecast_jobnum"
        try:
            cursor.execute(sql)
            # 获取所有记录列表
            results = cursor.fetchall()
            list = results[0]
            for i in list:
                # string转换成dict的方法
                list_ds = eval(i)
                for j in list_ds:
                    self.x_list.append(j['date'])
                    self.y_list.append(j['jobnum'])
        except:
           print ("Error: unable to fetch data")

    def get_date(self,mon,day):
        date = 0
        for i in range(mon-1):
            date += self.month[i]
        return date + day

    def train(self):
        tmp = []
        now_month = 0
        now_day = 0
        now_time = 0
        for date in self.x_list:
            split_str = date.split('-')
            now_month = int(split_str[0])
            now_day = int(split_str[1])
            time = self.get_date(now_month,now_day)
            tmp.append(time)
        self.x_list = tmp
        # Pandas将列表（List）转换为数据框（Dataframe）
        dic = {'x_list' : self.x_list,'y_list' : self.y_list}
        aa=pd.DataFrame(dic)
        aa.head()
        X = np.array(aa[['x_list']])
        Y = np.array(aa[['y_list']])
        # 划分数据集
        x_train, x_test, y_train, y_test = train_test_split(X, Y, test_size = 0.2, random_state=0)
        #将训练集代入到线性回归模型中训练(岭回归)
        model = linear_model.LinearRegression()
        model.fit (x_train,y_train)

        result = []
        for i in range(7):
            tmp_dict = {}
            if( now_day<self.month[now_month] ):
                now_day += 1
            else:
                now_month += 1
            now_time = self.get_date(now_month,now_day)
            now_time = np.array(now_time).reshape(1, -1)
            predict_value = model.predict(now_time)
            # predict_salary = round(float(predict_salary), 2)
            # print(type(predict_salary))
            predict_value = int(predict_value)
            format_time = str(now_month).zfill(2) + '-' + str(now_day).zfill(2)
            tmp_dict['date'] = format_time
            tmp_dict['jobnum'] = predict_value
            result.append(json.dumps(tmp_dict))
        result = str(result)
        # 去掉转义字符的干扰
        result = result.replace('"','\\"')
        result = result.replace('\'','')
        cursor = self.db.cursor()
        sql = "update tb_forecast_jobnum set forecast = '%s' where id = 1"%(result)
        print(sql)
        try:
            cursor.execute(sql)
            self.db.commit()
        except:
            print("insert error")
        self.db.close()

if __name__ == "__main__":
    p = ForecastAllJobnum()
    p.get_data()
    p.train()

