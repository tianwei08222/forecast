import pandas as pd
import pymysql
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn import linear_model
import json

class Forecast_Workexper_Jobnum:
    x_list = []
    one_year_list = []
    two_year_list = []
    three_year_list = []
    four_year_list = []
    five_year_list = []
    fc_one_year = []
    fc_two_year = []
    fc_three_year = []
    fc_four_year = []
    fc_five_year = []
    date_list = []
    result = []
    education_dict = {}
    now_month = 0
    now_day = 0
    month = [31,28,31,30,31,30,31,31,30,31,30,31]
    db = pymysql.connect("rm-uf6871zn4f8aq9vpvro.mysql.rds.aliyuncs.com", "user", "Group1234", "job_data")

    def get_data(self):
        # 使用cursor()方法获取操作游标
        cursor = self.db.cursor()
        # SQL 查询语句
        sql = "select result from tb_forecast_workexper_jobnum"
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
                    self.one_year_list.append(j['one'])
                    self.two_year_list.append(j['two'])
                    self.three_year_list.append(j['three'])
                    self.four_year_list.append(j['four'])
                    self.five_year_list.append(j['five'])
        except:
           print("Error: unable to fetch data")

    def get_date(self,mon,day):
        date = 0
        for i in range(mon-1):
            date += self.month[i]
        return date + day

    def get_x_list(self):
        tmp = []
        for date in self.x_list:
            split_str = date.split('-')
            self.now_month = int(split_str[0])
            self.now_day = int(split_str[1])
            time = self.get_date(self.now_month, self.now_day)
            tmp.append(time)
        self.x_list = tmp

    def child_train(self,y_list,res_list):
        tmp_now_month = self.now_month
        tmp_now_day = self.now_day
        # Pandas将列表（List）转换为数据框（Dataframe）
        dic = {'x_list' : self.x_list,'y_list' : y_list}
        aa=pd.DataFrame(dic)
        aa.head()
        X = np.array(aa[['x_list']])
        Y = np.array(aa[['y_list']])
        # 划分数据集
        x_train, x_test, y_train, y_test = train_test_split(X, Y, test_size = 0.2, random_state=0)
        #将训练集代入到线性回归模型中训练(岭回归)
        model = linear_model.LinearRegression()
        model.fit (x_train,y_train)
        for i in range(7):
            if( self.now_day<self.month[self.now_month] ):
                self.now_day += 1
            else:
                self.now_month += 1
            now_time = self.get_date(self.now_month,self.now_day)
            now_time = np.array(now_time).reshape(1, -1)
            predict_value = model.predict(now_time)
            predict_value = int(predict_value)
            format_time = str(self.now_month).zfill(2) + '-' + str(self.now_day).zfill(2)
            self.date_list.append(format_time)
            res_list.append(predict_value)

        self.now_month = tmp_now_month
        self.now_day = tmp_now_day

    def train(self):
        self.get_x_list()
        # 工作经验 1 2 3 4 5年
        self.child_train(self.one_year_list,self.fc_one_year)
        self.child_train(self.two_year_list,self.fc_two_year)
        self.child_train(self.three_year_list,self.fc_three_year)
        self.child_train(self.four_year_list,self.fc_four_year)
        self.child_train(self.five_year_list,self.fc_five_year)
        for i in range(7):
            tmp_dict = {}
            tmp_dict['date'] = self.date_list[i]
            tmp_dict['one'] = self.fc_one_year[i]
            tmp_dict['two'] = self.fc_two_year[i]
            tmp_dict['three'] = self.fc_three_year[i]
            tmp_dict['four'] = self.fc_four_year[i]
            tmp_dict['five'] = self.fc_five_year[i]
            self.result.append(json.dumps(tmp_dict))
        self.to_sql()

    def to_sql(self):
        self.result = str(self.result)
        # 去掉转义字符的干扰
        self.result = self.result.replace('"','\\"')
        self.result = self.result.replace('\'','')
        cursor = self.db.cursor()
        sql = "update tb_forecast_workexper_jobnum set forecast = '%s' where id = 1"%(self.result)
        print(sql)
        try:
            cursor.execute(sql)
            self.db.commit()
        except:
            print("insert error")
        self.db.close()

if __name__ == "__main__":
    p = Forecast_Workexper_Jobnum()
    p.get_data()
    p.train()


