import pandas as pd
import pymysql
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn import linear_model
import json

class Forecast_Companytype_Salary:
    x_list = []
    foreign_list = []
    joint_stock_list = []
    private_limited_list = []
    nationalized_business_list = []
    fc_foreign = []
    fc_joint_stock = []
    fc_private_limited = []
    fc_nationalized_business = []
    date_list = []
    result = []
    job_num_dict = {}
    now_month = 0
    now_day = 0
    month = [31,28,31,30,31,30,31,31,30,31,30,31]
    db = pymysql.connect("rm-uf6871zn4f8aq9vpvro.mysql.rds.aliyuncs.com", "user", "Group1234", "job_data")

    def get_data(self):
        # 使用cursor()方法获取操作游标
        cursor = self.db.cursor()
        # SQL 查询语句
        sql = "select result from tb_forecast_companytype_salary"
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
                    self.foreign_list.append(j['foreign'])
                    self.joint_stock_list.append(j['joint_stock'])
                    self.private_limited_list.append(j['private_limited'])
                    self.nationalized_business_list.append(j['nationalized_business'])
        except:
           print ("Error: unable to fetch data")

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

    def child_train(self,y_list,res_list,com_type):
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
            predict_value = round(float(predict_value), 2)
            format_time = str(self.now_month).zfill(2) + '-' + str(self.now_day).zfill(2)
            self.date_list.append(format_time)
            res_list.append(predict_value)

        self.now_month = tmp_now_month
        self.now_day = tmp_now_day

    def train(self):
        self.get_x_list()
        # 外资、合资、民营、国企
        self.child_train(self.foreign_list,self.fc_foreign,"foreign")
        self.child_train(self.joint_stock_list,self.fc_joint_stock,"joint_stock")
        self.child_train(self.private_limited_list,self.fc_private_limited,"private_limited")
        self.child_train(self.nationalized_business_list,self.fc_nationalized_business,"nationalized_business")
        for i in range(7):
            tmp_dict = {}
            tmp_dict['date'] = self.date_list[i]
            tmp_dict['foreign'] = self.fc_foreign[i]
            tmp_dict['joint_stock'] = self.fc_joint_stock[i]
            tmp_dict['private_limited'] = self.fc_private_limited[i]
            tmp_dict['nationalized_business'] = self.fc_nationalized_business[i]
            self.result.append(json.dumps(tmp_dict))
        self.to_sql()

    def to_sql(self):
        self.result = str(self.result)
        # 去掉转义字符的干扰
        self.result = self.result.replace('"','\\"')
        self.result = self.result.replace('\'','')
        cursor = self.db.cursor()
        sql = "update tb_forecast_companytype_salary set forecast = '%s' where id = 1"%(self.result)
        print(sql)
        try:
            cursor.execute(sql)
            self.db.commit()
        except:
            print("insert error")
        self.db.close()

if __name__ == "__main__":
    p = Forecast_Companytype_Salary()
    p.get_data()
    p.train()

# 外资、合资、民营、国企 （职位数）
# [{"date":"02-01","foreign":100,"joint_stock":125,"private_limited":140,"nationalized_business":150},{"date":"02-02","foreign":110,"joint_stock":120,"private_limited":120,"nationalized_business":160}]

# 外资、合资、民营、国企 （薪资）
# [{"date":"02-01","foreign":100,"joint_stock":125,"private_limited":140,"nationalized_business":150},{"date":"02-02","foreign":110,"joint_stock":120,"private_limited":120,"nationalized_business":160}]

# 本科、专科、硕士、博士 （职位数）
# [{"date":"02-01","undergraduate":15479,"junior_college":12896,"master":130,"learned_scholar":6477},{"date":"02-02","undergraduate":1479,"junior_college":1286,"master":1308,"learned_scholar":6007}]

# 本科、专科、硕士、博士 （薪资）
# [{"date":"02-01","undergraduate":15479,"junior_college":12896,"master":130,"learned_scholar":6477},{"date":"02-02","undergraduate":1479,"junior_college":1286,"master":1308,"learned_scholar":6007}]

# 该方向职位数
# [{"date":"01-10","jobnum":"1230"},{"date":"01-11","jobnum":"1228"},{"date":"01-12","jobnum":"1224"},{"date":"01-13","jobnum":"1210"},{"date":"01-14","jobnum":"1233"},{"date":"01-15","jobnum":"1211"},{"date":"01-16","jobnum":"1213"}]

# 工作经验年限与职位数
# [{"date":"01-14","one":2010,"two":2230,"three":2228,"four":2264,"five":2310},{"date":"01-15","one":2020,"two":2290,"three":2328,"four":2364,"five":2410},{"date":"01-16","one":2110,"two":2130,"three":2428,"four":2964,"five":2910}]

# 工作经验年限与薪资
# [{"date":"01-14","one":2010,"two":2230,"three":2228,"four":2264,"five":2310},{"date":"01-15","one":2020,"two":2290,"three":2328,"four":2364,"five":2410},{"date":"01-16","one":2110,"two":2130,"three":2428,"four":2964,"five":2910}]
