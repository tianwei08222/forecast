# -*- coding: utf-8 -*-
from elasticsearch import helpers
from elasticsearch import Elasticsearch
from datetime import datetime, date, timedelta
import pandas as pd
import pymysql
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn import linear_model
import json

class forecast_companytype_jobnum:
    ES_SERVERS = [{
        'host': '10.0.0.28',
        'port': 9200
    }]
    es_client = Elasticsearch(
        hosts=ES_SERVERS
    )
    es_search_options = None

    # 检索选项
    def __init__(self, dir, date):
        self.es_search_options = {
            "query": {
                "bool": {
                    "must": [
                        {"term": {"direction": dir}},
                        {"term": {"relase_date": date}}
                    ]
                }
            }
        }

    def search(self, date):
        es_result = self.get_search_result(self.es_search_options)
        final_result = self.get_result_list(es_result)
        return final_result

    def get_result_list(self, es_result):
        final_result = []
        for item in es_result:
            final_result.append(item['_source'])
        return final_result

    def get_search_result(self, es_search_options, scroll='5m', index='job_data', doc_type='jdbc', timeout="1m"):
        es_result = helpers.scan(
            client=self.es_client,
            query=self.es_search_options,
            scroll=scroll,
            index=index,
            doc_type=doc_type,
            timeout=timeout
        )
        return es_result


def getDatetimeToday():
    t = date.today()  # date类型
    dt = datetime.strptime(str(t), '%Y-%m-%d')  # date转str再转datetime
    return dt


def getDatetimeYesterday(num):
    today = getDatetimeToday()  # datetime类型当前日期
    yesterday = today + timedelta(days=-num)  # 减去一天
    return yesterday.date()


def getDatetimeNextday(num):
    today = getDatetimeToday()  # datetime类型当前日期
    yesterday = today + timedelta(days=num)  # 一天
    return yesterday.date()


def aver(li):
    if len(li) == 0:
        return 0
    s = 0
    for i in range(len(li)):
        s += li[i]
    return s / len(li)

tr_foreign = []
tr_joint_stock = []
tr_private_limited = []
tr_nationalized_business = []

def train(x_list, y_list, name):
    dic = {'x_list': x_list, 'y_list': y_list}
    aa = pd.DataFrame(dic)
    aa.head()
    X = np.array(aa[['x_list']])
    Y = np.array(aa[['y_list']])
    # 划分数据集
    x_train, x_test, y_train, y_test = train_test_split(X, Y, test_size=0.2, random_state=0)
    # 将训练集代入到线性回归模型中训练(岭回归)
    model = linear_model.LinearRegression()
    model.fit(x_train, y_train)
    # 预测薪水(保留两位小数)
    for date_num in range(31, 38):
        date_num = np.array(date_num).reshape(1, -1)
        predict_value = model.predict(date_num)
        predict_value = int(predict_value)
        if name == "foreign":
            tr_foreign.append(predict_value)
        elif name == "joint_stock":
            tr_joint_stock.append(predict_value)
        elif name == "private_limited":
            tr_private_limited.append(predict_value)
        elif name == "nationalized_business":
            tr_nationalized_business.append(predict_value)

def insert_to_sql(dir,date,list):
    list = str(list)
    list = list.replace('"', '\\"')
    list = list.replace('\'', '')
    con = pymysql.connect("rm-uf6871zn4f8aq9vpvro.mysql.rds.aliyuncs.com", "user", "AvtarGroup1234", "job_data")
    cursor = con.cursor()
    sql = "INSERT INTO tb_forecast_companytype_jobnum (jobtype_two_id,time,result) VALUES (%d,'%s','%s')"
    data = (dir, date, list)
    try:
        cursor.execute(sql % data)
        con.commit()
    except:
        print("Error: unable to fetch data")


if __name__ == '__main__':
    con = pymysql.connect("rm-uf6871zn4f8aq9vpvro.mysql.rds.aliyuncs.com", "user", "AvtarGroup1234", "job_data")
    cursor = con.cursor()
    sql = "delete from tb_forecast_companytype_jobnum"
    try:
        cursor.execute(sql)
        con.commit()
    except:
        print("Error!")
    x_list = []
    for i in range(1, 31):
        x_list.append(i)
    for dir in range(1, 10):
        res = []
        # 历史30天(按时间排序后)的薪资
        foreign = []
        joint_stock = []
        private_limited = []
        nationalized_business = []
        for date_num in range(29, -1, -1):
            today = getDatetimeYesterday(date_num)
            forecast = forecast_companytype_jobnum(dir, today)
            final_results = forecast.search(today)
            dic = {}
            dic['date'] = str(today)
            # 各公司类型的薪资 外资、合资、民营、国企 （职位数）
            dic['foreign'] = 0
            dic['joint_stock'] = 0
            dic['private_limited'] = 0
            dic['nationalized_business'] = 0
            for it in final_results:
                if it['company_type'] is not None and len(it['company_type']) > 0:
                    if "外资" in it['company_type']:
                        dic['foreign'] += 1
                if it['company_type'] is not None and len(it['company_type']) > 0:
                    if "合资" in it['company_type']:
                        dic['joint_stock'] += 1
                if it['company_type'] is not None and len(it['company_type']) > 0:
                    if "民营" in it['company_type']:
                        dic['private_limited'] += 1
                if it['company_type'] is not None and len(it['company_type']) > 0:
                    if "国企" in it['company_type']:
                        dic['nationalized_business'] += 1
            # 当天为空（没爬）自动填充
            if dic['foreign'] == 0:
                dic['foreign'] = 6000
            if dic['joint_stock'] == 0:
                dic['joint_stock'] = 2000
            if dic['private_limited'] == 0:
                dic['private_limited'] = 10000
            if dic['nationalized_business'] == 0:
                dic['nationalized_business'] = 4000

            foreign.append(dic['foreign'])
            joint_stock.append(dic['joint_stock'])
            private_limited.append(dic['private_limited'])
            nationalized_business.append(dic['nationalized_business'])
            # res.append(dic)
            res.append(json.dumps(dic))

        train(x_list, foreign, "foreign")
        train(x_list, joint_stock, "joint_stock")
        train(x_list, private_limited, "private_limited")
        train(x_list, nationalized_business, "nationalized_business")

        fc_res = []
        for day in range(1, 8):
            dic = {}
            dic['date'] = str(getDatetimeNextday(day))
            dic['foreign'] = tr_foreign[day - 1]
            dic['joint_stock'] = tr_joint_stock[day - 1]
            dic['private_limited'] = tr_private_limited[day - 1]
            dic['nationalized_business'] = tr_nationalized_business[day - 1]
            fc_res.append(json.dumps(dic))
        dt = str(getDatetimeNextday(7))
        insert_to_sql(dir, dt, fc_res)

        # 历史7天各个学历职位数
        for i in range(0, 24):
            res_list = []
            for j in range(0, 7):
                res_list.append(res[i + j])
            day = str(getDatetimeYesterday(23 - i))
            insert_to_sql(dir, day, res_list)

        # 清空上一个方向
        tr_foreign = []
        tr_joint_stock = []
        tr_private_limited = []
        tr_nationalized_business = []