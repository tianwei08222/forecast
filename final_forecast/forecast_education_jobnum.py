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

class forecast_education_salary:
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
    return round(s / len(li), 2)

tr_technical_secondary = []
tr_junior_college = []
tr_undergraduate = []
tr_master = []
tr_learned_scholar = []

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
    # 不同学历下的薪资
    for date_num in range(31, 38):
        date_num = np.array(date_num).reshape(1, -1)
        predict_value = model.predict(date_num)
        predict_value = round(float(predict_value), 2)
        if name == "technical_secondary":
            tr_technical_secondary.append(predict_value)
        elif name == "junior_college":
            tr_junior_college.append(predict_value)
        elif name == "undergraduate":
            tr_undergraduate.append(predict_value)
        elif name == "master":
            tr_master.append(predict_value)
        elif name == "learned_scholar":
            tr_learned_scholar.append(predict_value)


def insert_to_sql(dir,date,list):
    list = str(list)
    list = list.replace('"', '\\"')
    list = list.replace('\'', '')
    con = pymysql.connect("rm-uf6871zn4f8aq9vpvro.mysql.rds.aliyuncs.com", "user", "AvtarGroup1234", "job_data")
    cursor = con.cursor()
    sql = "INSERT INTO tb_forecast_education_jobnum (jobtype_two_id,time,result) VALUES (%d,'%s','%s')"
    data = (dir, date, list)
    try:
        cursor.execute(sql % data)
        con.commit()
    except:
        print("Error: unable to fetch data")


if __name__ == '__main__':
    con = pymysql.connect("rm-uf6871zn4f8aq9vpvro.mysql.rds.aliyuncs.com", "user", "AvtarGroup1234", "job_data")
    cursor = con.cursor()
    sql = "delete from tb_forecast_education_jobnum"
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
        # technical_secondary  中专
        # junior_college 大专
        # undergraduate  本科
        # master 硕士
        # learned_scholar 博士
        technical_secondary = []
        junior_college = []
        undergraduate = []
        master = []
        learned_scholar = []
        for date_num in range(29, -1, -1):
            today = getDatetimeYesterday(date_num)
            forecast = forecast_education_salary(dir, today)
            final_results = forecast.search(today)
            dic = {}
            dic['date'] = str(today)
            # 不同学历薪资
            dic['technical_secondary'] = 0
            dic['junior_college'] = 0
            dic['undergraduate'] = 0
            dic['master'] = 0
            dic['learned_scholar'] = 0
            for it in final_results:
                if it['education_level'] is not None and len(it['education_level']) > 0:
                    if "中专" in it['education_level']:
                        dic['technical_secondary'] += 1
                if it['education_level'] is not None and len(it['education_level']) > 0:
                    if "大专" in it['education_level']:
                        dic['junior_college'] += 1
                if it['education_level'] is not None and len(it['education_level']) > 0:
                    if "本科" in it['education_level']:
                        dic['undergraduate'] += 1
                if it['education_level'] is not None and len(it['education_level']) > 0:
                    if "硕士" in it['education_level']:
                        dic['master'] += 1
                if it['education_level'] is not None and len(it['education_level']) > 0:
                    if "博士" in it['education_level']:
                        dic['learned_scholar'] += 1

            # 当天为空（没爬）自动填充
            if dic['technical_secondary'] == 0:
                dic['technical_secondary'] = 200
            if dic['junior_college'] == 0:
                dic['junior_college'] = 1500
            if dic['undergraduate'] == 0:
                dic['undergraduate'] = 21000
            if dic['master'] == 0:
                dic['master'] = 3200
            if dic['learned_scholar'] == 0:
                dic['learned_scholar'] = 1600

            technical_secondary.append(dic['technical_secondary'])
            junior_college.append(dic['junior_college'])
            undergraduate.append(dic['undergraduate'])
            master.append(dic['master'])
            learned_scholar.append(dic['learned_scholar'])
            res.append(json.dumps(dic))

        train(x_list, technical_secondary, "technical_secondary")
        train(x_list, junior_college, "junior_college")
        train(x_list, undergraduate, "undergraduate")
        train(x_list, master, "master")
        train(x_list, learned_scholar, "learned_scholar")

        fc_res = []
        for day in range(1, 8):
            dic = {}
            dic['date'] = str(getDatetimeNextday(day))
            dic['technical_secondary'] = tr_technical_secondary[day - 1]
            dic['junior_college'] = tr_junior_college[day - 1]
            dic['undergraduate'] = tr_undergraduate[day - 1]
            dic['master'] = tr_master[day - 1]
            dic['learned_scholar'] = tr_learned_scholar[day - 1]
            fc_res.append(json.dumps(dic))
        dt = str(getDatetimeNextday(7))
        insert_to_sql(dir, dt, fc_res)

        # 历史7天各个学历职位数
        for i in range(0, 24):
            res_list = []
            for j in range(0,7):
                res_list.append(res[i+j])
            day = str(getDatetimeYesterday(23-i))
            insert_to_sql(dir, day, res_list)

        # 清空上一个方向的预测值
        tr_technical_secondary = []
        tr_junior_college = []
        tr_undergraduate = []
        tr_master = []
        tr_learned_scholar = []