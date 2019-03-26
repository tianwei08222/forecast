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

class forecast_workexper_jobnum_salary:
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
    return round(s / len(li),2)


tr_zero_jobnum = []
tr_one_jobnum = []
tr_two_jobnum = []
tr_three_jobnum = []
tr_four_jobnum = []
tr_five_jobnum = []

tr_zero_salary = []
tr_one_salary = []
tr_two_salary = []
tr_three_salary = []
tr_four_salary = []
tr_five_salary = []


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
        if "salary" in name:
            predict_value = round(float(predict_value), 2)
        else :
            predict_value = int(predict_value)
        if name == "zero_jobnum":
            tr_zero_jobnum.append(predict_value)
        elif name == "one_jobnum":
            tr_one_jobnum.append(predict_value)
        elif name == "two_jobnum":
            tr_two_jobnum.append(predict_value)
        elif name == "three_jobnum":
            tr_three_jobnum.append(predict_value)
        elif name == "four_jobnum":
            tr_four_jobnum.append(predict_value)
        elif name == "five_jobnum":
            tr_five_jobnum.append(predict_value)
        elif name == "zero_salary":
            tr_zero_salary.append(predict_value)
        elif name == "one_salary":
            tr_one_salary.append(predict_value)
        elif name == "two_salary":
            tr_two_salary.append(predict_value)
        elif name == "three_salary":
            tr_three_salary.append(predict_value)
        elif name == "four_salary":
            tr_four_salary.append(predict_value)
        elif name == "five_salary":
            tr_five_salary.append(predict_value)

if __name__ == '__main__':
    con = pymysql.connect("rm-uf6871zn4f8aq9vpvro.mysql.rds.aliyuncs.com", "user", "AvtarGroup1234", "job_data")
    cursor = con.cursor()
    sql = "delete from tb_forecast_workexper_jobnum_salary"
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
        # 历史30天(按时间排序后)的职位数和薪资
        zero_jobnum = []
        one_jobnum = []
        two_jobnum = []
        three_jobnum = []
        four_jobnum = []
        five_jobnum = []

        zero_salary = []
        one_salary = []
        two_salary = []
        three_salary = []
        four_salary = []
        five_salary = []
        for date_num in range(29, -1, -1):
            today = getDatetimeYesterday(date_num)
            tmp_dir = str(dir)
            tmp_today = str(today)
            forecast = forecast_workexper_jobnum_salary(tmp_dir, tmp_today)
            final_results = forecast.search(today)
            dic = {}
            dic['date'] = str(today)
            # 0、1、2、3、4、5年工作经验的职位数
            dic['zero_jobnum'] = 0
            dic['one_jobnum'] = 0
            dic['two_jobnum'] = 0
            dic['three_jobnum'] = 0
            dic['four_jobnum'] = 0
            dic['five_jobnum'] = 0
            # 0、1、2、3、4、5年工作经验的薪资集合
            zero_all = []
            one_all = []
            two_all = []
            three_all = []
            four_all = []
            five_all = []
            # 0、1、2、3、4、5年工作经验的平均薪资
            dic['zero_salary'] = 0
            dic['one_salary'] = 0
            dic['two_salary'] = 0
            dic['three_salary'] = 0
            dic['four_salary'] = 0
            dic['five_salary'] = 0

            for it in final_results:
                if it['work_exper'] is not None and len(it['work_exper']) > 0:
                    if "无" in it['work_exper']:
                        dic['zero_jobnum'] += 1
                        if it['job_salary_min'] is not None and len(it['job_salary_min']) > 0 and it['job_salary_max'] is not None and len(it['job_salary_max']) > 0:
                            zero_all.append(round((float(it['job_salary_min']) + float(it['job_salary_max'])) / 2))
                if it['work_exper'] is not None and len(it['work_exper']) > 0:
                    if "1" in it['work_exper']:
                        dic['one_jobnum'] += 1
                        if it['job_salary_min'] is not None and len(it['job_salary_min']) > 0 and it['job_salary_max'] is not None and len(it['job_salary_max']) > 0:
                            one_all.append(round((float(it['job_salary_min']) + float(it['job_salary_max'])) / 2))
                if it['work_exper'] is not None and len(it['work_exper']) > 0:
                    if "2" in it['work_exper']:
                        dic['two_jobnum'] += 1
                        if it['job_salary_min'] is not None and len(it['job_salary_min']) > 0 and it['job_salary_max'] is not None and len(it['job_salary_max']) > 0:
                            two_all.append((float(it['job_salary_min']) + float(it['job_salary_max'])) / 2)
                if it['work_exper'] is not None and len(it['work_exper']) > 0:
                    if "3" in it['work_exper']:
                        dic['three_jobnum'] += 1
                        if it['job_salary_min'] is not None and len(it['job_salary_min']) > 0 and it['job_salary_max'] is not None and len(it['job_salary_max']) > 0:
                            three_all.append(round((float(it['job_salary_min']) + float(it['job_salary_max'])) / 2))
                if it['work_exper'] is not None and len(it['work_exper']) > 0:
                    if "4" in it['work_exper']:
                        dic['four_jobnum'] += 1
                        if it['job_salary_min'] is not None and len(it['job_salary_min']) > 0 and it['job_salary_max'] is not None and len(it['job_salary_max']) > 0:
                            four_all.append(round((float(it['job_salary_min']) + float(it['job_salary_max'])) / 2))
                if it['work_exper'] is not None and len(it['work_exper']) > 0:
                    if "5" in it['work_exper']:
                        dic['five_jobnum'] += 1
                        if it['job_salary_min'] is not None and len(it['job_salary_min']) > 0 and it['job_salary_max'] is not None and len(it['job_salary_max']) > 0:
                            five_all.append(round((float(it['job_salary_min']) + float(it['job_salary_max'])) / 2))
            dic['zero_salary'] = round(float(aver(zero_all)), 2)
            dic['one_salary'] = round(float(aver(one_all)), 2)
            dic['two_salary'] = round(float(aver(two_all)), 2)
            dic['three_salary'] = round(float(aver(three_all)), 2)
            dic['four_salary'] = round(float(aver(four_all)), 2)
            dic['five_salary'] = round(float(aver(five_all)), 2)
            # 当天为空（没爬）自动填充
            if dic['zero_salary'] == 0:
                dic['zero_salary'] = 6.50
            if dic['one_salary'] == 0:
                dic['one_salary'] = 8.89
            if dic['two_salary'] == 0:
                dic['two_salary'] = 11.45
            if dic['three_salary'] == 0:
                dic['three_salary'] = 12.23
            if dic['four_salary'] == 0:
                dic['four_salary'] = 17.34
            if dic['five_salary'] == 0:
                dic['five_salary'] = 23.72

            zero_salary.append(dic['zero_salary'])
            one_salary.append(dic['one_salary'])
            two_salary.append(dic['two_salary'])
            three_salary.append(dic['three_salary'])
            four_salary.append(dic['four_salary'])
            five_salary.append(dic['five_salary'])

            # 当天为空（没爬）自动填充
            if dic['zero_jobnum'] == 0:
                dic['zero_jobnum'] = 19000
            if dic['one_jobnum'] == 0:
                dic['one_jobnum'] = 7300
            if dic['two_jobnum'] == 0:
                dic['two_jobnum'] = 9000
            if dic['three_jobnum'] == 0:
                dic['three_jobnum'] = 12000
            if dic['four_jobnum'] == 0:
                dic['four_jobnum'] = 11000
            if dic['five_jobnum'] == 0:
                dic['five_jobnum'] = 8800

            zero_jobnum.append(dic['zero_jobnum'])
            one_jobnum.append(dic['one_jobnum'])
            two_jobnum.append(dic['two_jobnum'])
            three_jobnum.append(dic['three_jobnum'])
            four_jobnum.append(dic['four_jobnum'])
            five_jobnum.append(dic['five_jobnum'])
            res.append(json.dumps(dic))
        train(x_list, zero_jobnum, "zero_jobnum")
        train(x_list, one_jobnum, "one_jobnum")
        train(x_list, two_jobnum, "two_jobnum")
        train(x_list, three_jobnum, "three_jobnum")
        train(x_list, four_jobnum, "four_jobnum")
        train(x_list, five_jobnum, "five_jobnum")

        train(x_list, zero_salary, "zero_salary")
        train(x_list, one_salary, "one_salary")
        train(x_list, two_salary, "two_salary")
        train(x_list, three_salary, "three_salary")
        train(x_list, four_salary, "four_salary")
        train(x_list, five_salary, "five_salary")

        for day in range(1, 8):
            dic = {}
            dic['date'] = str(getDatetimeNextday(day))
            dic['zero_jobnum'] = tr_zero_jobnum[day - 1]
            dic['one_jobnum'] = tr_one_jobnum[day - 1]
            dic['two_jobnum'] = tr_two_jobnum[day - 1]
            dic['three_jobnum'] = tr_three_jobnum[day - 1]
            dic['four_jobnum'] = tr_four_jobnum[day - 1]
            dic['five_jobnum'] = tr_five_jobnum[day - 1]

            dic['zero_salary'] = round(float(tr_zero_salary[day - 1]),2)
            print("dic['zero_salary']")
            print(dic['zero_salary'])
            dic['one_salary'] = round(float(tr_one_salary[day - 1]), 2)
            dic['two_salary'] = round(float(tr_two_salary[day - 1]), 2)
            dic['three_salary'] = round(float(tr_three_salary[day - 1]), 2)
            dic['four_salary'] = round(float(tr_four_salary[day - 1]), 2)
            dic['five_salary'] = round(float(tr_five_salary[day - 1]), 2)
            res.append(json.dumps(dic))
        res = str(res)
        res = res.replace('"', '\\"')
        res = res.replace('\'', '')
        con = pymysql.connect("rm-uf6871zn4f8aq9vpvro.mysql.rds.aliyuncs.com", "user", "AvtarGroup1234", "job_data")
        cursor = con.cursor()
        sql = "INSERT INTO tb_forecast_workexper_jobnum_salary (jobtype_two_id,time,result) VALUES (%d,'%s','%s')"
        data = (dir, str(getDatetimeToday().date()), res)
        try:
            cursor.execute(sql % data)
            con.commit()
        except:
            print("Error: unable to fetch data")
        tr_zero_jobnum = []
        tr_one_jobnum = []
        tr_two_jobnum = []
        tr_three_jobnum = []
        tr_four_jobnum = []
        tr_five_jobnum = []

        tr_zero_salary = []
        tr_one_salary = []
        tr_two_salary = []
        tr_three_salary = []
        tr_four_salary = []
        tr_five_salary = []
