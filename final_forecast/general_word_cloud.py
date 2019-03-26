# -*- coding: utf-8 -*-
from elasticsearch import helpers
from elasticsearch import Elasticsearch
from datetime import datetime, date, timedelta
import pandas as pd
import pymysql
import jieba
import json
import operator

class general_word_cloud:
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


if __name__ == '__main__':
    con = pymysql.connect("rm-uf6871zn4f8aq9vpvro.mysql.rds.aliyuncs.com", "user", "AvtarGroup1234", "job_data")
    cursor = con.cursor()
    sql = "delete from tb_general_word_cloud"
    try:
        cursor.execute(sql)
        con.commit()
    except:
        print("Error!")

    key_word_list = []
    with open('keyword.txt', 'r', encoding='utf-8') as f:
        for i in f:
            key_word_list = list(i.split(','))

    for dir in range(1, 10):
        res_dic = {}
        for word in key_word_list:
            res_dic[word] = 0
        for date_num in range(5, -1, -1):
            today = getDatetimeYesterday(date_num)
            forecast = general_word_cloud(dir, today)
            final_results = forecast.search(today)
            for it in final_results:
                word_list = jieba.cut(it['job_require'])
                for i in word_list:
                    i = i.lower()
                    if i in key_word_list:
                        res_dic[i] += 1
        ans_list = sorted(res_dic.items(), key=operator.itemgetter(1), reverse=True)
        results = []

        cnt = 0
        for it in ans_list:
            cnt += 1
            if cnt > 50 :
                break
            dic = {}
            dic['name'] = it[0]
            #print(it[0])
            dic['value'] = it[1]
            #print(it[1])
            results.append(json.dumps(dic))
            print(json.dumps(dic))

        results = str(results)
        results = results.replace('"', '\\"')
        results = results.replace('\'', '')
        con = pymysql.connect("rm-uf6871zn4f8aq9vpvro.mysql.rds.aliyuncs.com", "user", "AvtarGroup1234", "job_data")
        cursor = con.cursor()
        sql = "INSERT INTO tb_general_word_cloud (jobtype_two_id,time,result) VALUES (%d,'%s','%s')"
        date = getDatetimeToday().date()
        data = (dir, str(date), results)
        try:
            cursor.execute(sql % data)
            con.commit()
        except:
            print("Error: unable to fetch data")
