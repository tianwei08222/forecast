# -*- coding: utf-8 -*-

import pymysql.cursors
import jieba

class JobspiderPipeline(object):
    able_word_set = set()
    tech_word_set = set()
    def __init__(self):
        able_word_list = []
        tech_word_list = []
        with open('ability.txt', 'r',encoding='gbk') as f:
            for i in f:
                able_word_list = list(i.split(','))
        for i in able_word_list:
            self.able_word_set.add(i)
        with open('technology.txt', 'r') as f:
            for i in f:
                tech_word_list = list(i.split(','))
        for i in tech_word_list:
            self.tech_word_set.add(i)

    # 能力和技术关键词分词结果集
    able_word = set()
    tech_word = set()
    def cut(self,str):
        self.able_word = set()
        self.tech_word = set()
        key_word_list = jieba.cut(str)
        for i in key_word_list:
            if i in self.able_word_set:
                self.able_word.add(i)
            elif i in self.tech_word_set:
                self.tech_word.add(i)

    connect = pymysql.connect("rm-uf6871zn4f8aq9vpvro.mysql.rds.aliyuncs.com", "user", "Group1234", "job_data")
    # 获取游标
    cursor = connect.cursor()
    #  数据插入mysql
    def process_item(self):
        sql = "select * from tb_job_info_new where id < 10"
        self.cursor.execute(sql)
        res = self.cursor.fetchall()


        # if res[0] is not None:
        #     self.cut(res[0])
        #     # 执行SQL语句
        #     self.cursor.execute(sql)
        #     # 提交到数据库执行
        #     self.connect.commit()

    def close_spider(self, spider):
        self.cursor.close()
        self.connect.close()

if __name__ == "__main__":
    jp = JobspiderPipeline()
    jp.process_item()