# -*- coding: gb2312 -*-
from sklearn.feature_extraction.text import CountVectorizer, TfidfVectorizer
import jieba
import pymysql
import re

def cut_word(text):
    return ",".join(jieba.cut(text))

def tfidf_demo(data):
    data = re.sub(r"[0-9\s+\.\!\/_,$%^*()?;；:-【】+\"\']+|[+??！，;:。？、~@#￥%……&*（）]+", " ", data)
    data_new = []
    data_new.append(cut_word(data))
    print(len(data_new))
    data = []
    for i in data_new:
        if(i[0].isalpha()):
            print(i)
            data.append(i)
    # 1、实例化一个转换器类
    transfer = TfidfVectorizer(stop_words=["一种", "所以"])
    # 2、调用fit_transform 得到权重矩阵
    if(len(data)>0):
        data_final = transfer.fit_transform(data)
        print("特征名字：\n", transfer.get_feature_names())
    return None


if __name__ == "__main__":
    dic = ['1','2','3']
    print(','.join(dic))
    print(type(dic))
     # db = pymysql.connect("127.0.0.1", "root", "123456", "test")
     # cursor = db.cursor()
     # sql = "select job_info2 from software_enginee"
     # cursor.execute(sql)
     # results = cursor.fetchall()
     # data = "1、计算机相关专业全日制本科及以上学历，8年以上工作经验，3年以上大数据相关经验；,2、精通java或scala语言，熟悉spark，hadoop，kafka，hive，hbase，zookeeper等大数据相关技术；,3、精通SQL语句并对redis，mongodb等nosql数据库有一定经验。,4、责任心强，工作踏实，有团队协作精神，有一定管理经验和沟通能力强者优先；,5、有自然语言处理或舆情分析等相关经验者优先。,,,1、负责公安行业AI和大数据平台搭建,2、根据公安业务需求，制定数据仓库和数据分析平台的整体技术框架方案并根据业务扩展持续更新,3、攻克技术难关，保证大数据系统的稳定运行,4、带领技术开发团队完成大数据产品的开发"
     # tfidf_demo(data)
     # data = "沟通,团队,合作,需求,分析,系统,设计,逻辑,学习,协作,综合,优化,表达,解决问题,理论,编码,架构,研究,攻关,抗压,组织,规划,书面,管理,分工,调研,市场,文档,洞察,定位,发现,评估,编写,写作,分配,协助,理解,梳理,领导,决策,反应,培训,挖掘,引导,战略,加工,思维,搜集,开发,运维,创意,跟踪,判断,构思,撰写,把握,定义,总结,反思"
     # f = open('ability.txt','w')
     # f.write(data)

     # with open('technology.txt', 'r') as f:
     #     for line in f:
     #         result = list(line.split(','))
     # print(result)