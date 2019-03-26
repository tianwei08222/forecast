# -*- coding: gb2312 -*-
import jieba
import pymysql

st = set()

def tfidf_demo(data):
    li = jieba.cut(data)
    for uchar in li:
        if (uchar >= u'\u0041' and uchar <= u'\u005a') or (uchar >= u'\u0061' and uchar <= u'\u007a'):
            if len(uchar) >= 3 and len(uchar) <= 10:
                st.add(uchar)

if __name__ == "__main__":
     db = pymysql.connect("127.0.0.1", "root", "123456", "test")
     # 使用cursor()方法获取操作游标
     cursor = db.cursor()
     # SQL 查询语句
     sql = "select job_info2 from software_enginee"
     cursor.execute(sql)
     # 获取所有记录列表
     results = cursor.fetchall()
     for i in results:
         if i[0] is not None:
            tfidf_demo(i[0])
     f = open('test.txt', 'w')
     for i in st:
         f.write(i+',')