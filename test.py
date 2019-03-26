# -*- coding: gb2312 -*-
from sklearn.feature_extraction.text import CountVectorizer, TfidfVectorizer
import jieba
import pymysql
import re

def cut_word(text):
    return ",".join(jieba.cut(text))

def tfidf_demo(data):
    data = re.sub(r"[0-9\s+\.\!\/_,$%^*()?;��:-����+\"\']+|[+??����;:������~@#��%����&*����]+", " ", data)
    data_new = []
    data_new.append(cut_word(data))
    print(len(data_new))
    data = []
    for i in data_new:
        if(i[0].isalpha()):
            print(i)
            data.append(i)
    # 1��ʵ����һ��ת������
    transfer = TfidfVectorizer(stop_words=["һ��", "����"])
    # 2������fit_transform �õ�Ȩ�ؾ���
    if(len(data)>0):
        data_final = transfer.fit_transform(data)
        print("�������֣�\n", transfer.get_feature_names())
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
     # data = "1����������רҵȫ���Ʊ��Ƽ�����ѧ����8�����Ϲ������飬3�����ϴ�������ؾ��飻,2����ͨjava��scala���ԣ���Ϥspark��hadoop��kafka��hive��hbase��zookeeper�ȴ�������ؼ�����,3����ͨSQL��䲢��redis��mongodb��nosql���ݿ���һ�����顣,4��������ǿ������̤ʵ�����Ŷ�Э��������һ��������͹�ͨ����ǿ�����ȣ�,5������Ȼ���Դ���������������ؾ��������ȡ�,,,1�����𹫰���ҵAI�ʹ�����ƽ̨�,2�����ݹ���ҵ�������ƶ����ݲֿ�����ݷ���ƽ̨�����弼����ܷ���������ҵ����չ��������,3�����˼����ѹأ���֤������ϵͳ���ȶ�����,4�����켼�������Ŷ���ɴ����ݲ�Ʒ�Ŀ���"
     # tfidf_demo(data)
     # data = "��ͨ,�Ŷ�,����,����,����,ϵͳ,���,�߼�,ѧϰ,Э��,�ۺ�,�Ż�,���,�������,����,����,�ܹ�,�о�,����,��ѹ,��֯,�滮,����,����,�ֹ�,����,�г�,�ĵ�,����,��λ,����,����,��д,д��,����,Э��,���,����,�쵼,����,��Ӧ,��ѵ,�ھ�,����,ս��,�ӹ�,˼ά,�Ѽ�,����,��ά,����,����,�ж�,��˼,׫д,����,����,�ܽ�,��˼"
     # f = open('ability.txt','w')
     # f.write(data)

     # with open('technology.txt', 'r') as f:
     #     for line in f:
     #         result = list(line.split(','))
     # print(result)