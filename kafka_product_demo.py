from pykafka import KafkaClient
from kafka import KafkaProducer

host = '10.0.0.28:9092,10.0.0.28:9093,10.0.0.28:9094'
# host = 'tw:9092'
client = KafkaClient(hosts = host)
topic = client.topics["sortware".encode()]
# 将产生kafka同步消息，这个调用仅仅在我们已经确认消息已经发送到集群之后
with topic.get_sync_producer() as producer:
    for i in range(10000):
        producer.produce(('test message is yms  ' + str(i ** 2)).encode())


# producer = KafkaProducer( bootstrap_servers='192.168.154.133:9092')
# for i in range(10):
#     data={
#         "name":"李四",
#         "age":23,
#         "gender":"男",
#         "id":i
#     }
#     producer.send('test', data)
# producer.close()