from kafka import KafkaConsumer
import json

'''
    消费者demo
    消费test_lyl2主题中的数据
    注意事项：如需以json格式读取数据需加上value_deserializer参数
'''


consumer = KafkaConsumer('test_lyl2',group_id="lyl-gid1",
                         bootstrap_servers=['10.0.0.23:9092'],
                         auto_offset_reset='earliest',value_deserializer=json.loads
                         )

for message in consumer:
    print(message.value)