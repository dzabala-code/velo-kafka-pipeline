from kafka import KafkaConsumer
from datetime import datetime

consumer=KafkaConsumer(bootstrap_servers="localhost:9092",group_id="monitor")
consumer.subscribe(pattern='.*')

for message in consumer:
    date= datetime.fromtimestamp((message.timestamp)/1000)
    print(f'Topic-name : {message.topic},Partition-id : {message.partition},offset-id : {message.offset},timestamp : {date}')