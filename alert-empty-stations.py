import time
from kafka import KafkaConsumer
import json


topic_empty="empty-stations"

consumer = KafkaConsumer(topic_empty, bootstrap_servers="localhost:9092", group_id="isempty")


for message in consumer:
        texte=message.value.decode("utf-8")
        station = json.loads(texte)
        status=station['status']

        if status=='empty':
            city=station['station']['contract_name']
            location=station['station']['address']
            print(f'station in {city} at {location} is now empty')


                        
