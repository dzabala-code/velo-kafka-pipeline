import time
from kafka import KafkaConsumer
import json


topic_empty="empty-stations"

consumer = KafkaConsumer(topic_empty, bootstrap_servers="localhost:9092", group_id="alert-empty")


for message in consumer:
        texte=message.value.decode("utf-8")
        station = json.loads(texte)
        

        if station['status']=='empty':
            city=station['contract_name']
            location=station['address']
            print(f'station in {city} at {location} is now empty')


                        
