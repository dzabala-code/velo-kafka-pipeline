import time
from kafka import KafkaConsumer
from kafka import KafkaProducer
import json


topic_empty="empty-stations"
topic_station = "stations-status"
consumer = KafkaConsumer(topic_station, bootstrap_servers="localhost:9092", group_id="stations-empty")
producer = KafkaProducer(bootstrap_servers="localhost:9092",
                            value_serializer=str.encode)

prior_history={}

for message in consumer:
    texte=message.value.decode("utf-8")
    station = json.loads(texte)
    bikes_now=station['station']['available_bikes']

    id_station=station['station']['name']
    station_target=prior_history.get(id_station)

    if id_station in prior_history:

        if (bikes_now!=0) and (station_target==0):
            message = json.dumps({**station['station'],"status":'available'})
            producer.send(topic_empty, message)

        if (bikes_now==0) and (station_target!=0):
            message = json.dumps({**station['station'],"status":'empty'})
            producer.send(topic_empty, message)

    prior_history[id_station] = bikes_now
                        
