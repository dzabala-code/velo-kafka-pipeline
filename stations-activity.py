import time
from kafka import KafkaConsumer
from kafka import KafkaProducer
import json

topic_name = "velib-stations"
topic_station = "stations-status"
consumer = KafkaConsumer(topic_name, bootstrap_servers="localhost:9092", group_id="stations-activity")
producer = KafkaProducer(bootstrap_servers="localhost:9092",
                            value_serializer=str.encode)

prior_history ={}


for message in consumer:
    texte=message.value.decode("utf-8")
    station = json.loads(texte)

    id_station=station['name']
    station_target=prior_history.get(id_station)

    if id_station in prior_history:
        bike_rented= station['available_bike_stands'] - station_target['available_bike_stands']
        bike_back=station['available_bikes'] - station_target['available_bikes']

        prior_history[id_station] = {
                "available_bikes": station["available_bikes"],
                "available_bike_stands": station["available_bike_stands"]
        }
        if bike_back!=0 or bike_rented!=0:
            message = json.dumps({"station":station,"bike_back":bike_back,"bike_rented":bike_rented})
            producer.send(topic_station, message)

    else : 
        prior_history[id_station] = {
                        "available_bikes": station["available_bikes"],
                        "available_bike_stands": station["available_bike_stands"]
                }

    

    