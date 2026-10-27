import requests
import time
from kafka import KafkaProducer
import json
import os

url = f"https://api.jcdecaux.com/vls/v1/stations?apiKey={os.environ['JCDECAUX_API_KEY']}"
producer = KafkaProducer(bootstrap_servers="localhost:9092",
                            value_serializer=str.encode)

    
topic_name = "velib-stations"

while True :

    try :
        response=requests.get(url)
        response.raise_for_status()
        data_response=response.json()
            
    except requests.exceptions.RequestException as e:
        print(f"Error during request : {e}")
        time.sleep(10)
        continue


    for station in data_response:
        message = json.dumps(station)
        producer.send(topic_name, message)

    print(f"{time.strftime('%H:%M:%S')} : {len(data_response)} stations envoyées à {topic_name}")
        

    time.sleep(10)
    