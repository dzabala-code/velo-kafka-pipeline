import time
from kafka import KafkaConsumer
from kafka import KafkaProducer
import json

topic_name = "velib-stations"
consumer = KafkaConsumer(topic_name, bootstrap_servers="localhost:9092", group_id="archive-data")

with open("archive-velib.txt", mode="a", encoding="utf-8") as fichier:
    for message in consumer:
        message_texte = message.value.decode('utf-8')
        fichier.write(message_texte + "\n")
        fichier.flush()
