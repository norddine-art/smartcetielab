import json
import time
import random
from confluent_kafka import Producer
producer = Producer({'bootstrap.servers': '127.0.0.1:9092'})
topic = 'serre_telemetry'
print("Le capleur vertuelle est active et demarre l'envoie des donnees de telemetrie ...")

while True:
    data = {
        'temperature': round(random.uniform(15.0, 40.0), 2),
        'humidite': round(random.uniform(20.0, 80.0), 2)
    }
    producer.produce(topic, json.dumps(data).encode('utf-8'))
    producer.flush()
    print(f"📡 Données envoyées -> Température: {data['temperature']}°C | Humidité: {data['humidite']}%")
    time.sleep(1)