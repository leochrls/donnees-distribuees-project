# infrastructure/scripts/producer.py

import json
import time
import random
from kafka import KafkaProducer

# --- Configuration ---
KAFKA_BROKER_URL = 'localhost:9092'
KAFKA_TOPIC_NAME = 'iot_sensor_data'
SENSOR_IDS = ['sensor-alpha-01', 'sensor-beta-02', 'sensor-gamma-03', 'sensor-delta-04', 'sensor-epsilon-05']

# Producteur Kafka
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER_URL,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

print("Le producteur Kafka est prêt. Envoi des données...")

# --- Boucle envoie des données ---
while True:
    try:
        sensor_id = random.choice(SENSOR_IDS)
        temperature = round(random.uniform(20.0, 30.0) + random.choice([-5, 0, 5, 10]) * random.random(), 2)
        humidity = round(random.uniform(40.0, 60.0), 2)
        timestamp = time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())

        message = {
            'sensor_id': sensor_id,
            'temperature': temperature,
            'humidity': humidity,
            'timestamp': timestamp
        }

        # Envoi du message au topic KAFKA
        producer.send(KAFKA_TOPIC_NAME, message)
        print(f"Message envoyé: {message}")

        # Délai envoie du prochain message
        time.sleep(2)
    
    except Exception as e:
        print(f"Une erreur est survenue: {e}")
        time.sleep(5) # Attendre avant de réessayer