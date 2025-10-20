# infrastructure/scripts/historical_data_generator.py

import csv
import random
import numpy as np
from datetime import datetime, timedelta

print("Génération du jeu de données d'entraînement historique pour 5 capteurs...")

# --- Configuration Générale ---
SENSOR_IDS = [
    'sensor-alpha-01', 
    'sensor-beta-02', 
    'sensor-gamma-03',
    'sensor-delta-04',
    'sensor-epsilon-05'
]
START_DATE = datetime(2025, 7, 1)
DAYS_TO_SIMULATE = 90
POINTS_PER_HOUR = 60
TOTAL_POINTS_PER_SENSOR = DAYS_TO_SIMULATE * 24 * POINTS_PER_HOUR

# --- Fonction pour générer les données d'un seul capteur ---
#     Chaque capteur aura une température de base et des anomalies légèrement différentes.
def generate_sensor_data(sensor_id, base_temp, base_humidity):
    print(f"  Génération des données pour {sensor_id}...")
    
    # Comportement Normal (avec cycles journaliers)
    time_points = np.linspace(0, DAYS_TO_SIMULATE * 2 * np.pi, TOTAL_POINTS_PER_SENSOR)
    # Le comportement de base est le même (cycle jour/nuit), mais la température moyenne est différente
    normal_temperature = base_temp + 8 * np.sin(time_points) + np.random.normal(0, 0.5, TOTAL_POINTS_PER_SENSOR)
    normal_humidity = base_humidity - 15 * np.sin(time_points) + np.random.normal(0, 1, TOTAL_POINTS_PER_SENSOR)

    timestamps = [START_DATE + timedelta(minutes=i) for i in range(TOTAL_POINTS_PER_SENSOR)]
    sensor_data_rows = []

    i = 0
    while i < TOTAL_POINTS_PER_SENSOR:
        temp = normal_temperature[i]
        hum = normal_humidity[i]
        ts = timestamps[i]
        is_anomaly = 0

        # Injection d'Anomalies (chaque capteur a des probabilités différentes)
        # 1. Pics de température soudains
        if random.random() < (0.001 * random.uniform(0.5, 1.5)): # Probabilité légèrement variable
            temp += random.uniform(15, 25)
            is_anomaly = 1
        
        # 2. Capteur bloqué
        if random.random() < (0.0005 * random.uniform(0.5, 1.5)):
            stuck_value = temp
            duration = random.randint(15, 30) # Durée de blocage variable
            for j in range(duration):
                if (i + j) < TOTAL_POINTS_PER_SENSOR:
                    sensor_data_rows.append([
                        timestamps[i+j].strftime('%Y-%m-%dT%H:%M:%SZ'),
                        sensor_id,
                        stuck_value,
                        round(normal_humidity[i+j], 2),
                        1 # C'est une anomalie
                    ])
            i += duration -1 # On saute les points qu'on vient d'ajouter
            i += 1
            continue
            
        sensor_data_rows.append([
            ts.strftime('%Y-%m-%dT%H:%M:%SZ'),
            sensor_id,
            round(temp, 2),
            round(hum, 2),
            is_anomaly
        ])
        i += 1
        
    return sensor_data_rows

# --- Boucle principale pour générer les données de tous les capteurs ---
all_data_rows = []
sensor_configs = {
    'sensor-alpha-01': {'base_temp': 22, 'base_humidity': 50},
    'sensor-beta-02': {'base_temp': 25, 'base_humidity': 55}, # Un peu plus chaud et humide
    'sensor-gamma-03': {'base_temp': 18, 'base_humidity': 45}, # Un peu plus froid et sec
    'sensor-delta-04': {'base_temp': 22, 'base_humidity': 50},
    'sensor-epsilon-05': {'base_temp': 28, 'base_humidity': 60} # Nettement plus chaud et humide
}

for sensor_id in SENSOR_IDS:
    config = sensor_configs[sensor_id]
    sensor_data = generate_sensor_data(sensor_id, config['base_temp'], config['base_humidity'])
    all_data_rows.extend(sensor_data)

# Mélanger les données pour que les lignes des différents capteurs ne se suivent pas
print("Mélange des données...")
random.shuffle(all_data_rows)

# Trier les données par timestamp pour avoir un ordre chronologique final
print("Tri final par date...")
all_data_rows.sort(key=lambda x: x[0])

# --- Écriture dans un fichier CSV ---
output_file = 'historical_sensor_data.csv'
with open(output_file, 'w', newline='') as f:
    writer = csv.writer(f)
    writer.writerow(['timestamp', 'sensor_id', 'temperature', 'humidity', 'is_anomaly'])
    writer.writerows(all_data_rows)

print(f"\nTerminé. {len(all_data_rows)} lignes de données pour {len(SENSOR_IDS)} capteurs ont été générées dans '{output_file}'.")