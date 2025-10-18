# infrastructure/spark/app/streaming_processor.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, FloatType
from influxdb_client import InfluxDBClient, Point, WriteOptions

# --- Configuration ---
KAFKA_BROKER_URL = 'kafka:9093'
KAFKA_TOPIC_NAME = 'iot_sensor_data'

# --- Configuration des Bases de Données ---
# InfluxDB
INFLUXDB_URL = "http://influxdb:8086"
INFLUXDB_TOKEN = "jIWlKAmWBp6S7BBe0mDV70e9A24yj7iT5PMtZvvpdWUgn4MvdZq2fsxWPpCWZ7c1ZNXyeSbAOGdN2bn9mFokIg=="  
INFLUXDB_ORG = "iot_project"
INFLUXDB_BUCKET_RAW = "raw_data"

# MongoDB
MONGO_URI = "mongodb://root:root@mongodb:27017/" # IMPORTANT: Mettez votre mot de passe

# Fonction pour traiter chaque micro-batch
def process_batch(df, epoch_id):
    # Ne rien faire si le DataFrame est vide
    if df.count() == 0:
        return

    # Mettre en cache le DataFrame car nous allons l'utiliser deux fois
    df.cache()
    print(f"--- Processing Batch #{epoch_id}, Records: {df.count()} ---")
    df.show(5)

    # --- Écriture dans InfluxDB (Données brutes) ---
    try:
        # Collecter les données pour les envoyer au client InfluxDB
        # Pour de gros volumes, une approche plus distribuée serait nécessaire
        data_to_write = df.collect()

        with InfluxDBClient(url=INFLUXDB_URL, token=INFLUXDB_TOKEN, org=INFLUXDB_ORG) as client:
            write_api = client.write_api(write_options=WriteOptions(batch_size=500))
            
            points = []
            for row in data_to_write:
                point = Point("sensor_measurement") \
                    .tag("sensor_id", row["sensor_id"]) \
                    .field("temperature", row["temperature"]) \
                    .field("humidity", row["humidity"]) \
                    .time(row["timestamp"])
                points.append(point)
            
            write_api.write(bucket=INFLUXDB_BUCKET_RAW, record=points)
            print(f"Successfully wrote {len(points)} points to InfluxDB.")

    except Exception as e:
        print(f"An error occurred while writing to InfluxDB: {e}")

    # --- Écriture dans MongoDB (Données traitées/enrichies) ---
    # Pour l'instant, nous écrivons les mêmes données. Plus tard, vous ajouterez l'enrichissement.
    try:
        df.write \
            .format("mongodb") \
            .mode("append") \
            .option("uri", MONGO_URI) \
            .option("database", "iot_project_db") \
            .option("collection", "processed_data") \
            .save()
        print("Successfully wrote data to MongoDB.")
    except Exception as e:
        print(f"An error occurred while writing to MongoDB: {e}")

    # Retirer le DataFrame du cache
    df.unpersist()


# --- Initialisation de la session Spark ---
spark = SparkSession.builder \
    .appName("IoTStreamProcessor") \
    .config("spark.mongodb.write.connection.uri", MONGO_URI) \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# --- Lecture du stream Kafka et Transformations ---
df_kafka = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER_URL) \
    .option("subscribe", KAFKA_TOPIC_NAME) \
    .option("startingOffsets", "latest") \
    .load()

json_schema = StructType([
    StructField("sensor_id", StringType(), True),
    StructField("temperature", FloatType(), True),
    StructField("humidity", FloatType(), True),
    StructField("timestamp", StringType(), True)
])

df_structured = df_kafka.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), json_schema).alias("data")) \
    .select("data.*")

# --- Lancement du Stream avec `foreachBatch` ---
query = df_structured.writeStream \
    .foreachBatch(process_batch) \
    .outputMode("append") \
    .start()

query.awaitTermination()