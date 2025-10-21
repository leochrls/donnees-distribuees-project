# infrastructure/spark/app/streaming_processor.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, FloatType
from pyspark.ml import PipelineModel  
from influxdb_client import InfluxDBClient, Point, WriteOptions

# --- Configuration ---
KAFKA_BROKER_URL = 'kafka:9093'
KAFKA_TOPIC_NAME = 'iot_sensor_data'

# --- Configuration des Bases de Données ---
INFLUXDB_URL = "http://influxdb:8086"
INFLUXDB_TOKEN = "jIWlKAmWBp6S7BBe0mDV70e9A24yj7iT5PMtZvvpdWUgn4MvdZq2fsxWPpCWZ7c1ZNXyeSbAOGdN2bn9mFokIg=="  
INFLUXDB_ORG = "iot_project"
INFLUXDB_BUCKET_RAW = "raw_data"
MONGO_URI = "mongodb://root:root@mongodb:27017/"

# Chargement du modèle de Machine Learning depuis HDFS 
model_path = "hdfs://namenode:9000/models/anomaly_detection_rf"
print(f"Chargement du modèle de détection d'anomalies depuis {model_path}...")
try:
    anomaly_model = PipelineModel.load(model_path)
    print("Modèle chargé avec succès.")
except Exception as e:
    print(f"ERREUR: Impossible de charger le modèle. Assurez-vous qu'il a bien été entraîné et sauvegardé. Erreur: {e}")
    exit()

# Fonction pour traiter chaque micro-batch
def process_batch(df, epoch_id):
    if df.count() == 0:
        return

    print(f"--- Processing Batch #{epoch_id}, Records: {df.count()} ---")

    # Appliquer le modèle pour faire des prédictions 
    # Le modèle va ajouter plusieurs colonnes, dont une "prediction" (0.0 pour normal, 1.0 pour anomalie)
    predictions = anomaly_model.transform(df)
    
    # On sélectionne les colonnes utiles pour la sauvegarde
    output_df = predictions.select(
        col("timestamp"),
        col("sensor_id"),
        col("temperature"),
        col("humidity"),
        col("prediction").cast("integer").alias("is_anomaly_predicted") # On renomme et on convertit en entier
    )
    
    output_df.cache() # On met en cache pour réutilisation
    
    print("Données après prédiction :")
    output_df.show(10)

    # --- Écriture dans InfluxDB (Données brutes) ---
    # Cette partie ne change pas, on veut toujours visualiser toutes les données brutes.
    try:
        data_to_write_influx = output_df.collect()
        with InfluxDBClient(url=INFLUXDB_URL, token=INFLUXDB_TOKEN, org=INFLUXDB_ORG) as client:
            write_api = client.write_api(write_options=WriteOptions(batch_size=500))
            points = [
                Point("sensor_measurement")
                .tag("sensor_id", row["sensor_id"])
                .field("temperature", row["temperature"])
                .field("humidity", row["humidity"])
                .time(row["timestamp"])
                for row in data_to_write_influx
            ]
            write_api.write(bucket=INFLUXDB_BUCKET_RAW, record=points)
            print(f"Successfully wrote {len(points)} points to InfluxDB.")
    except Exception as e:
        print(f"An error occurred while writing to InfluxDB: {e}")

    
    # Écrire TOUTES les données traitées (avec leur prédiction) dans une collection
    try:
        output_df.write \
            .format("mongodb") \
            .mode("append") \
            .option("connection.uri", MONGO_URI) \
            .option("database", "iot_project_db") \
            .option("collection", "processed_data") \
            .save()
        print("Successfully wrote processed data to MongoDB.")
    except Exception as e:
        print(f"An error occurred while writing to MongoDB (processed_data): {e}")

    # 2. Filtrer pour ne garder que les anomalies et les écrire dans une collection dédiée
    df_anomalies = output_df.filter(col("is_anomaly_predicted") == 1)
    
    if df_anomalies.count() > 0:
        print(f"!!! {df_anomalies.count()} ANOMALIES DÉTECTÉES ET SAUVEGARDÉES !!!")
        try:
            df_anomalies.write \
                .format("mongodb") \
                .mode("append") \
                .option("connection.uri", MONGO_URI) \
                .option("database", "iot_project_db") \
                .option("collection", "anomalies") \
                .save()
            print("Successfully wrote anomalies to MongoDB.")
        except Exception as e:
            print(f"An error occurred while writing to MongoDB (anomalies): {e}")

    output_df.unpersist()


# --- Initialisation Spark, lecture Kafka et lancement du Stream ---
# (Cette partie reste identique à avant)

spark = SparkSession.builder \
    .appName("IoTStreamProcessorWithML") \
    .config("spark.mongodb.write.connection.uri", MONGO_URI) \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

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

query = df_structured.writeStream \
    .foreachBatch(process_batch) \
    .outputMode("append") \
    .start()

query.awaitTermination()