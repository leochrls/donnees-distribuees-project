# infrastructure/spark/app/train_anomaly_model.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType, TimestampType
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

print("Démarrage du script d'entraînement du modèle de détection d'anomalies...")

# --- Initialisation de la session Spark ---
spark = SparkSession.builder \
    .appName("AnomalyDetectionModelTraining") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# --- Définition du schéma et chargement des données depuis HDFS ---
schema = StructType([
    StructField("timestamp", StringType(), True),
    StructField("sensor_id", StringType(), True),
    StructField("temperature", FloatType(), True),
    StructField("humidity", FloatType(), True),
    StructField("is_anomaly", IntegerType(), True) # Notre "label"
])

hdfs_path = "hdfs://namenode:9000/datasets/iot_training/historical_sensor_data.csv"

print(f"Chargement des données depuis HDFS: {hdfs_path}")
df = spark.read.csv(hdfs_path, header=True, schema=schema)
df.printSchema()
print(f"Nombre total de lignes chargées: {df.count()}")
df.show(5)

# --- Préparation des données pour le Machine Learning (Feature Engineering) ---
print("Préparation des données (Feature Engineering)...")

# 1. Sélectionner les colonnes utiles pour la prédiction (nos "features")
# On va utiliser la température et l'humidité pour prédire si c'est une anomalie.
feature_columns = ["temperature", "humidity"]

# 2. VectorAssembler : Spark MLlib a besoin que toutes les features soient dans une seule colonne de type "vecteur".
assembler = VectorAssembler(inputCols=feature_columns, outputCol="features_unscaled")

# 3. StandardScaler : Met les features à la même échelle (très important pour beaucoup de modèles ML).
scaler = StandardScaler(inputCol="features_unscaled", outputCol="features", withStd=True, withMean=True)

# 4. Définir le modèle : un classificateur Random Forest.
# `labelCol` est la colonne que l'on veut prédire.
# `featuresCol` est la colonne contenant nos features préparées.
rf = RandomForestClassifier(labelCol="is_anomaly", featuresCol="features", numTrees=100, maxDepth=10, seed=42)

# 5. Créer un Pipeline : enchaîne toutes les étapes de préparation et le modèle.
pipeline = Pipeline(stages=[assembler, scaler, rf])

# --- Division des données : Entraînement et Test ---
print("Division des données en set d'entraînement et de test...")
(training_data, test_data) = df.randomSplit([0.8, 0.2], seed=42)
print(f"Taille du set d'entraînement: {training_data.count()}")
print(f"Taille du set de test: {test_data.count()}")

# --- Entraînement du modèle ---
print("Entraînement du modèle Random Forest...")
model = pipeline.fit(training_data)
print("Entraînement terminé.")

# --- Évaluation du modèle ---
print("Évaluation du modèle sur le set de test...")
predictions = model.transform(test_data)

evaluator_accuracy = MulticlassClassificationEvaluator(labelCol="is_anomaly", predictionCol="prediction", metricName="accuracy")
accuracy = evaluator_accuracy.evaluate(predictions)

evaluator_f1 = MulticlassClassificationEvaluator(labelCol="is_anomaly", predictionCol="prediction", metricName="f1")
f1_score = evaluator_f1.evaluate(predictions)

print(f"  Accuracy du modèle: {accuracy:.2%}")
print(f"  F1-Score du modèle: {f1_score:.2%}") # Le F1-score est souvent plus pertinent pour les données déséquilibrées.

# --- Sauvegarde du modèle entraîné sur HDFS ---
model_path = "hdfs://namenode:9000/models/anomaly_detection_rf"
print(f"Sauvegarde du modèle entraîné sur HDFS à l'emplacement: {model_path}")
model.write().overwrite().save(model_path)
print("Modèle sauvegardé avec succès.")

spark.stop()