from pyspark.sql import SparkSession
from joblib import load
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
from pyspark.sql.functions import col, window, round,from_json
from pyspark.sql.functions import udf
from pyspark.sql.types import DoubleType
import numpy as np
from pyspark.sql.types import ArrayType, DoubleType
from pyspark.sql.functions import pandas_udf, PandasUDFType
from pyspark.sql.types import StructType, StructField, DoubleType
import pandas as pd
from pyspark.sql.types import StructType, StructField, DoubleType


# List of packages
spark_dependencies = ["org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2",
                      "org.postgresql:postgresql:42.2.23"]  # PostgreSQL JDBC driver]


# Chemin vers le pilote PostgreSQL
postgres_jar_path = "/Users/surelmanda/.ivy2/cache/org.postgresql/postgresql/jars/postgresql-42.2.23.jar"

# Configurations Spark
spark_configs = {
    "spark.jars.packages": ",".join(spark_dependencies),  # Ajouter les dépendances Spark
    "spark.jars": postgres_jar_path,  # Chemin vers le pilote PostgreSQL
    "spark.driver.extraClassPath": postgres_jar_path,  # Chemin supplémentaire pour le pilote PostgreSQL sur le driver
    "spark.executor.extraClassPath": postgres_jar_path,  # Chemin supplémentaire pour le pilote PostgreSQL sur les exécuteurs
    "spark.sql.adaptive.enabled": "false",  # Désactiver l'exécution de requêtes adaptative
}

# Initialiser la session Spark avec les dépendances et configurations
spark = (
    SparkSession.builder
    .appName("KafkaStreaming")
    .config("spark.jars.packages", spark_configs["spark.jars.packages"])  # Ajouter les dépendances Spark
    .config("spark.jars", spark_configs["spark.jars"])  # Chemin vers le pilote PostgreSQL
    .config("spark.driver.extraClassPath", spark_configs["spark.driver.extraClassPath"])  # Chemin supplémentaire pour le pilote PostgreSQL sur le driver
    .config("spark.executor.extraClassPath", spark_configs["spark.executor.extraClassPath"])  # Chemin supplémentaire pour le pilote PostgreSQL sur les exécuteurs
    .config("spark.sql.adaptive.enabled", spark_configs["spark.sql.adaptive.enabled"])  # Désactiver l'exécution de requêtes adaptative
    .getOrCreate()
)


# Définir le schéma des données à recupérer sur mon topic Kafka
# Définir le schéma des données à récupérer sur mon topic Kafka
kafka_schema = StructType([
    StructField("ambient_temperature", DoubleType(), True),
    StructField("ambient_pressure", DoubleType(), True),
    StructField("ambient_humidity", DoubleType(), True),
    StructField("air_filter_diff_pressure", DoubleType(), True),
    StructField("exhaust_pressure", DoubleType(), True),
    StructField("inlet_temperature", DoubleType(), True),
    StructField("after_temperature", DoubleType(), True),
    StructField("discharge_pressure", DoubleType(), True),
    StructField("energy_yield", DoubleType(), True),
    StructField("carbon_monoxide", DoubleType(), True),
    StructField("nitrogen_oxides", DoubleType(), True),
    StructField("timestamp", TimestampType(), True)
])

# Définir les colonnes nécessaires pour la prédiction
features_columns = ['AT', 'AP', 'AH', 'AFDP', 'GTEP', 'TIT', 'TAT', 'TEY', 'CDP', 'CO']

# Définir le schéma pour les fonctionnalités (features)
features_schema = StructType([StructField(feature, DoubleType(), True) for feature in features_columns])

# Chemin vers votre modèle
model_path = "/Users/surelmanda/Downloads/AirGUARD/Gas_Turbine_Streaming/Models/modele_entrene.joblib"

# Charger le modèle de régression
regression_model = load(model_path)

# Function to rename columns and preprocess data before prediction
# Function to rename columns and preprocess data before prediction
def preprocess_data(batch_df):
    # Renaming columns as per your specified logic
    batch_df = batch_df \
        .withColumnRenamed("ambient_temperature", "AT") \
        .withColumnRenamed("ambient_pressure", "AP") \
        .withColumnRenamed("ambient_humidity", "AH") \
        .withColumnRenamed("air_filter_diff_pressure", "AFDP") \
        .withColumnRenamed("exhaust_pressure", "GTEP") \
        .withColumnRenamed("inlet_temperature", "TIT") \
        .withColumnRenamed("after_temperature", "TAT") \
        .withColumnRenamed("discharge_pressure", "TEY") \
        .withColumnRenamed("energy_yield", "CDP") \
        .withColumnRenamed("carbon_monoxide", "CO") \
        .withColumnRenamed("nitrogen_oxides", "nitro") \
        .withColumnRenamed("timestamp", "timst")

    # Select only the necessary columns for prediction
    features_columns = ['AT', 'AP', 'AH', 'AFDP', 'GTEP', 'TIT', 'TAT', 'TEY', 'CDP', 'CO', 'nitro','timst']
    selected_features = batch_df.select(*features_columns)

    # Convert the selected features DataFrame to a Pandas DataFrame
    pandas_df = selected_features.toPandas()

    # Extract 'nitro' column values as a list
    nitro_values = pandas_df['nitro'].tolist()

    # Extract 'timst' column values as a list
    timst_values = pandas_df['timst'].tolist()

    # Drop the 'nitro' column from the Pandas DataFrame
    pandas_df = pandas_df.drop(columns=['nitro','timst'])

    return nitro_values, timst_values, pandas_df


# Function to predict using the loaded machine learning model
def predict_and_add_columns(batch_df, epoch_id):
    # Preprocess the data, rename columns, and select necessary features
    nitro_values,timst_values, processed_data = preprocess_data(batch_df)

    # Check if there are records to predict
    if not processed_data.empty:
        # Load the pre-trained machine learning model
        regression_model = load(model_path)

        # Make predictions on the selected features
        predictions = regression_model.predict(processed_data)
 
        # Add the 'nitro' column to the Pandas DataFrame
        processed_data['actual_nitro'] = nitro_values

        # Add the 'predictions' column to the Pandas DataFrame
        processed_data['predict_nitro'] = predictions.round(3).tolist()

        # Add the 'nitro' column to the Pandas DataFrame
        processed_data['timestamp'] = timst_values

        # Select only the specified three columns for visualization
        result_df = processed_data[['actual_nitro', 'predict_nitro','timestamp']]

        # Convert the Pandas DataFrame back to a Spark DataFrame
        result_df_spark = spark.createDataFrame(result_df)

        # Write the result DataFrame to the console or any other sink as needed
        result_df_spark.show()
    else:
        print("No records to predict.")


# Définir les options de configuration pour Kafka
kafka_params = {
    "bootstrap.servers": "localhost:9092",  # Correction ici
    "group.id": "read_consumer_groupID",
    "auto.offset.reset": "earliest",
    "name_topic": ["Gas_Turbine_topic"]

}

# Lire les données Kafka en streaming
raw_stream = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_params["bootstrap.servers"]) \
    .option("subscribe", ",".join(kafka_params["name_topic"])) \
    .load()

# Convertir les données JSON du message en colonnes structurées
parsed_data = raw_stream.selectExpr("CAST(value AS STRING)").select(from_json("value", kafka_schema).alias("data"))

# Sélectionner les colonnes pertinentes
donnees_selectionnees = parsed_data.select("data.ambient_temperature", "data.ambient_pressure", "data.ambient_humidity",\
                                            "data.air_filter_diff_pressure", "data.exhaust_pressure", "data.inlet_temperature",\
                                            "data.after_temperature", "data.discharge_pressure", "data.energy_yield",\
                                            "data.carbon_monoxide", "data.nitrogen_oxides", "data.timestamp")

# Renommer les colonnes avec de nouveaux noms de 5 caractères
donnees_renommees = donnees_selectionnees \
    .withColumnRenamed("data.ambient_temperature", "AT") \
    .withColumnRenamed("data.ambient_pressure", "AP") \
    .withColumnRenamed("data.ambient_humidity", "AH") \
    .withColumnRenamed("data.air_filter_diff_pressure", "AFDP") \
    .withColumnRenamed("data.exhaust_pressure", "GTEP") \
    .withColumnRenamed("data.inlet_temperature", "TIT") \
    .withColumnRenamed("data.after_temperature", "TAT") \
    .withColumnRenamed("data.discharge_pressure", "TEY") \
    .withColumnRenamed("data.energy_yield", "CDP") \
    .withColumnRenamed("data.carbon_monoxide", "CO") \
    .withColumnRenamed("data.nitrogen_oxides", "nitro") \
    .withColumnRenamed("data.timestamp", "timst")



# Afficher les données en streaming sans utiliser un filtre sur les colonnes null:
# query = donnees_renommees.writeStream.outputMode("append").format("console").start()

# Appliquer la fonction sur chaque micro-batch
query = donnees_renommees \
    .writeStream \
    .outputMode("update") \
    .foreachBatch(predict_and_add_columns) \
    .start()

query.awaitTermination()