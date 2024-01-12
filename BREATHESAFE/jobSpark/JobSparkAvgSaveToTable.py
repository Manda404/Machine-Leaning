from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
from pyspark.sql.functions import col, window,avg, max, count, round, regexp_replace,from_json

# Créer une session Spark
# Inclure la dépendance Kafka
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


# Définir le schéma des données
schema = StructType([
    StructField("no2", DoubleType(), True),
    StructField("o3", DoubleType(), True),
    StructField("pm10", DoubleType(), True),
    StructField("so2", DoubleType(), True),
    StructField("time", TimestampType(), True)

])

# Définir les options de configuration pour Kafka
kafka_params = {
    "bootstrap.servers": "localhost:9092",  # Correction ici
    "group.id": "read_consumer_groupID",
    "auto.offset.reset": "earliest"
}

topics = ["Values_topic"]

# Lire les données Kafka en streaming
raw_stream = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_params["bootstrap.servers"]) \
    .option("subscribe", ",".join(topics)) \
    .load()

# Convertir les données JSON du message en colonnes structurées
parsed_data = raw_stream.selectExpr("CAST(value AS STRING)").select(from_json("value", schema).alias("data"))

# Sélectionner les colonnes pertinentes
selected_data = parsed_data.select("data.no2", "data.o3", "data.pm10", "data.so2","data.time") # "data.time"

# Afficher les données en streaming sans utiliser un filtre sur les colonnes null:
# query = selected_data.writeStream.outputMode("append").format("console").start()

# Attendre la fin du streaming
# query.awaitTermination()

# Filtrer les lignes contenant des valeurs non nulles
filtered_data = selected_data.filter(
    col("no2").isNotNull() &
    col("o3").isNotNull() &
    col("pm10").isNotNull() &
    col("so2").isNotNull()
)


#window(col("time") :
#window est utilisé pour créer des fenêtres temporelles distinctes dans lesquelles les données peuvent être agrégées.
# Ajouter une colonne de fenêtre temporelle d'une minute
windowed_data = filtered_data.withColumn("time", window(col("time"), "2 minute")) # th {2024-01-09 05:04 ....


# Agréger les données dans la fenêtre temporelle et arrondir les valeurs
aggregated_data = windowed_data.groupBy("time").agg(
    round(avg(col("pm10")), 3).alias("avg_pm10"),
    round(avg(col("no2")), 3).alias("avg_no2"),
    round(avg(col("o3")), 3).alias("avg_o3"),
    round(avg(col("so2")), 3).alias("avg_so2"),
    max(col("time")).alias("max_time"),
    count("time").alias("valid_values") 
)

# Sélectionner les colonnes pertinentes
aggregated_data_select = aggregated_data.select("avg_no2", "avg_o3", "avg_so2", "avg_pm10","valid_values") 

# Afficher les données agrégées en streaming en utilisant comme mode update
aggregated_data_select.writeStream.outputMode("update").format("console").start()


# Configuration pour la connexion à PostgreSQL
# Définition des propriétés de connexion à la base de données PostgreSQL
properties = {
    # Nom d'utilisateur pour la connexion à la base de données
    "user": "surelmanda",
    
    # Mot de passe associé à l'utilisateur pour la connexion à la base de données
    "password": "postgres",
    
    # Pilote JDBC utilisé pour la connexion à la base de données PostgreSQL
    "driver": "org.postgresql.Driver",
    
    # URL de connexion à la base de données PostgreSQL
    # jdbc:postgresql://localhost:5432/DataBase_Polluants
    # localhost:5432 est l'adresse du serveur PostgreSQL et DataBase_Polluants est le nom de la base de données
    "url": "jdbc:postgresql://localhost:5432/DataBase_Polluants",
    
    # Version du pilote JDBC PostgreSQL utilisé
    "version": "42.2.23",
    
    # Nom de la table dans la base de données où les données seront stockées
    "table_name": "aggregated_data",
}


# j'écris le DataFrame aggregated_data_select dans la table PostgreSQL
aggregated_data_select \
    .writeStream \
    .outputMode("update") \
    .foreachBatch(lambda batch_df, batch_id: batch_df.write.jdbc(
        url=properties["url"],
        table=properties["table_name"],
        mode="append",  # Utilisez "append" pour ajouter les données existantes
        properties={"user": properties["user"], "password": properties["password"], "driver": properties["driver"]}
    )) \
    .start() \
    .awaitTermination() # Attendre que le streaming soit terminé