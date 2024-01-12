from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, TimestampType, DoubleType, StringType, IntegerType
from pyspark.sql.functions import col, regexp_replace, split, to_timestamp, from_json
from pyspark.sql.streaming import DataStreamReader
from pyspark.sql.functions import sum as make_sum
from pyspark.sql.functions import window


# Topic Kafka où lire les données
topics = ["topic_des_votants"]

# Définir les paramètres Kafka
kafka_params = {
    "bootstrap.servers": "localhost:9092",  # Correction: Utilisez localhost au lieu de localhosts
    "group.id": "les_votants_group",
    "auto.offset.reset": "earliest"
}

# Définir le schéma des données du topic Kafka
vote_schema = StructType([
      # Définir le schéma pour les données du vote
        StructField("voter_id", StringType(), True),
        StructField("candidate_id", StringType(), True),
        StructField("voting_time", TimestampType(), True),
        StructField("voter_name", StringType(), True),
        StructField("party_affiliation", StringType(), True),
        StructField("biography", StringType(), True),
        StructField("campaign_platform", StringType(), True),
        StructField("photo_url", StringType(), True),
        StructField("candidate_name", StringType(), True),
        StructField("date_of_birth", StringType(), True),
        StructField("gender", StringType(), True),
        StructField("nationality", StringType(), True),
        StructField("registration_number", StringType(), True),
        StructField("address", StructType([
            StructField("street", StringType(), True),
            StructField("city", StringType(), True),
            StructField("state", StringType(), True),
            StructField("country", StringType(), True),
            StructField("postcode", StringType(), True)
        ]), True),
        StructField("email", StringType(), True),
        StructField("phone_number", StringType(), True),
        StructField("cell_number", StringType(), True),
        StructField("picture", StringType(), True),
        StructField("registered_age", IntegerType(), True),
        StructField("vote", IntegerType(), True)
    ])

# Créer une session Spark avec des configurations spécifiques
spark = (
    SparkSession.builder
    .appName("AnalyseElectionRealTime")  # Nom de l'application Spark
    .master("local[*]")  # Utiliser l'exécution Spark locale avec tous les cœurs disponibles
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2")  # Intégration Spark-Kafka
    #.config("spark.jars", "org.postgresql:postgresql:42.2.23") # Pilote PostgreSQL (commenté pour le moment)
    .config("spark.jars", 
            "/Users/surelmanda/.ivy2/cache/org.postgresql/postgresql/jars/postgresql-42.2.23.jar")  # Chemin vers le pilote PostgreSQL
    .config("spark.driver.extraClassPath", 
            "/Users/surelmanda/.ivy2/cache/org.postgresql/postgresql/jars/postgresql-42.2.23.jar")  # Chemin supplémentaire pour le pilote PostgreSQL sur le driver
    .config("spark.executor.extraClassPath", 
            "/Users/surelmanda/.ivy2/cache/org.postgresql/postgresql/jars/postgresql-42.2.23.jar")  # Chemin supplémentaire pour le pilote PostgreSQL sur les exécuteurs
    .config("spark.sql.adaptive.enabled", "false")  # Désactiver l'exécution de requêtes adaptative
    .getOrCreate()  # Obtenir ou créer une session Spark
)

# À ce stade, la session Spark a été créée avec les configurations spécifiées.
# Vous pouvez ajouter votre code Spark ici.

# Lire les données Kafka en streaming et les traiter
votes_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_params["bootstrap.servers"]) \
    .option("subscribe", ",".join(topics)) \
    .option("startingOffsets", kafka_params["auto.offset.reset"]) \
    .load()

# Convertir les données en DataFrame
data_stream = votes_df \
    .selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), vote_schema).alias("data")) \
    .select("data.*")

# Prétraitement des données : conversion de types et ajout d'une fenêtre temporelle
votes_df = data_stream.withColumn("voting_time", col("voting_time").cast(TimestampType())) \
                      .withColumn('vote', col('vote').cast(IntegerType()))

# La fonction withWatermark est utilisée dans Apache Spark Structured Streaming 
# pour définir une "marque temporelle" sur une colonne de type temporel (généralement un horodatage). 
# j'utilise la fonction withWatermark pour spécifier une fenêtre temporelle d'une minute à partir de la colonne "voting_time
enriched_votes_df = votes_df.withWatermark("voting_time", "1 minute")

# Agrégation des votes par candidat et participation par emplacement:
votes_per_candidate = enriched_votes_df.groupBy(
    "candidate_id", "candidate_name", "party_affiliation",
    "photo_url", window("voting_time", "1 minute")
).agg(make_sum("vote").alias("total_votes"))

# Sélectionnez uniquement les colonnes nécessaires
votes_per_candidate = votes_per_candidate.select(
    "candidate_id", "candidate_name", "party_affiliation",
    "photo_url", "total_votes",
    col("window.start").alias("window_start"),
    col("window.end").alias("window_end")
)

# Configuration pour la connexion à PostgreSQL
jdbc_url = "jdbc:postgresql://localhost:5432/DB_Election_Ece"
table_name = "staging"
properties = {
    "user": "surelmanda",
    "password": "postgres",
    "driver": "org.postgresql.Driver",
}

# Écriture du DataFrame résultant dans la table PostgreSQL
query = (
    votes_per_candidate.writeStream
    .outputMode("complete")
    .foreachBatch(lambda batch_df, batch_id: batch_df.write.jdbc(
        url=jdbc_url,
        table=table_name,
        mode="append",  # Utilisez "append" pour ajouter les données existantes
        properties=properties
    ))
    .start()
)

# Attendre que le streaming soit terminé
query.awaitTermination()