# Configuration du job Spark
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date
from pyspark.sql.functions import from_json, col, avg
from pyspark.sql.functions import split, col
from pyspark.sql.types import StructType, StringType, DoubleType
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

# Initialisez la session Spark avec la dépendance Kafka
# List of packages including the PostgreSQL JDBC driver ( Dépendances Spark et PostgreSQL )
spark_dependencies = [
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2",
    "org.postgresql:postgresql:42.2.23",  # PostgreSQL JDBC driver
]

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
    .appName("Processsing_resultats_t2")
    .config("spark.jars.packages", spark_configs["spark.jars.packages"])  # Ajouter les dépendances Spark
    .config("spark.jars", spark_configs["spark.jars"])  # Chemin vers le pilote PostgreSQL
    .config("spark.driver.extraClassPath", spark_configs["spark.driver.extraClassPath"])  # Chemin supplémentaire pour le pilote PostgreSQL sur le driver
    .config("spark.executor.extraClassPath", spark_configs["spark.executor.extraClassPath"])  # Chemin supplémentaire pour le pilote PostgreSQL sur les exécuteurs
    .config("spark.sql.adaptive.enabled", spark_configs["spark.sql.adaptive.enabled"])  # Désactiver l'exécution de requêtes adaptative
    .getOrCreate()
)

# Masquer les avertissements
spark.sparkContext.setLogLevel("ERROR")

# Paramètres Kafka
kafka_bootstrap_servers = 'localhost:9092'
kafka_topic = 'Topic_Resultat_TourT2'

# Define the schema for the DataFrame
schema = StructType([
    StructField("Code du département", StringType(), True),
    StructField("Libellé du département", StringType(), True),
    StructField("Etat saisie", StringType(), True),
    StructField("Inscrits", IntegerType(), True),
    StructField("Abstentions", IntegerType(), True),
    StructField("% Abs/Ins", DoubleType(), True),
    StructField("Votants", IntegerType(), True),
    StructField("% Vot/Ins", DoubleType(), True),
    StructField("Blancs", IntegerType(), True),
    StructField("% Blancs/Ins", DoubleType(), True),
    StructField("% Blancs/Vot", DoubleType(), True),
    StructField("Nuls", IntegerType(), True),
    StructField("% Nuls/Ins", DoubleType(), True),
    StructField("% Nuls/Vot", DoubleType(), True),
    StructField("Exprimés", IntegerType(), True),
    StructField("% Exp/Ins", DoubleType(), True),
    StructField("% Exp/Vot", DoubleType(), True),
    StructField("Sexe", StringType(), True),
    StructField("Nom", StringType(), True),
    StructField("Prénom", StringType(), True),
    StructField("Voix", IntegerType(), True),
    StructField("% Voix/Ins", DoubleType(), True),
    StructField("% Voix/Exp", DoubleType(), True),
    StructField("Unnamed: 23", StringType(), True),
    StructField("Unnamed: 24", StringType(), True),
    StructField("Unnamed: 25", StringType(), True),
    StructField("Unnamed: 26", IntegerType(), True),
    StructField("Unnamed: 27", DoubleType(), True),
    StructField("Unnamed: 28", DoubleType(), True),
    # Add more fields based on your data types
])

# Read data from Kafka
raw_data = spark \
          .read \
          .format("kafka") \
          .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
          .option("subscribe", kafka_topic) \
          .option("startingOffsets", "earliest") \
          .load()

# Convert the value column from Kafka to a string
data_string = raw_data.selectExpr("CAST(value AS STRING)")

# Parse the JSON data and apply the schema
parsed_data = data_string \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# Show the DataFrame with the "value" column as string
parsed_data.show(truncate=False)

# Sélectionner uniquement les 30 premières colonnes nécessaires de la table Emploi
selected_columns = [
    "Code du département", "Libellé du département", "Etat saisie", "Inscrits", "Abstentions", "% Abs/Ins", 
    "Votants", "% Vot/Ins", "Blancs", "% Blancs/Ins", "% Blancs/Vot", "Nuls", "% Nuls/Ins", "% Nuls/Vot", 
    "Exprimés", "% Exp/Ins", "% Exp/Vot", "Sexe", "Nom", "Prénom", "Voix", "% Voix/Ins", "% Voix/Exp", 
    "Unnamed: 23", "Unnamed: 24", "Unnamed: 25", "Unnamed: 26", "Unnamed: 27", "Unnamed: 28"
]
# Select only the specified columns
select_df = parsed_data.select(selected_columns)

# Noms des colonnes
columns = ["Code du département", "Libellé du département", "Etat saisie", "Inscrits", "Abstentions", 
           "% Abs/Ins", "Votants", "% Vot/Ins", "Blancs", "% Blancs/Ins", "% Blancs/Vot", "Nuls", 
           "% Nuls/Ins", "% Nuls/Vot", "Exprimés", "% Exp/Ins", "% Exp/Vot", "Sexe", "Nom", "Prénom",
           "Voix", "% Voix/Ins", "% Voix/Exp"] 


new_name = []
# Renommage des colonnes suivantes en utilisant le modèle cyclique
columns_to_rename = ["Sexe", "Nom", "Prénom", "Voix", "% Voix/Ins", "% Voix/Exp"]
for i in range(1, 2):
    for col in columns_to_rename:
        columns.append(f"{col}_{i}")

# Renommer les colonnes dans le DataFrame
select_df = select_df.toDF(*columns)

# Afficher le DataFrame résultant
# select_df.show(truncate=False)

# Configuration pour la connexion à PostgreSQL
properties = {
    "user": "surelmanda",
    "password": "postgres",
    "driver": "org.postgresql.Driver",
    "url": "jdbc:postgresql://localhost:5432/DB_Election_Pipeline",
    "version": "42.2.23",
    "table_name":"resultat_tourt2",
}

# Écriture du DataFrame résultant dans la table Logement dans PostgreSQL avec troncature
select_df.write.jdbc(
    url=properties["url"],
    table=properties["table_name"],
    mode="overwrite",  # Utiliser "overwrite" pour tronquer la table
    properties={
        "user": properties["user"],
        "password": properties["password"],
        "driver": properties["driver"],
        "truncate": "true"  # Ajouter l'option truncate
    }
)

# Arrêter la session Spark après le traitement
spark.stop()