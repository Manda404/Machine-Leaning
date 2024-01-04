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
    .appName("Processsing_resultats_t1")
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
kafka_topic = 'Topic_Resultat_ToursT1'

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
    StructField("Unnamed: 29", StringType(), True),
    StructField("Unnamed: 30", StringType(), True),
    StructField("Unnamed: 31", StringType(), True),
    StructField("Unnamed: 32", IntegerType(), True),
    StructField("Unnamed: 33", DoubleType(), True),
    StructField("Unnamed: 34", DoubleType(), True),
    StructField("Unnamed: 35", StringType(), True),
    StructField("Unnamed: 36", StringType(), True),
    StructField("Unnamed: 37", StringType(), True),
    StructField("Unnamed: 38", IntegerType(), True),
    StructField("Unnamed: 39", DoubleType(), True),
    StructField("Unnamed: 40", DoubleType(), True),
    StructField("Unnamed: 41", StringType(), True),
    StructField("Unnamed: 42", StringType(), True),
    StructField("Unnamed: 43", StringType(), True),
    StructField("Unnamed: 44", IntegerType(), True),
    StructField("Unnamed: 45", DoubleType(), True),
    StructField("Unnamed: 46", DoubleType(), True),
    StructField("Unnamed: 47", StringType(), True),
    StructField("Unnamed: 48", StringType(), True),
    StructField("Unnamed: 49", StringType(), True),
    StructField("Unnamed: 50", IntegerType(), True),
    StructField("Unnamed: 51", DoubleType(), True),
    StructField("Unnamed: 52", DoubleType(), True),
    StructField("Unnamed: 53", StringType(), True),
    StructField("Unnamed: 54", StringType(), True),
    StructField("Unnamed: 55", StringType(), True),
    StructField("Unnamed: 56", IntegerType(), True),
    StructField("Unnamed: 57", DoubleType(), True),
    StructField("Unnamed: 58", DoubleType(), True),
    StructField("Unnamed: 59", StringType(), True),
    StructField("Unnamed: 60", StringType(), True),
    StructField("Unnamed: 61", StringType(), True),
    StructField("Unnamed: 62", IntegerType(), True),
    StructField("Unnamed: 63", DoubleType(), True),
    StructField("Unnamed: 64", DoubleType(), True),
    StructField("Unnamed: 65", StringType(), True),
    StructField("Unnamed: 66", StringType(), True),
    StructField("Unnamed: 67", StringType(), True),
    StructField("Unnamed: 68", IntegerType(), True),
    StructField("Unnamed: 69", DoubleType(), True),
    StructField("Unnamed: 70", DoubleType(), True),
    StructField("Unnamed: 71", StringType(), True),
    StructField("Unnamed: 72", StringType(), True),
    StructField("Unnamed: 73", StringType(), True),
    StructField("Unnamed: 74", IntegerType(), True),
    StructField("Unnamed: 75", DoubleType(), True),
    StructField("Unnamed: 76", DoubleType(), True),
    StructField("Unnamed: 77", StringType(), True),
    StructField("Unnamed: 78", StringType(), True),
    StructField("Unnamed: 79", StringType(), True),
    StructField("Unnamed: 80", IntegerType(), True),
    StructField("Unnamed: 81", DoubleType(), True),
    StructField("Unnamed: 82", DoubleType(), True),
    StructField("Unnamed: 83", StringType(), True),
    StructField("Unnamed: 84", StringType(), True),
    StructField("Unnamed: 85", StringType(), True),
    StructField("Unnamed: 86", IntegerType(), True),
    StructField("Unnamed: 87", DoubleType(), True),
    StructField("Unnamed: 88", DoubleType(), True),
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
    "Unnamed: 23", "Unnamed: 24", "Unnamed: 25", "Unnamed: 26", "Unnamed: 27", "Unnamed: 28", "Unnamed: 29", 
    "Unnamed: 30", "Unnamed: 31", "Unnamed: 32", "Unnamed: 33", "Unnamed: 34", "Unnamed: 35", "Unnamed: 36", 
    "Unnamed: 37", "Unnamed: 38", "Unnamed: 39", "Unnamed: 40", "Unnamed: 41", "Unnamed: 42", "Unnamed: 43", 
    "Unnamed: 44", "Unnamed: 45", "Unnamed: 46", "Unnamed: 47", "Unnamed: 48", "Unnamed: 49", "Unnamed: 50", 
    "Unnamed: 51", "Unnamed: 52", "Unnamed: 53", "Unnamed: 54", "Unnamed: 55", "Unnamed: 56", "Unnamed: 57", 
    "Unnamed: 58", "Unnamed: 59", "Unnamed: 60", "Unnamed: 61", "Unnamed: 62", "Unnamed: 63", "Unnamed: 64", 
    "Unnamed: 65", "Unnamed: 66", "Unnamed: 67", "Unnamed: 68", "Unnamed: 69", "Unnamed: 70", "Unnamed: 71", 
    "Unnamed: 72", "Unnamed: 73", "Unnamed: 74", "Unnamed: 75", "Unnamed: 76", "Unnamed: 77", "Unnamed: 78", 
    "Unnamed: 79", "Unnamed: 80", "Unnamed: 81", "Unnamed: 82", "Unnamed: 83", "Unnamed: 84", "Unnamed: 85", 
    "Unnamed: 86", "Unnamed: 87", "Unnamed: 88"
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
for i in range(1, 12):
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
    "table_name":"resultat_tourt1",
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