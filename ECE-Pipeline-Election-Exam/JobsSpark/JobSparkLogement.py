# Configuration du job Spark
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date
from pyspark.sql.functions import from_json, col, avg
from pyspark.sql.functions import split, col
from pyspark.sql.types import StructType, StringType, DoubleType

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
    .appName("WriteToPostgres_Table_Logement")
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
kafka_topic = 'Topic_Logement'

# Configuration du job Spark avec le schéma spécifié
df = spark \
    .read \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", kafka_topic) \
    .option("startingOffsets", "earliest") \
    .load()

# Convert the "value" column to string
df_string = df.select(col("value").cast("string").alias("string_value"))

# Show the DataFrame with the "value" column as string
# df_string.show(truncate=False)

# Ajoutez des colonnes  de la table logement en fonction des données
split_col = split(df_string['string_value'], ',')

# Créer le DataFrame avec les colonnes de la table logement
df = df_string \
    .withColumn('CODGEO', split_col.getItem(0).cast('string')) \
    .withColumn('REG', split_col.getItem(1).cast('string')) \
    .withColumn('DEP', split_col.getItem(2).cast('string')) \
    .withColumn('LIBGEO', split_col.getItem(3).cast('string')) \
    .withColumn('P20_LOG', split_col.getItem(4).cast('integer')) \
    .withColumn('P20_RP', split_col.getItem(5).cast('integer')) \
    .withColumn('P20_RSECOCC', split_col.getItem(6).cast('integer')) \
    .withColumn('P20_LOGVAC', split_col.getItem(7).cast('integer')) \
    .withColumn('P20_MAISON', split_col.getItem(8).cast('integer')) \
    .withColumn('P20_APPART', split_col.getItem(9).cast('integer')) \
    .withColumn('P20_RP_1P', split_col.getItem(10).cast('integer')) \
    .withColumn('P20_RP_2P', split_col.getItem(11).cast('integer')) \
    .withColumn('P20_RP_3P', split_col.getItem(12).cast('integer')) \
    .withColumn('P20_RP_4P', split_col.getItem(13).cast('integer')) \
    .withColumn('P20_RP_5PP', split_col.getItem(14).cast('integer')) \
    .withColumn('P20_NBPI_RP', split_col.getItem(15).cast('integer')) \
    .withColumn('P20_RPMAISON', split_col.getItem(16).cast('integer')) \
    .withColumn('P20_NBPI_RPMAISON', split_col.getItem(17).cast('integer')) \
    .withColumn('P20_RPAPPART', split_col.getItem(18).cast('integer')) \
    .withColumn('P20_NBPI_RPAPPART', split_col.getItem(19).cast('integer')) \
    .withColumn('C20_RP_HSTU1P', split_col.getItem(20).cast('integer')) \
    .withColumn('C20_RP_HSTU1P_SUROCC', split_col.getItem(21).cast('integer')) \
    .withColumn('P20_RP_ACHTOT', split_col.getItem(22).cast('integer')) \
    .withColumn('P20_RP_ACH19', split_col.getItem(23).cast('integer')) \
    .withColumn('P20_RP_ACH45', split_col.getItem(24).cast('integer')) \
    .withColumn('P20_RP_ACH70', split_col.getItem(25).cast('integer')) \
    .withColumn('P20_RP_ACH90', split_col.getItem(26).cast('integer')) \
    .withColumn('P20_RP_ACH05', split_col.getItem(27).cast('integer')) \
    .withColumn('P20_RP_ACH17', split_col.getItem(28).cast('integer')) \
    .withColumn('P20_RPMAISON_ACH19', split_col.getItem(29).cast('integer'))

# Sélectionner uniquement les 30 premières colonnes nécessaires de la table logement
select_df = df.select(
    'CODGEO', 'REG', 'DEP', 'LIBGEO', 'P20_LOG', 'P20_RP', 'P20_RSECOCC', 'P20_LOGVAC',
    'P20_MAISON', 'P20_APPART', 'P20_RP_1P', 'P20_RP_2P', 'P20_RP_3P', 'P20_RP_4P',
    'P20_RP_5PP', 'P20_NBPI_RP', 'P20_RPMAISON', 'P20_NBPI_RPMAISON', 'P20_RPAPPART',
    'P20_NBPI_RPAPPART', 'C20_RP_HSTU1P', 'C20_RP_HSTU1P_SUROCC', 'P20_RP_ACHTOT',
    'P20_RP_ACH19', 'P20_RP_ACH45', 'P20_RP_ACH70', 'P20_RP_ACH90', 'P20_RP_ACH05',
    'P20_RP_ACH17', 'P20_RPMAISON_ACH19'
)
# Afficher le DataFrame résultant
# select_df.show(truncate=False)

# Configuration pour la connexion à PostgreSQL
properties = {
    "user": "surelmanda",
    "password": "postgres",
    "driver": "org.postgresql.Driver",
    "url": "jdbc:postgresql://localhost:5432/DB_Election_Pipeline",
    "version": "42.2.23",
    "table_name":"logement",
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