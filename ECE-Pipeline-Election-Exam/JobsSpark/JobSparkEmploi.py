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
    .appName("Processsing_Emploi")
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
kafka_topic = 'Topic_Emploi'

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

# Créer le DataFrame avec les colonnes de la table Emploi
df = df_string \
    .withColumn('CODGEO', split_col.getItem(0).cast('string')) \
    .withColumn('REG', split_col.getItem(1).cast('string')) \
    .withColumn('DEP', split_col.getItem(2).cast('string')) \
    .withColumn('LIBGEO', split_col.getItem(3).cast('string')) \
    .withColumn('P20_POP1564', split_col.getItem(4).cast('int')) \
    .withColumn('P20_POP1524', split_col.getItem(5).cast('int')) \
    .withColumn('P20_POP2554', split_col.getItem(6).cast('int')) \
    .withColumn('P20_POP5564', split_col.getItem(7).cast('int')) \
    .withColumn('P20_H1564', split_col.getItem(8).cast('int')) \
    .withColumn('P20_H1524', split_col.getItem(9).cast('int')) \
    .withColumn('P20_H2554', split_col.getItem(10).cast('int')) \
    .withColumn('P20_H5564', split_col.getItem(11).cast('int')) \
    .withColumn('P20_F1564', split_col.getItem(12).cast('int')) \
    .withColumn('P20_F1524', split_col.getItem(13).cast('int')) \
    .withColumn('P20_F2554', split_col.getItem(14).cast('int')) \
    .withColumn('P20_F5564', split_col.getItem(15).cast('int')) \
    .withColumn('P20_ACT1564', split_col.getItem(16).cast('int')) \
    .withColumn('P20_ACT1524', split_col.getItem(17).cast('int')) \
    .withColumn('P20_ACT2554', split_col.getItem(18).cast('int')) \
    .withColumn('P20_ACT5564', split_col.getItem(19).cast('int')) \
    .withColumn('P20_HACT1564', split_col.getItem(20).cast('int')) \
    .withColumn('P20_HACT1524', split_col.getItem(21).cast('int')) \
    .withColumn('P20_HACT2554', split_col.getItem(22).cast('int')) \
    .withColumn('P20_HACT5564', split_col.getItem(23).cast('int')) \
    .withColumn('P20_FACT1564', split_col.getItem(24).cast('int')) \
    .withColumn('P20_FACT1524', split_col.getItem(25).cast('int')) \
    .withColumn('P20_FACT2554', split_col.getItem(26).cast('int')) \
    .withColumn('P20_FACT5564', split_col.getItem(27).cast('int')) \
    .withColumn('P20_ACTOCC1564', split_col.getItem(28).cast('int')) \
    .withColumn('P20_ACTOCC1524', split_col.getItem(29).cast('int'))

# Sélectionner uniquement les 30 premières colonnes nécessaires de la table Emploi
select_df = df.select(
    'CODGEO', 'REG', 'DEP', 'LIBGEO',
    'P20_POP1564', 'P20_POP1524', 'P20_POP2554', 'P20_POP5564',
    'P20_H1564', 'P20_H1524', 'P20_H2554', 'P20_H5564',
    'P20_F1564', 'P20_F1524', 'P20_F2554', 'P20_F5564',
    'P20_ACT1564', 'P20_ACT1524', 'P20_ACT2554', 'P20_ACT5564',
    'P20_HACT1564', 'P20_HACT1524', 'P20_HACT2554', 'P20_HACT5564',
    'P20_FACT1564', 'P20_FACT1524', 'P20_FACT2554', 'P20_FACT5564',
    'P20_ACTOCC1564','P20_ACTOCC1524'
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
    "table_name":"emploi",
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