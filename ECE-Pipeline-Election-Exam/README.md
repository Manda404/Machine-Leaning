#Rapport de fin de module

Dans le cadre de la réalisation des analyses sur les élections présidentielles en France d’avril 2022, nous avons choisi de mettre en place un data pipline pour la réalisation de ce projet. 
Sur le site français des statistiques, nous avons trouvé des dataset pouvant nous aider à avoir des données telles que :
Celles des résultats des 1ers et 2èmes tours, du recensement 2020 et de l’emploi. Ces données nous ont permis de mettre en place des piplines de données de la collette jusqu’à l’analyse.
Technologie utilisées
Nifi
Airflow
Python
Kafka
Spark
Postgresql

Apache Nifi nous a permis d’intégré les données venant d’internet
Pour le développement des flux de données, les processeurs comme :
invokeHTTP avec lequel nous avons récupéré les sources de données sur data.gouv
Exemple :
https://www.insee.fr/fr/statistiques/fichier/7632446/base-cc-evol-struct-pop-2020_xlsx.zip
https://www.insee.fr /fr/statistiques/fichier/7632867/base-cc-emploi-pop-active-2020_csv.zip
https://www.insee.fr /fr/statistiques/fichier/7631186/base-cc-logement-2020_csv.zip
Ces sources de données ont été converti à l’aide du processor UnpackContent avant d’être envoyé vers le processeur SplitText qui nous a permis de traiter les données ligne par ligne. Enfin le processeur PublishKafka, avec lequel les données traitées ont été stockés.
Chaque flux correspond à une source de données
 
 
Les données comme celles des recensement, l’emploi et logement sont traitées à l’aide d’un jobspark.
Ci-dessous le code pour le traitement des données liées à l’emploi
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

Les données des résultats des 1ers et 2èmes tours localement récoltés sont envoyées dans kafka à partir d’un script python.
Une fois dans kafka, elles sont récupérées et traitées dans spark.
Apache Spark : à travers des jobs spark, les données sont traitées et envoyées dans une base de données postgresql dans laquelle les tables sont créés pour analyser quelques colonnes des tables 
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


La base de données postgresql permet dans notre cas de stockées les données traitées et à travers un code python ils sont dispatchées dans les data modèles qui créés des tables :
Ces data modèles sont réparties en deux et organisés de la même manière
Le 1er pour les resultats du 1er tour et le 2ème pour les résultats du 2ème 
La liaison entre les tables du data modèle 1er tour est faite comme suit :

Dim_candidats et dim_departement sont liées à la table fact_vote par les clés
    COD_CANDIDAT INT DEFAULT nextval('cod_candidat_seq') PRIMARY KEY,
Pour dim_candidats et 
    cod_dep INT DEFAULT nextval('cod_dep_sequence') PRIMARY KEY,
pour dim_departement  

 
Apache Airflow sert d'orchestrateur de flux de travail, il nous aide dans la planification, l’automatisation, la coordination et la surveillance des différentes étapes de notre flux de données.
 
Dans notre cas précis, les parties traitements de données sont des parties automatisées par airflow. Voir sur le schéma ci-dessus les parties orchestrer.

