<div style="text-align:center">
    <img src="Architecture.jpeg" alt="Texte de remplacement" width="800"/>
</div>


# Projet d'Analyses des Élections Présidentielles en France - Data Pipeline

Dans le cadre de la réalisation des analyses sur les élections présidentielles en France d’avril 2022, nous avons choisi de mettre en place un data pipeline pour la réalisation de ce projet. Sur le site français des statistiques, nous avons trouvé des datasets pouvant nous aider à avoir des données telles que celles des résultats des 1ers et 2èmes tours, du recensement 2020 et de l’emploi. Ces données nous ont permis de mettre en place des pipelines de données de la collecte jusqu’à l’analyse.

## Technologies Utilisées

- Nifi
- Airflow
- Python
- Kafka
- Spark
- PostgreSQL

## Flux de Données

Apache Nifi nous a permis d’intégrer les données venant d'internet. Pour le développement des flux de données, les processeurs tels que `invokeHTTP` nous ont aidés à récupérer les sources de données sur data.gouv. Les sources de données ont été converties à l’aide du processor `UnpackContent` avant d’être envoyées vers le processeur `SplitText` qui nous a permis de traiter les données ligne par ligne. Enfin, le processeur `PublishKafka` a été utilisé pour stocker les données. Chaque flux correspond à une source de données.

Exemples de sources de données :
- [Base CC Évolution Structurelle de la Population 2020](https://www.insee.fr/fr/statistiques/fichier/7632446/base-cc-evol-struct-pop-2020_xlsx.zip)
- [Base CC Emploi Population Active 2020](https://www.insee.fr/fr/statistiques/fichier/7632867/base-cc-emploi-pop-active-2020_csv.zip)
- [Base CC Logement 2020](https://www.insee.fr/fr/statistiques/fichier/7631186/base-cc-logement-2020_csv.zip)

## Kafka

Nous avons utilisé Kafka pour stocker les données collectées. Les données sur les résultats des 1ers et 2èmes tours, de la Population, de l’Emploi et du Logement, sont envoyées dans Kafka. Une fois dans Kafka, elles sont récupérées et traitées par des jobs Spark. Les résultats sont ensuite déposés dans des tables.

## Traitement des Données

Les données telles que celles du recensement, de l’emploi et du logement sont traitées à l’aide d'un job Spark. Les données des résultats des 1ers et 2èmes tours, localement récoltées, sont envoyées dans Kafka à partir d'un script Python. Une fois dans Kafka, elles sont récupérées et traitées dans Spark.

## Apache Spark

À travers des jobs Spark, les données sont traitées et envoyées dans une base de données PostgreSQL. Les tables sont créées pour analyser quelques colonnes des tables.

## Base de Données PostgreSQL

La base de données PostgreSQL permet de stocker les données traitées. À travers un code Python, elles sont dispatchées dans les data modèles qui créent des tables. Ces data modèles sont répartis en deux et organisés de la même manière : le 1er pour les résultats du 1er tour et le 2ème pour les résultats du 2ème.

## Liaisons entre les Tables

Pour le data modèle 1er tour, la liaison entre les tables est faite comme suit :
- `Dim_candidats` et `Dim_departement` sont liées à la table `Fact_vote` par les clés `COD_CANDIDAT` et `COD_DEP`.

```sql
-- Exemple de colonne pour la liaison dans Dim_candidats
COD_CANDIDAT INT DEFAULT nextval('cod_candidat_seq') PRIMARY KEY,

-- Exemple de colonne pour la liaison dans Dim_departement
COD_DEP INT DEFAULT nextval('cod_dep_sequence') PRIMARY KEY,
