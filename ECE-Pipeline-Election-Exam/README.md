<div style="text-align:center">
    <img src="Architecture.jpeg" alt="Texte de remplacement" width="600"/>
</div>


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
https://www.insee.fr/fr/statistiques/fichier/7632867/base-cc-emploi-pop-active-2020_csv.zip
https://www.insee.fr/fr/statistiques/fichier/7631186/base-cc-logement-2020_csv.zip
Ces sources de données ont été converti à l’aide du processor UnpackContent avant d’être envoyé vers le processeur SplitText qui nous a permis de traiter les données ligne par ligne. Enfin le processeur PublishKafka, avec lequel les données traitées ont été stockés.
Chaque flux correspond à une source de données
 
 
Les données comme celles des recensement, l’emploi et logement sont traitées à l’aide d’un jobspark.
Les données des résultats des 1ers et 2èmes tours localement récoltés sont envoyées dans kafka à partir d’un script python.
Une fois dans kafka, elles sont récupérées et traitées dans spark.
Apache Spark : à travers des jobs spark, les données sont traitées et envoyées dans une base de données postgresql dans laquelle les tables sont créés pour analyser quelques colonnes des tables.

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