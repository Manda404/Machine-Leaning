#!/bin/bash

# Configuration Kafka
KAFKA_HOME="/Users/surelmanda/Downloads/Ece-Pipelines/nifi/kafka"
ZOOKEEPER_HOST="localhost:9092"
KAFKA_TOPICS_SCRIPT="$KAFKA_HOME/bin/kafka-topics.sh"

# Liste des topics à créer sur kafka
TOPIC1="Topic_Emploi"
TOPIC2="Topic_Population"
TOPIC3="Topic_Logement"
TOPIC4="Topic_Resultat_TourT1"
TOPIC5="Topic_Resultat_TourT2"

# Vérifier si les topics existent
topics_exist() {
  local topic=$1
  $KAFKA_TOPICS_SCRIPT --list --bootstrap-server localhost:9092 | grep -q $topic
}

# Créer ou supprimer des topics
manage_topic() {
  local topic=$1

  if topics_exist $topic; then
    echo "Le topic $topic existe. Suppression du topic."
    $KAFKA_TOPICS_SCRIPT --bootstrap-server $ZOOKEEPER_HOST  --delete --topic $topic
    echo "Création du topic $topic."
    $KAFKA_TOPICS_SCRIPT --bootstrap-server $ZOOKEEPER_HOST --create --topic $topic --partitions 3 --replication-factor 1
  else
    echo "Le topic $topic n'existe pas. Création du topic."
    $KAFKA_TOPICS_SCRIPT --bootstrap-server $ZOOKEEPER_HOST --create --topic $topic --partitions 3 --replication-factor 1
  fi
}

# Gestion des topics
manage_topic $TOPIC1
manage_topic $TOPIC2
manage_topic $TOPIC3
manage_topic $TOPIC4
manage_topic $TOPIC5