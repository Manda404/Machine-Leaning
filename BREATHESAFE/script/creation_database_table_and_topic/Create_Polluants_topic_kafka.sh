#!/bin/bash

echo "Creation des topics necessaire sur le broker local Kafka..."

##########################
# Configuration Kafka
##########################

KAFKA_HOME="/Users/surelmanda/Downloads/Ece-Pipelines/nifi/kafka"
ZOOKEEPER_HOST="localhost:9092"
KAFKA_TOPICS_SCRIPT="$KAFKA_HOME/bin/kafka-topics.sh"
KAFKA_CONFIG_SCRIPT="$KAFKA_HOME/bin/kafka-configs.sh"

##########################
# Liste des topics à créer sur Kafka
##########################

TOPIC1="Values_topic"


##########################
# Fonctions
##########################

# Vérifier si les topics existent
topics_exist() {
  local topic=$1
  $KAFKA_TOPICS_SCRIPT --list --bootstrap-server $ZOOKEEPER_HOST | grep -q $topic
}

# Créer ou supprimer des topics et configurer la taille du message
manage_topic() {
  local topic=$1

  echo "Vérification de l'existence du topic $topic..."

  if topics_exist $topic; then
    echo "Le topic $topic existe. Suppression du topic."
    $KAFKA_TOPICS_SCRIPT --bootstrap-server $ZOOKEEPER_HOST --delete --topic $topic || {
      echo "Erreur lors de la suppression du topic $topic."
      exit 1
    }
  fi

  echo "Création du topic $topic."
  $KAFKA_TOPICS_SCRIPT --bootstrap-server $ZOOKEEPER_HOST --create --topic $topic --partitions 3 --replication-factor 1 || {
    echo "Erreur lors de la création du topic $topic."
    exit 1
  }

  echo "Configuration de la taille du message pour le topic $topic."
  $KAFKA_CONFIG_SCRIPT --bootstrap-server $ZOOKEEPER_HOST --alter --entity-name $topic --entity-type topics --add-config max.message.bytes=200000 || {
    echo "Erreur lors de la configuration de la taille du message pour le topic $topic."
    exit 1
  }

  echo "Le topic $topic a été créé avec succès."
}

##########################
# Gestion des topics
##########################

manage_topic $TOPIC1


##########################
# Message d'achèvement
##########################

echo "L'exécution du script est terminée."