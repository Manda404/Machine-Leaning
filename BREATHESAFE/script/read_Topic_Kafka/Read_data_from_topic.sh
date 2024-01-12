#!/bin/bash

KAFKA_HOME="/Users/surelmanda/Downloads/Ece-Pipelines/nifi/kafka"

# Assurez-vous que la variable d'environnement KAFKA_HOME est définie
if [ -z "$KAFKA_HOME" ]; then
    echo "Erreur : La variable d'environnement KAFKA_HOME n'est pas définie."
    exit 1
fi

# Spécifiez les paramètres de la commande
bootstrap_server="localhost:9092"
topic="Values_topic"

# Construisez le chemin complet vers kafka-console-consumer.sh
console_consumer_script="$KAFKA_HOME/bin/kafka-console-consumer.sh"

# Construisez la commande kafka-console-consumer.sh
command="$console_consumer_script --bootstrap-server $bootstrap_server --topic $topic --from-beginning"

# Exécutez la commande
echo "Lancement de la commande : $command"
$command
