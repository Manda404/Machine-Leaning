# import necessary libraries
import pandas as pd
from confluent_kafka import Producer
from datetime import datetime
import time
import random
import json  # Ajout du module json

def configurer_producteur_kafka(bootstrap_servers):
    # Configuration du producteur Kafka
    kafka_config = {
        'bootstrap.servers': bootstrap_servers,
        # Ajoutez d'autres configurations Kafka si nécessaire
    }
    # Créer un producteur Kafka
    producer = Producer(kafka_config)
    return producer

# Function to send a message to Kafka
def envoyer_ligne_a_kafka(producer, kafka_topic,index, message):
    # Convertir le dictionnaire en une chaîne JSON
    data_json = json.dumps(message)
    # Visualiser la donnée avant de l'envoyer
    print(f"Donnée à envoyer au topic Kafka : {data_json}")
    print("")
    # Envoyer au topic Kafka
    producer.produce(kafka_topic, key=str(index), value=data_json)
    producer.flush()  # Assurer que le message est envoyé immédiatement


# Read Excel file into a DataFrame
data = pd.read_excel('/Users/surelmanda/Downloads/AirGUARD/Extraction/FichierPipeline/resultats-par-niveau-dpt-t2-france-entiere.xlsx')

# Configure Kafka producer
bootstrap_servers = 'localhost:9092' # Replace with your Kafka bootstrap servers
topic = 'Topic_Resultat_TourT2'     # Replace with your Kafka topic

producer = configurer_producteur_kafka(bootstrap_servers)

# Iterate through rows of the DataFrame and send each row to Kafka
for index, row in data.head(50).iterrows():
    # Convert the row to a dictionary and then to a JSON string
    message = row.to_dict()
    envoyer_ligne_a_kafka(producer, topic,index, message)
    # Ajouter 1.5 secondes avant l'exécution de la fonction:
    time.sleep(1.15)

# Close the Kafka producer
producer.flush()
# ./bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic Topic_Resultat_ToursT1 --from-beginning