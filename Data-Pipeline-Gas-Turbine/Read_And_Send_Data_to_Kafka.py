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

def envoyer_ligne_a_kafka(producer, kafka_topic, ligne):
    # Récupérer les colonnes nécessaires
    # column_names = ['AT', 'AP', 'AH', 'AFDP', 'GTEP', 'TIT', 'TAT', 'CDP', 'TEY', 'CO', 'NOX']
    # data_values = {col: ligne[col] for col in column_names}

    # Ajouter d'autres informations au dictionnaire
    # data_values['timestamp'] = datetime.now().isoformat()

    # Créer un dictionnaire avec les données
    # data_to_send = data_values


    # Récupérer les colonnes nécessaires
    data_a_envoyer = {
        'ambient_temperature': ligne['AT'],
        'ambient_pressure': ligne['AP'],
        'ambient_humidity': ligne['AH'],
        'air_filter_diff_pressure': ligne['AFDP'],
        'exhaust_pressure': ligne['GTEP'],
        'inlet_temperature': ligne['TIT'],
        'after_temperature': ligne['TAT'],
        'discharge_pressure': ligne['CDP'],
        'energy_yield': ligne['TEY'],
        'carbon_monoxide': ligne['CO'],
        'nitrogen_oxides': ligne['NOX'],
        'timestamp': datetime.now().isoformat()
    }

    # Convertir le dictionnaire en une chaîne JSON
    data_json = json.dumps(data_a_envoyer)

    # Visualiser la donnée avant de l'envoyer
    print(f"Donnée à envoyer au topic Kafka : {data_json}")

    # Envoyer au topic Kafka
    producer.produce(kafka_topic, key=str(ligne['ID']), value=data_json)
    producer.flush()  # Assurer que le message est envoyé immédiatement

def lire_csv_et_envoyer_a_kafka(chemin_du_fichier_csv, separateur, kafka_topic, bootstrap_servers, column_names):
    # Configurer le producteur Kafka
    producer = configurer_producteur_kafka(bootstrap_servers)

    # Lire le fichier CSV
    try:
        for chunk in pd.read_csv(chemin_du_fichier_csv, sep=separateur, chunksize=1, iterator=True, skiprows=1, header=None, names=column_names):
            # chunk est un DataFrame contenant une seule ligne
            ligne = chunk.iloc[0]
        
            # print("Information sur la ligne du dataset de test: \n", ligne)

            # Envoyer la ligne au topic Kafka
            envoyer_ligne_a_kafka(producer, kafka_topic, ligne)

            # Ajouter un temps aléatoire entre 1 et 5 secondes avant d'envoyer la prochaine ligne
            time.sleep(random.uniform(10, 25) / 10)

    except FileNotFoundError:
        print(f"Le fichier '{chemin_du_fichier_csv}' n'a pas été trouvé.")
    except pd.errors.EmptyDataError:
        print(f"Le fichier '{chemin_du_fichier_csv}' est vide.")
    except pd.errors.ParserError:
        print(f"Erreur de lecture du fichier '{chemin_du_fichier_csv}'. Vérifiez le format du fichier.")

    # Fermer le producteur Kafka à la fin
    producer.flush()  # Assurer que tout message restant est envoyé
    producer.close()

# Exemple d'utilisation
chemin_du_fichier_csv = '/Users/surelmanda/Downloads/AirGUARD/Gas_Turbine_Streaming/data/data_test.csv'
separateur_csv = ','  # Remplacez par votre séparateur réel
kafka_topic = 'Gas_Turbine_topic'
bootstrap_servers = 'localhost:9092'  # Remplacez par les serveurs Kafka réels
# Spécifier les noms de colonnes
column_names = ['ID','AT', 'AP', 'AH', 'AFDP', 'GTEP', 'TIT', 'TAT', 'TEY', 'CDP', 'CO', 'NOX']
# Ajouter 10.5 secondes avant l'exécution de la fonction:
time.sleep(1.15)
# Lancement de la fonction:
lire_csv_et_envoyer_a_kafka(chemin_du_fichier_csv, separateur_csv, kafka_topic, bootstrap_servers, column_names)