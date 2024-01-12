import time
import json  # Ajout du module json
import random
import requests
from datetime import datetime
import matplotlib.pyplot as plt
from matplotlib.animation import FuncAnimation
from confluent_kafka import Producer

# Endpoint de l'API
api_endpoint_pollutants = "http://127.0.0.1:5000/polluants"

# Listes pour stocker les données
max_data_points = 10  # Limite du nombre de points affichés à la fois
pollutants_data = {'time': [], 'no2': [], 'so2': [], 'pm10': [], 'o3': []}


def configurer_producteur_kafka(bootstrap_servers):
    # Configuration du producteur Kafka
    kafka_config = {
        'bootstrap.servers': bootstrap_servers,
        # Ajoutez d'autres configurations Kafka si nécessaire
    }
    # Créer un producteur Kafka
    producer = Producer(kafka_config)
    return producer

kafka_topic = 'Values_topic'
bootstrap_servers = 'localhost:9092'  # Remplacez par les serveurs Kafka réels
# Configurer le producteur Kafka
producer = configurer_producteur_kafka(bootstrap_servers)


def generate_random_id():
    return random.randint(10000, 99999)

def send_data_to_kafka(producer, kafka_topic,data):

    # Convertir le dictionnaire en une chaîne JSON
    data_json = json.dumps(data)

    # Visualiser la donnée avant de l'envoyer
    print(f"Donnée à envoyer au topic Kafka : {data_json}")

    # Envoyer au topic Kafka
    producer.produce(kafka_topic, key=str(generate_random_id()), value=data_json)
    producer.flush()  # Assurer que le message est envoyé immédiatement


# Fonction pour interroger l'API et récupérer les données
def fetch_data(endpoint, data_dict):
    response = requests.get(endpoint)
    if response.status_code == 200:
        data = response.json()
        #time.sleep(1.5)  # Attente d'une seconde entre chaque requête

        # Pause l'exécution du programme pendant un nombre aléatoire de secondes (entre 1 et 5 secondes)
        time.sleep(random.uniform(1, 5) / 20)
    
        # Ajouter les données au dictionnaire
        data_dict['time'].append(time.time())  # Utiliser le temps actuel
        for key in ['no2', 'so2', 'pm10', 'o3']:
            data_dict[key].append(data[key])

        # Limiter le nombre de points affichés
        if len(data_dict['time']) > max_data_points:
            for key in data_dict:
                data_dict[key] = data_dict[key][-max_data_points:]

        # send data to kafka:
        data['time']=datetime.now().isoformat()
        send_data_to_kafka(producer,kafka_topic,data)
        print(f"Id_data {generate_random_id()} - Données émises par l'API : {data}")
        print("")
        return True
    else:
        return False

# Fonction pour mettre à jour le graphique
def update_plot(frame):
    fetch_data(api_endpoint_pollutants, pollutants_data)

    plt.clf()  # Effacer le graphique existant

    plt.subplot(2, 1, 1)
    plt.plot(pollutants_data['time'], pollutants_data['no2'], marker='o', label='NO2')
    plt.plot(pollutants_data['time'], pollutants_data['so2'], marker='o', label='SO2')
    plt.title('Niveaux de NO2 et SO2 au fil du temps')
    plt.xlabel('Temps')
    plt.ylabel('Niveau de Polluants')
    plt.legend()

    plt.subplot(2, 1, 2)
    plt.plot(pollutants_data['time'], pollutants_data['pm10'], marker='o', color='black', label='pm10')
    plt.plot(pollutants_data['time'], pollutants_data['o3'], marker='o', color='red', label='O3')
    plt.title('Niveaux de Particules Fines et O3 au fil du temps')
    plt.xlabel('Temps')
    plt.ylabel('Niveau de Polluants')
    plt.legend()

# Ajouter de l'espace entre les deux figures
plt.subplots_adjust(hspace=0.6)


# Créer une animation
animation = FuncAnimation(plt.gcf(), update_plot, interval=1000)  # Mise à jour toutes les 1000 millisecondes (1 seconde)

plt.show()