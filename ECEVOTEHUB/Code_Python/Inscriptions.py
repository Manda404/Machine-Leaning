import time
import json
import random
import psycopg2
import requests
random.seed(42)

BASE_URL = 'https://randomuser.me/api/?nat=gb' # Api pour recuperer les information sur des individu

PARTIES = ["InnoTech Alliance Party", "EcoEngage Coalition Party", 
           "ÉcoVation Party", "UnityTech Force", "FutureForward Engineers Party"]

#InnoTech Alliance - Mettant l'accent sur l'innovation technologique et les avancées scientifiques, cette partie pourrait représenter le dynamisme et la vision futuriste des étudiants en ingénierie.
#EcoEngage Coalition - Axée sur l'engagement écologique et le développement durable, cette partie pourrait être dédiée à promouvoir des initiatives respectueuses de l'environnement au sein de l'école d'ingénieurs.
#ÉcoVation Party - [Description de la troisième partie]
#UnityTech Force - Encourageant l'unité et la collaboration au sein de la communauté étudiante en ingénierie, cette partie pourrait mettre en avant la puissance collective des étudiants pour créer un environnement académique positif et coopératif.
#FutureForward Engineers Party - [Description de la cinquième partie]


# vairables
total_électeurs = 100
total_candidats = len(PARTIES)

# je genere les données sur les électeurs:
def générer_des_données_électeurs():
    response = requests.get(BASE_URL)
    if response.status_code == 200:
        user_data = response.json()['results'][0]
        return {
            "voter_id": user_data['login']['uuid'],
            "voter_name": f"{user_data['name']['first']} {user_data['name']['last']}",
            "date_of_birth": user_data['dob']['date'],
            "gender": user_data['gender'],
            "nationality": user_data['nat'],
            "registration_number": user_data['login']['username'],
            "address": {
                "street": f"{user_data['location']['street']['number']} {user_data['location']['street']['name']}",
                "city": user_data['location']['city'],
                "state": user_data['location']['state'],
                "country": user_data['location']['country'],
                "postcode": user_data['location']['postcode']
            },
            "email": user_data['email'],
            "phone_number": user_data['phone'],
            "cell_number": user_data['cell'],
            "picture": user_data['picture']['large'],
            "registered_age": user_data['registered']['age']
        }
    else:
        return "Error fetching data"

# je genere les données sur les candidats aux elections des déleguées à l'ecole ECE: 
def générer_des_données_candidats(candidate_number, total_parties):
    response = requests.get(BASE_URL + '&gender=' + ('female' if candidate_number % 2 == 1 else 'male'))
    if response.status_code == 200:
        user_data = response.json()['results'][0]


        return {
            "candidate_id": user_data['login']['uuid'],
            "candidate_name": f"{user_data['name']['first']} {user_data['name']['last']}",
            "party_affiliation": PARTIES[candidate_number % total_parties],
            "biography": "A brief bio of the candidate.",
            "campaign_platform": "Key campaign promises or platform.",
            "photo_url": user_data['picture']['large']
        }
    else:
        return "Error fetching data"

def insert_électeur(conn, cur, voter):
    cur.execute("""
                   INSERT INTO Inscrits (voter_id, voter_name, date_of_birth, gender, nationality, 
                   registration_number, address_street, address_city, address_state, address_country, 
                   address_postcode, email, phone_number, cell_number, picture, registered_age)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s,%s,%s,%s,%s,%s)
                        """,
                (voter["voter_id"], voter['voter_name'], voter['date_of_birth'], voter['gender'],
                 voter['nationality'], voter['registration_number'], voter['address']['street'],
                 voter['address']['city'], voter['address']['state'], voter['address']['country'],
                 voter['address']['postcode'], voter['email'], voter['phone_number'],
                 voter['cell_number'], voter['picture'], voter['registered_age'])
                )
    conn.commit()

if __name__ == "__main__":
    try:
        # Connexion à la base de données PostgreSQL
        conn = psycopg2.connect("host=localhost dbname=DB_Election_Ece user=surelmanda password=postgres")
        # Création d'un curseur pour exécuter des requêtes SQL
        cur = conn.cursor()
        # Récupération des candidats depuis la base de données
        cur.execute("""SELECT * FROM candidats""")
        candidats = cur.fetchall()

        #print("Visualisation de la table candidats: ", candidats)

        # Vérifie si la liste des candidats est vide
        if len(candidats) == 0:
            # Boucle pour générer trois candidats
            for i in range(total_candidats):
                # Génère les données d'un candidat en utilisant une fonction (générer_des_données_candidats)
                candidat = générer_des_données_candidats(i, total_candidats)

                # Affiche les données du candidat (utile pour le débogage)
                print(f"les données candidat qui ont été générées: ", candidat)

                # Exécute une requête SQL pour insérer les données du candidat dans la base de données
                cur.execute("""
                    INSERT INTO Candidats (candidate_id, candidate_name, party_affiliation, 
                    biography, campaign_platform, photo_url)
                    VALUES (%s, %s, %s, %s, %s, %s)
                """, (
                    candidat['candidate_id'], candidat['candidate_name'], candidat['party_affiliation'],
                    candidat['biography'], candidat['campaign_platform'], candidat['photo_url']))

            # Commit des modifications dans la base de données
            conn.commit()

        # Boucle pour générer des données des électeurs et les envoyer sur le topic Kafka
        for i in range(total_électeurs):
            # Générer des données d'électeur
            électeur = générer_des_données_électeurs()

            print(f"les données de l'électeur qui ont été générées: ", électeur)

            # Insérer les données de l'électeur dans la base de données
            insert_électeur(conn, cur, électeur)

    finally:
        # Fermer la connexion à la base de données
        cur.close()
        conn.close()
