import time
import json
import random
import psycopg2
from datetime import datetime, timedelta
from kafka import KafkaProducer

def obtenir_candidats_de_la_base(table_candidats):
    try:
        # Connexion à la base de données PostgreSQL
        # psycopg2.connect est une fonction du module psycopg2 en Python qui est utilisée pour établir une connexion à une base de données PostgreSQL.
        # La fonction connect est utilisée pour créer une connexion à la base de données PostgreSQL.
        connection = psycopg2.connect("host=localhost dbname=DB_Election_Ece user=surelmanda password=postgres")
        cursor = connection.cursor()

        # Requête pour récupérer les candidats au format JSON
        cursor.execute(f"""
            SELECT row_to_json(t)
            FROM (
                SELECT * FROM {table_candidats}
            ) t;
        """)
        
        # Récupération des candidats
        # Cursor (cur) : En PostgreSQL (et dans de nombreuses bases de données), un curseur est utilisé pour parcourir les résultats d'une requête SQL.
        # fetchall() : C'est une méthode du curseur qui récupère toutes les lignes résultantes d'une requête SQL sous forme de liste de tuples. Chaque tuple représente une ligne de la table résultante.
        json_candidats = cursor.fetchall()
        
        # Formatage des résultats
        candidats = [candidat[0] for candidat in json_candidats]

        # Vérification s'il y a des candidats
        if len(candidats) == 0:
            raise Exception("Aucun candidat trouvé dans la base de données")
        else:
            # Affichage des candidats (remplaçable par le traitement souhaité)
            # print(candidats)
            for i in range(len(candidats)):
                # Génère les données d'un candidat en utilisant une fonction (générer_des_données_candidats)
                print(f"les données candidat {i+1} qui a été récupéré: ", candidats[i])
                print("")
        
        # Fermeture du curseur et de la connexion
        cursor.close()
        connection.close()

        # Retourner les candidats (remplaçable par le traitement souhaité)
        return candidats

    except Exception as e:
        # Gestion des exceptions
        print(f"Une erreur s'est produite lors de la récupération des candidats: {e}")


def obtenir_électeurs_de_la_base(table_inscrits):
    try:
        # Connexion à la base de données PostgreSQL
        # psycopg2.connect est une fonction du module psycopg2 en Python qui est utilisée pour établir une connexion à une base de données PostgreSQL.
        # La fonction connect est utilisée pour créer une connexion à la base de données PostgreSQL.
        connection = psycopg2.connect("host=localhost dbname=DB_Election_Ece user=surelmanda password=postgres")
        cursor = connection.cursor()

        # Requête pour récupérer les candidats au format JSON
        cursor.execute(f"""
            SELECT row_to_json(t)
            FROM (
                SELECT * FROM {table_inscrits}
            ) t;
        """)
        
        # Récupération des électeurs
        # Cursor (cur) : En PostgreSQL (et dans de nombreuses bases de données), un curseur est utilisé pour parcourir les résultats d'une requête SQL.
        # fetchall() : C'est une méthode du curseur qui récupère toutes les lignes résultantes d'une requête SQL sous forme de liste de tuples. Chaque tuple représente une ligne de la table résultante.
        json_inscrits = cursor.fetchall()
        
        # Formatage des résultats
        electeurs = [inscrit[0] for inscrit in json_inscrits]

        # Vérification s'il y a des électeurs 
        if len(electeurs) == 0:
            raise Exception("Aucun electeur trouvé dans la base de données")
        else:
            # Affichage 2 des electeurs
            for i, electeur in enumerate(electeurs, start=1):
                # Génère les données d'un electeur en utilisant une fonction
                print(f"les données électeur {i} qui a été récupéré: ", electeur)
                print("")
        
        # Fermeture du curseur et de la connexion
        cursor.close()
        connection.close()

        # Retourner les electeurs
        return electeurs


    except Exception as e:
        # Gestion des exceptions
        print(f"Une erreur s'est produite lors de la récupération des électeurs: {e}")

def inserer_vote_dans_base_de_donnees(voter_id, candidate_id, voting_time, connection, cursor):
    """
    Insère un vote dans la table des votes dans la base de données PostgreSQL.

    Parameters:
    - voter_id (str): ID de l'électeur qui vote.
    - candidate_id (str): ID du candidat pour lequel l'électeur a voté.
    - voting_time (str): Horodatage du moment du vote.
    - connection (psycopg2.extensions.connection): Objet de connexion à la base de données.
    - cursor (psycopg2.extensions.cursor): Objet de curseur pour exécuter des requêtes SQL.

    Returns:
    - int: 1 en cas de succès, -1 en cas d'échec.
    """
    try:
        # Requête SQL pour l'insertion du vote
        query = """
            INSERT INTO votants (voter_id, candidate_id, voting_time)
            VALUES (%s, %s, %s)
        """

        # Exécution de la requête SQL avec les paramètres
        cursor.execute(query, (voter_id, candidate_id, voting_time))

        # Commit des modifications dans la base de données
        connection.commit()

        # Retourner 1 en cas de succès
        return 1
    except Exception as e:
        # Gérer les erreurs lors de l'insertion du vote
        print("Erreur lors de l'insertion du vote : {}".format(e))
        # En cas d'erreur, annuler les modifications dans la base de données et retourner -1
        connection.rollback()
        return -1



def choix_du_candidat(table_inscrits, table_candidats,interval_seconds = 1.1):
    # Obtenir la liste des candidats à partir de la base de données
    liste_des_candidats = obtenir_candidats_de_la_base(table_candidats)

    # Obtenir la liste des électeurs à partir de la base de données
    liste_des_inscrits = obtenir_électeurs_de_la_base(table_inscrits)

    # Connexion à la base de données PostgreSQL
    connection = psycopg2.connect("host=localhost dbname=DB_Election_Ece user=surelmanda password=postgres")
    cursor = connection.cursor()

    # Nom du topic kafka
    topic_votants = 'topic_des_votants'

    # Configuration du producteur Kafka ==> Producteur Synchrone (KafkaProducer) 
    producer = KafkaProducer(bootstrap_servers='localhost:9092',value_serializer=lambda v: json.dumps(v).encode('utf-8'))

    # j'obtiens la date et l'heure actuelles pour l'ouverture des bureaux
    heure_ouverture = datetime.now()
    print(f"Ouverture des bureaux de vote à : {heure_ouverture.strftime('%Y-%m-%d %H:%M:%S')}")

    # Boucle à travers la liste des inscrits pour effectuer le choix du candidat
    for inscrit in liste_des_inscrits:
        # Choisir aléatoirement un candidat parmi la liste des candidats
        chosen_candidate = random.choice(liste_des_candidats)

        # Créer un objet "vote" en combinant les informations du votant et du candidat choisi
        vote = inscrit | chosen_candidate | {"voting_time": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"), "vote": 1}
        try:
            # Afficher le message indiquant quel utilisateur vote pour quel candidat
            print("L'utilisateur {} vote pour le candidat : {}".format(vote['voter_id'], vote['candidate_id']))

            # Appel de la fonction pour insérer un vote
            flag = inserer_vote_dans_base_de_donnees(vote['voter_id'], vote['candidate_id'], vote['voting_time'], connection, cursor)
            
            if(flag==1):
                # Convertir la chaîne de caractères en bytes (UTF-8)
                key_bytes = vote["voter_id"].encode('utf-8')
                
                # Envoi des données à Kafka : Produire le vote dans le topic 'les_votants_topic' pour que d'autres services puissent le consommer
                producer.send(topic=topic_votants, value=vote,key=key_bytes) #, headers=None, partition=None, timestamp_ms=None
                
                # Ajouter un temps aléatoire entre 1 et 5 secondes avant d'envoyer la prochaine ligne
                time.sleep(random.uniform(1, 5))  # Pause aléatoire entre les votes
        finally:
                continue
    # Fermeture de la connexion à la base de données
    cursor.close()
    connection.close()
        
    # Obtenez la date et l'heure actuelles pour la fermeture des bureaux
    heure_fermeture = datetime.now()
    print(f"Fermeture des bureaux de vote à : {heure_fermeture.strftime('%Y-%m-%d %H:%M:%S')}")

    # Calculez la durée totale d'ouverture des bureaux
    duree_ouverture = heure_fermeture - heure_ouverture
    print(f"Durée totale d'ouverture des bureaux : {duree_ouverture}")

    
    # Fermer le producteur Kafka à la fin
    producer.flush()  # Assurer que tout message restant est envoyé
    producer.close()

if __name__ == "__main__":   
    # Définition des noms des tables dans la base de données
    table_inscr = "inscrits"   # Nom de la table contenant les inscrits
    table_cand = "candidats"   # Nom de la table contenant les candidats

    # Appel de la fonction choix_du_candidat avec les tables spécifiées
    #interval_seconds = 1.1 L'intervalle par défaut pour le temps entre les itérations est utilisé (1 seconde)
    choix_du_candidat(table_inscr, table_cand)

