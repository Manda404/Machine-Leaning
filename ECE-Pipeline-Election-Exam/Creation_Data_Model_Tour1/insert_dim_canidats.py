import psycopg2

# Connexion à la base de données  #"host=localhost dbname=DB_Election_Ece user=surelmanda password=postgres"
conn = psycopg2.connect(
    dbname="DB_Election_Pipeline",
    user="surelmanda",
    password="postgres",
    host="localhost"
   # port="votre_port"
)

# Création d'un curseur
cur = conn.cursor()

# Requête SQL à exécuter
query = """
        INSERT INTO DIM_CANDIDATS (nom, prénom, sexe)
        SELECT DISTINCT "Nom", "Prénom", "Sexe"
        FROM public.resultat_tourt1;
"""

# Exécution de la requête
cur.execute(query)

# Valider la transaction
conn.commit()

# Liste des colonnes dans les requêtes
colonnes = ["Nom", "Prénom", "Sexe"]

# Boucle pour chaque requête
for i in range(1, 12):
    # Construire la requête SQL
    query = f"""
        INSERT INTO DIM_CANDIDATS ({", ".join(colonnes)})
        SELECT DISTINCT "Nom_{i}", "Prénom_{i}", "Sexe_{i}"
        FROM public.resultat_tourt1;
    """
    
    # Exécuter la requête
    cur.execute(query)

# Valider la transaction
conn.commit()

# Fermer le curseur et la connexion
cur.close()
conn.close()
