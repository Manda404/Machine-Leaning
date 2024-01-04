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

# Exécution de la requête
cur.execute("""
    INSERT INTO DIM_DEPARTEMENTS_T2 ("Code du département", "Libellé du département", "Etat saisie")
    SELECT "Code du département", "Libellé du département", "Etat saisie"
    FROM public.resultat_tourt2
""")

# Validation de la transaction
conn.commit()

# Fermeture du curseur et de la connexion
cur.close()
conn.close()
