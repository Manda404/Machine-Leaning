import time
from sqlalchemy import create_engine
import pandas as pd

# Configuration de la connexion à la base de données PostgreSQL
db_url = "postgresql://surelmanda:postgres@localhost:5432/DB_Election_Ece"
table_staging = "staging"
table_resultat = "resultat"

# Créez un moteur de base de données en utilisant l'URL de connexion
engine = create_engine(db_url) # À ce stade, 'engine' est prêt à être utilisé pour interagir avec la base de données

def aggregate_and_insert():
    # Lire les données de la table Staging
    query_staging = f"SELECT * FROM {table_staging};"
    staging_df = pd.read_sql(query_staging, engine)

    # Effectuer l'agrégation
    result_df = staging_df.groupby(['candidate_id', 'candidate_name', 'photo_url']).agg({'total_votes': 'sum'}).reset_index()
    #result_df.rename(columns={'total_votes': 'vote'}, inplace=True)

    # Insérer les résultats dans la table Resultat
    result_df.to_sql(table_resultat, engine, if_exists='replace', index=False)

    print("Aggregation and insertion completed.")

if __name__ == "__main__":
    while True:
        aggregate_and_insert()
        time.sleep(5)