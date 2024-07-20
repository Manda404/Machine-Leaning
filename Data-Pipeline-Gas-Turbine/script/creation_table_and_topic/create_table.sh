#!/bin/bash

# Nom de la base de données à vérifier
DB_NAME="DataBase_Polluants"
DB_USER="surelmanda"

# Liste des tables à créer
TABLES=("predicted_data")

# Traiter chaque table
for TABLE_NAME in "${TABLES[@]}"; do
  # Supprimer la table si elle existe
  psql -U $DB_USER -d $DB_NAME -c "DROP TABLE IF EXISTS $TABLE_NAME CASCADE;"

  # Charger la structure de la table depuis le fichier SQL
  psql -U $DB_USER -d $DB_NAME -f "/Users/surelmanda/Downloads/AirGUARD/Gas_Turbine_Streaming/script/Tables/${TABLE_NAME}.sql"
  echo "La table $TABLE_NAME a été crée avec succès."
done

# Message d'achèvement
echo "L'exécution du script est terminée."