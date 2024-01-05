#!/bin/bash

# Nom de la base de données à vérifier
DB_NAME="DB_Election_Ece"
DB_USER="surelmanda"

# Vérifier si la base de données existe
EXISTING_DB=$(psql -lqt -U $DB_USER | cut -d \| -f 1 | grep -w $DB_NAME)

# Supprimer la base de données si elle existe
if [ -n "$EXISTING_DB" ]; then
  dropdb -U $DB_USER $DB_NAME
  echo "La base de données $DB_NAME existait et a été supprimée."
fi

# Créer la base de données
createdb -U $DB_USER $DB_NAME
echo "La base de données $DB_NAME a été créée avec succès."

# Tables
TABLE_CANDIDATS="Candidats"
TABLE_INSCRITS="Inscrits"
TABLE_VOTANTS="Votants"
TABLE_STAGING="Staging"
TABLE_RESULTAT="Resultat"

# Commande SQL pour créer la table "candidats"
SQL_COMMAND_CANDIDATS="CREATE TABLE $TABLE_CANDIDATS (
    candidate_id VARCHAR(255) PRIMARY KEY,
    candidate_name VARCHAR(255),
    party_affiliation VARCHAR(255),
    biography TEXT,
    campaign_platform TEXT,
    photo_url TEXT
);"

# Commande SQL pour créer la table "Inscrits"
SQL_COMMAND_INSCRITS="CREATE TABLE $TABLE_INSCRITS (
    voter_id VARCHAR(255) PRIMARY KEY,
    voter_name VARCHAR(255),
    date_of_birth VARCHAR(255),
    gender VARCHAR(255),
    nationality VARCHAR(255),
    registration_number VARCHAR(255),
    address_street VARCHAR(255),
    address_city VARCHAR(255),
    address_state VARCHAR(255),
    address_country VARCHAR(255),
    address_postcode VARCHAR(255),
    email VARCHAR(255),
    phone_number VARCHAR(255),
    cell_number VARCHAR(255),
    picture TEXT,
    registered_age INTEGER
);"

# Commande SQL pour créer la table "Votants"
SQL_COMMAND_VOTANTS="CREATE TABLE $TABLE_VOTANTS (
    voter_id VARCHAR(255) UNIQUE,
    candidate_id VARCHAR(255),
    voting_time TIMESTAMP,
    vote int DEFAULT 1,
    PRIMARY KEY (voter_id, candidate_id)
);"

# Commande SQL pour créer la table "Staging"
SQL_COMMAND_STAGING="CREATE TABLE $TABLE_STAGING (
    candidate_id VARCHAR(255),
    candidate_name VARCHAR(255),
    party_affiliation VARCHAR(255),
    photo_url TEXT,
    total_votes int,
    window_start TIMESTAMP, 
    window_end TIMESTAMP
);"

# Commande SQL pour créer la table "Resultat"
SQL_COMMAND_RESULTAT="CREATE TABLE $TABLE_RESULTAT (
    candidate_id VARCHAR(255),
    candidate_name VARCHAR(255),
    total_votes int DEFAULT 0,
    photo_url TEXT,
    PRIMARY KEY (candidate_id, candidate_name)
);"

# Exécution de la commande SQL pour créer la table "candidats"
psql -d $DB_NAME -U $DB_USER -c "$SQL_COMMAND_CANDIDATS"
echo "La table $TABLE_CANDIDATS a été créée avec succès."

# Exécution de la commande SQL pour créer la table "Inscrits"
psql -d $DB_NAME -U $DB_USER -c "$SQL_COMMAND_INSCRITS"
echo "La table $TABLE_INSCRITS a été créée avec succès."

# Exécution de la commande SQL pour créer la table "Votants"
psql -d $DB_NAME -U $DB_USER -c "$SQL_COMMAND_VOTANTS"
echo "La table $TABLE_VOTANTS a été créée avec succès."

# Exécution de la commande SQL pour créer la table "Staging"
psql -d $DB_NAME -U $DB_USER -c "$SQL_COMMAND_STAGING"
echo "La table $TABLE_STAGING a été créée avec succès."

# Exécution de la commande SQL pour créer la table "Resultat"
psql -d $DB_NAME -U $DB_USER -c "$SQL_COMMAND_RESULTAT"
echo "La table $TABLE_RESULTAT a été créée avec succès."
