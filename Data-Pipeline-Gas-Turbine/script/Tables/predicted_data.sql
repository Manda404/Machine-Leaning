-- Création de la séquence
CREATE SEQUENCE cod_id_seq START 1;

-- Création de la table avec la colonne COD_ID
CREATE TABLE predicted_data (
    COD_ID INT DEFAULT nextval('cod_id_seq') PRIMARY KEY,  -- Identifiant unique auto-incrémenté
    Actual_Nox FLOAT,  -- Valeurs réelles de NOx
    Predicted_Nox FLOAT,  -- Valeurs prédites de NOx
    Read_Timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP  -- Horodatage pour la date et l'heure actuelles
);

-- Réinitialiser la séquence après un TRUNCATE ou un DROP
ALTER SEQUENCE cod_id_seq OWNED BY predicted_data.COD_ID;
