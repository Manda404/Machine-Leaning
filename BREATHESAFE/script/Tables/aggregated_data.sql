-- Création de la séquence
CREATE SEQUENCE cod_id_seq START 1;

-- Création de la table avec la colonne COD_ID
CREATE TABLE aggregated_data (
    COD_ID INT DEFAULT nextval('cod_id_seq') PRIMARY KEY,
    avg_no2 FLOAT,
    avg_o3 FLOAT,
    avg_so2 FLOAT,
    avg_pm10 FLOAT,
    valid_values INTEGER,
    max_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Réinitialiser la séquence après un TRUNCATE ou un DROP
ALTER SEQUENCE cod_id_seq OWNED BY aggregated_data.COD_ID;