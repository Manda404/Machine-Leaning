-- Création de la séquence
CREATE SEQUENCE cod_candidat_seq START 1;

-- Création de la table avec la colonne COD_CANDIDAT
CREATE TABLE DIM_CANDIDATS (
    COD_CANDIDAT INT DEFAULT nextval('cod_candidat_seq') PRIMARY KEY,
    Nom VARCHAR(255),
    Prénom VARCHAR(255),
    Sexe VARCHAR(255)
);

-- Réinitialiser la séquence après un TRUNCATE ou un DROP
ALTER SEQUENCE cod_candidat_seq OWNED BY DIM_CANDIDATS.COD_CANDIDAT;