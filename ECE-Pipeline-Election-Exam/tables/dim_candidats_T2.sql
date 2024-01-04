-- Création de la séquence
CREATE SEQUENCE cod_candidat_seq_t2 START 1;

-- Création de la table DIM_CANDIDATS_T2 avec la colonne COD_CANDIDAT
CREATE TABLE DIM_CANDIDATS_T2 (
    COD_CANDIDAT INT DEFAULT nextval('cod_candidat_seq_t2') PRIMARY KEY,
    Nom VARCHAR(255),
    Prénom VARCHAR(255),
    Sexe VARCHAR(255)
);

-- Réinitialiser la séquence après un TRUNCATE ou un DROP
ALTER SEQUENCE cod_candidat_seq_t2 OWNED BY DIM_CANDIDATS_T2.COD_CANDIDAT;