-- Création de la séquence
CREATE SEQUENCE cod_dep_sequence_t2 START 1;

-- Création de la table DIM_DEPARTEMENTS_T2 avec la colonne cod_dep
CREATE TABLE  DIM_DEPARTEMENTS_T2 (
    cod_dep INT DEFAULT nextval('cod_dep_sequence_t2') PRIMARY KEY,
    "Code du département" VARCHAR(10),
    "Libellé du département" VARCHAR(255),
    "Etat saisie" VARCHAR(15)
);

-- Réinitialiser la séquence après un TRUNCATE ou un DROP
ALTER SEQUENCE cod_dep_sequence_t2 OWNED BY DIM_DEPARTEMENTS_T2.cod_dep;