-- Création de la séquence
CREATE SEQUENCE cod_dep_sequence START 1;

-- Création de la table dim_departements
CREATE TABLE dim_departements (
    cod_dep INT DEFAULT nextval('cod_dep_sequence') PRIMARY KEY,
    "Code du département" VARCHAR(10),
    "Libellé du département" VARCHAR(255),
    "Etat saisie" VARCHAR(15)
);

-- Réinitialiser la séquence après un TRUNCATE ou un DROP
ALTER SEQUENCE cod_dep_sequence OWNED BY dim_departements.cod_dep;