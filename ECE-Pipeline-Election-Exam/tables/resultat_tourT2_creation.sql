-- resultat_tourT2_creation.sql

CREATE TABLE resultat_tourT2 (
    "Code du département" VARCHAR(10),
    "Libellé du département" VARCHAR(255),
    "Etat saisie" VARCHAR(255),
    "Inscrits" INTEGER,
    "Abstentions" INTEGER,
    "% Abs/Ins" INTEGER,
    "Votants" INTEGER,
    "% Vot/Ins" INTEGER,
    "Blancs" INTEGER,
    "% Blancs/Ins" INTEGER,
    "% Blancs/Vot" INTEGER,
    "Nuls" INTEGER,
    "% Nuls/Ins" INTEGER,
    "% Nuls/Vot" INTEGER,
    "Exprimés" INTEGER,
    "% Exp/Ins" INTEGER,
    "% Exp/Vot" INTEGER,
    "Sexe" VARCHAR(255),
    "Nom" VARCHAR(255),
    "Prénom" VARCHAR(255),
    "Voix" INTEGER,
    "% Voix/Ins" INTEGER,
    "% Voix/Exp" INTEGER,
    "Sexe_1" VARCHAR(255),
    "Nom_1" VARCHAR(255),
    "Prénom_1" VARCHAR(255),
    "Voix_1" INTEGER,
    "% Voix/Ins_1" INTEGER,
    "% Voix/Exp_1" INTEGER
);