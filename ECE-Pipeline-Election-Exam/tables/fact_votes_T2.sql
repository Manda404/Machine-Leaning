-- Création de la séquence pour cod_vote
CREATE SEQUENCE cod_vote_sequence_t2 START 1;

-- Création de la table des votes tour 2
CREATE TABLE FACT_VOTES_T2 (
    cod_vote INT DEFAULT nextval('cod_vote_sequence_t2') PRIMARY KEY,
    cod_dep INT,
    cod_candidat INT,
    inscrits INTEGER,
    abstentions INTEGER,
    pct_abs_ins INTEGER,
    votants INTEGER,
    pct_vot_ins INTEGER,
    blancs INTEGER,
    pct_blancs_ins INTEGER,
    pct_blancs_vot INTEGER,
    nuls INTEGER,
    pct_nuls_ins INTEGER,
    pct_nuls_vot INTEGER,
    exprimes INTEGER,
    pct_exp_ins INTEGER,
    pct_exp_vot INTEGER,
    voix INTEGER,
    pct_voix_ins INTEGER,
    pct_voix_exp INTEGER,

    -- Ajout de contraintes de clé étrangère
    FOREIGN KEY (cod_dep) REFERENCES DIM_DEPARTEMENTS_T2 (cod_dep),
    FOREIGN KEY (cod_candidat) REFERENCES DIM_CANDIDATS_T2(COD_CANDIDAT)
    );

-- Réinitialiser la séquence après un TRUNCATE ou un DROP
ALTER SEQUENCE cod_vote_sequence_t2 OWNED BY FACT_VOTES_T2.cod_vote;