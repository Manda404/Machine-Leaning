import psycopg2

# Connexion à la base de données  # "host=localhost dbname=DB_Election_Ece user=surelmanda password=postgres"
# Paramètres de connexion à la base de données
db_params = {
    'dbname': 'DB_Election_Pipeline',
    'user': 'surelmanda',
    'password': 'postgres',
    'host': 'localhost'
    #,'port': '5432'
}

# Connexion à la base de données
conn = psycopg2.connect(**db_params)
cur = conn.cursor()

# Requête SQL
sql_query = """
INSERT INTO FACT_VOTES_T2 (cod_dep, cod_candidat, inscrits, abstentions, pct_abs_ins, votants, pct_vot_ins, blancs,
                        pct_blancs_ins, pct_blancs_vot, nuls, pct_nuls_ins, pct_nuls_vot, exprimes, pct_exp_ins,
                        pct_exp_vot,voix,pct_voix_ins,pct_voix_exp)
SELECT dd.cod_dep, dc.cod_candidat, rt."Inscrits", rt."Abstentions", rt."% Abs/Ins", rt."Votants",
       rt."% Vot/Ins", rt."Blancs", rt."% Blancs/Ins", rt."% Blancs/Vot",rt."Nuls", rt."% Nuls/Ins",
       rt."% Nuls/Vot", rt."Exprimés",rt."% Exp/Ins", rt."% Exp/Vot", rt."Voix", rt."% Voix/Ins",rt."% Voix/Exp"
FROM public.resultat_tourt2 rt
JOIN
    DIM_DEPARTEMENTS_T2 dd ON rt."Code du département" = dd."Code du département"
JOIN
    DIM_CANDIDATS_T2 dc ON UPPER(rt."Nom") = UPPER(dc.Nom) AND UPPER(rt."Prénom") = UPPER(dc.Prénom);
"""
# Exécution de la requête
cur.execute(sql_query)

# Validation des modifications
conn.commit()

# Boucle pour les colonnes Voix_i
for num_col in range(1, 2):
    # Requête SQL avec le numéro de colonne
    sql_query = f"""
    INSERT INTO FACT_VOTES_T2 (cod_dep, cod_candidat, inscrits, abstentions, pct_abs_ins, votants, pct_vot_ins, blancs,
                            pct_blancs_ins, pct_blancs_vot, nuls, pct_nuls_ins, pct_nuls_vot, exprimes, pct_exp_ins,
                            pct_exp_vot, voix, pct_voix_ins, pct_voix_exp)
    SELECT dd.cod_dep, dc.COD_CANDIDAT, rt."Inscrits", rt."Abstentions", rt."% Abs/Ins", rt."Votants",
           rt."% Vot/Ins", rt."Blancs", rt."% Blancs/Ins", rt."% Blancs/Vot",
           rt."Nuls", rt."% Nuls/Ins", rt."% Nuls/Vot", rt."Exprimés",
           rt."% Exp/Ins", rt."% Exp/Vot", rt."Voix_{num_col}", rt."% Voix/Ins_{num_col}", rt."% Voix/Exp_{num_col}"
    FROM public.resultat_tourt2 rt
    JOIN
        DIM_DEPARTEMENTS_T2 dd ON rt."Code du département" = dd."Code du département"
    JOIN
        DIM_CANDIDATS_T2 dc ON UPPER(rt."Nom_{num_col}") = UPPER(dc.Nom) AND UPPER(rt."Prénom_{num_col}") = UPPER(dc.Prénom);
    """

# Valider et fermer la connexion
conn.commit()
cur.close()
conn.close()