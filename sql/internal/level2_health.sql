-- Sonde du test de fumée Level 2 : conformité des KPIs réglementaires.
--
-- Renvoie une ligne de compteurs, consommée par scripts/smoke_l2. Rangée hors
-- de sql/level2/, qui ne contient que les requêtes analytiques affichées.
--
-- Les fourchettes proviennent de la note réglementaire de l'énoncé : NPL entre
-- 3 % et 8 %, loss ratio entre 50 % et 85 %.

SELECT
    (SELECT count(*) FROM iceberg.gold.npl_ratio_by_country WHERE loan_type = 'ENSEMBLE')
        AS npl_pays,
    (SELECT count(*) FROM iceberg.gold.npl_ratio_by_country
      WHERE loan_type = 'ENSEMBLE' AND npl_ratio BETWEEN 0.03 AND 0.08)
        AS npl_conformes,
    (SELECT count(*) FROM iceberg.gold.loss_ratio_by_product WHERE loss_ratio IS NOT NULL)
        AS loss_couples,
    (SELECT count(*) FROM iceberg.gold.loss_ratio_by_product
      WHERE loss_ratio BETWEEN 0.50 AND 0.85)
        AS loss_conformes;
