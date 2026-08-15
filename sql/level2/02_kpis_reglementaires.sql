-- KPIs réglementaires : BCEAO et CIMA (§2.3)
--
-- Les deux indicateurs sous surveillance des autorités de tutelle, avec leur
-- position par rapport au seuil. C'est le contenu du Dashboard 2 du Level 4.
--
-- Le NPL est un indicateur de **stock** : il rapporte l'encours douteux à
-- l'encours total du portefeuille à une date, et non aux seules échéances de la
-- période. La ligne ENSEMBLE porte l'indicateur réglementaire ; les autres en
-- donnent la ventilation par type de prêt.

SELECT
    country_code                                  AS pays,
    nb_prets,
    nb_prets_douteux,
    CAST(round(encours_total_eur) AS BIGINT)      AS encours_eur,
    CAST(round(encours_douteux_eur) AS BIGINT)    AS encours_douteux_eur,
    round(npl_ratio * 100, 2)                     AS npl_pct,
    CASE WHEN seuil_bceao_depasse THEN 'ALERTE — au-dessus de 5 %'
         WHEN npl_ratio > 0.04    THEN 'vigilance'
         ELSE 'conforme' END                      AS statut_bceao
FROM iceberg.gold.npl_ratio_by_country
WHERE loan_type = 'ENSEMBLE'
ORDER BY npl_ratio DESC;
