-- Rapport réglementaire consolidé, produit à J+1 (§2.1)
--
-- Sortie du DAG `dag_regulatory_report`, planifié à 00h30 UTC pour la journée
-- écoulée. Une ligne par pays et par journée déclarée.
--
-- Deux natures d'indicateurs y cohabitent, et les confondre serait une erreur
-- de lecture : le NPL et le loss ratio sont des **stocks**, photographiés à la
-- date du rapport, tandis que les volumes et les déclarations de soupçon sont
-- des **flux** de la seule journée.

SELECT
    reporting_date                                    AS journee_declaree,
    country_code                                      AS pays,
    round(npl_ratio * 100, 2)                         AS npl_pct,
    CASE WHEN npl_conforme THEN 'oui' ELSE 'NON' END  AS bceao_conforme,
    round(loss_ratio * 100, 1)                        AS loss_ratio_pct,
    CASE WHEN loss_ratio_conforme THEN 'oui' ELSE 'NON' END AS cima_conforme,
    nb_transactions,
    CAST(round(montant_transactions_eur) AS BIGINT)   AS montant_journee_eur,
    nb_declarations_aml,
    CAST(round(montant_declare_aml_eur) AS BIGINT)    AS montant_declare_eur
FROM iceberg.gold.regulatory_report
ORDER BY reporting_date DESC, npl_ratio DESC;
