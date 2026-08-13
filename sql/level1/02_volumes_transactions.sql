-- Volumes de transactions (§1.4 de l'énoncé)
--
-- Activité transactionnelle par pays et par type d'opération. Le taux d'échec
-- est calculé ici parce qu'il conditionne l'interprétation des montants : un
-- volume élevé assorti d'un fort taux d'échec ne traduit pas la même réalité
-- commerciale qu'un volume identique entièrement abouti.
--
-- La requête s'appuie sur le partitionnement par country_code : un filtre sur
-- un pays donné n'ouvre que les fichiers de la partition correspondante.

SELECT
    t.country_code                                              AS pays,
    t.transaction_type                                          AS type_operation,
    count(*)                                                    AS volume,
    CAST(round(sum(t.amount)) AS BIGINT)                        AS montant_total,
    CAST(round(approx_percentile(t.amount, 0.5)) AS BIGINT)     AS montant_median,
    CAST(round(sum(t.fee_amount)) AS BIGINT)                    AS commissions,
    round(100.0 * count_if(t.transaction_status <> 'SUCCESS')
          / count(*), 1)                                        AS taux_echec_pct
FROM iceberg.raw.bank_transactions AS t
GROUP BY t.country_code, t.transaction_type
ORDER BY t.country_code, volume DESC;
