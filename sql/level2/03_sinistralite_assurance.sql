-- Sinistralité de l'assurance : loss ratio et délais de traitement (§2.3)
--
-- Le ratio sinistres sur primes est comparé au seuil de vigilance CIMA de 70 %.
-- Un ratio élevé n'est pas nécessairement une anomalie : c'est un signal de
-- déséquilibre technique, qui appelle une révision tarifaire ou un
-- resserrement des conditions de souscription.
--
-- Le délai de traitement est joint pour la lecture métier : une branche à la
-- fois coûteuse en sinistres et lente à les régler cumule un problème de marge
-- et un problème de qualité de service.

SELECT
    l.country_code                                   AS pays,
    l.product_line                                   AS branche,
    CAST(round(l.primes_acquises_eur) AS BIGINT)     AS primes_eur,
    CAST(round(l.sinistres_regles_eur) AS BIGINT)    AS sinistres_eur,
    round(l.loss_ratio * 100, 1)                     AS loss_ratio_pct,
    CASE WHEN l.seuil_cima_depasse THEN 'au-dessus du seuil CIMA'
         ELSE 'conforme' END                         AS statut_cima,
    c.delai_moyen_jours,
    c.delai_p90_jours
FROM iceberg.gold.loss_ratio_by_product AS l
LEFT JOIN iceberg.gold.claims_processing_time AS c
       ON c.country_code = l.country_code
      AND c.ligne_metier = CASE WHEN l.product_line IN ('VIE', 'PREVOYANCE')
                                THEN 'VIE' ELSE 'IARD' END
WHERE l.loss_ratio IS NOT NULL
ORDER BY l.loss_ratio DESC
LIMIT 20;
