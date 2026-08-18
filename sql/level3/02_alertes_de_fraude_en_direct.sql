-- Alertes de fraude en direct, et cohérence du double récepteur (§3.3)
--
-- Le Job 2 écrit chaque alerte dans le topic `gold-fraud-alerts` et dans la
-- table Iceberg `gold.fraud_alerts`. La première est faite pour être consommée
-- — un moteur de règles, une console de supervision —, la seconde pour être
-- historisée et rejouée.
--
-- Les deux doivent dire la même chose. Cette requête les confronte : le topic
-- peut porter des doublons, la publication n'étant pas transactionnelle avec la
-- fusion Iceberg, mais le nombre d'alertes **distinctes** doit coïncider. Un
-- écart signalerait qu'une écriture a échoué là où l'autre a réussi.

WITH flux AS (
    SELECT alert_type,
           count(*) AS messages,
           count(DISTINCT alert_id) AS alertes_distinctes
    FROM kafka.default."gold-fraud-alerts"
    WHERE alert_id IS NOT NULL
    GROUP BY 1
),
historique AS (
    SELECT alert_type,
           count(*) AS alertes,
           count(DISTINCT subject_id) AS sujets,
           sum(occurrences) AS evenements,
           round(sum(amount_eur), 2) AS montant_eur
    FROM iceberg.gold.fraud_alerts
    GROUP BY 1
)
SELECT coalesce(h.alert_type, f.alert_type) AS type_alerte,
       coalesce(h.alertes, 0) AS alertes_iceberg,
       coalesce(f.alertes_distinctes, 0) AS alertes_kafka,
       coalesce(f.messages, 0) AS messages_kafka,
       coalesce(h.sujets, 0) AS sujets_incrimines,
       coalesce(h.evenements, 0) AS evenements_couverts,
       coalesce(h.montant_eur, 0) AS montant_eur,
       CASE WHEN coalesce(h.alertes, 0) = coalesce(f.alertes_distinctes, 0)
            THEN 'cohérent' ELSE 'ÉCART' END AS double_recepteur
FROM historique h
FULL OUTER JOIN flux f ON f.alert_type = h.alert_type
ORDER BY 1;
