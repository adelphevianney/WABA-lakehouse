-- Surveillance AML : rapprochement du temps réel et du consolidé (§3.3)
--
-- Le seuil déclaratif de la BCEAO est de 1 000 000 XOF en zone UEMOA et de
-- 5 000 GHS au Ghana. Il est évalué **en devise locale** : convertir avant de
-- comparer appliquerait au Ghana un seuil calibré pour la zone franc.
--
-- Deux chemins comptent ces franchissements. Le batch les agrège dans
-- `gold.daily_transaction_volume` (colonne `nb_au_dessus_seuil_aml`), calculée
-- depuis la table Silver. Le streaming les publie un à un dans
-- `gold-aml-events`, quelques secondes après la transaction.
--
-- Les deux comptes ne sont pas censés être égaux, et c'est le point : ils ne
-- portent pas sur le même horizon. Le batch couvre tout l'historique consolidé ;
-- le topic ne conserve que la fenêtre de rétention du broker. La dernière
-- colonne mesure donc ce que le lakehouse détient et que le bus a déjà oublié —
-- exactement la raison d'être de la couche batch dans une architecture Lambda.
--
-- Ce qu'il faut surveiller est l'inverse : un pays présent dans le flux et
-- absent du batch signale une consolidation en retard, et un montant qui
-- n'apparaît nulle part, une déclaration omise.

WITH temps_reel AS (
    SELECT country_code,
           count(DISTINCT transaction_id) AS evenements,
           count(DISTINCT account_id) AS comptes,
           round(sum(amount_eur), 2) AS montant_eur,
           round(max(threshold_ratio), 1) AS depassement_max
    FROM kafka.default."gold-aml-events"
    WHERE event_id IS NOT NULL
    GROUP BY 1
),
consolide AS (
    SELECT country_code, sum(nb_au_dessus_seuil_aml) AS evenements
    FROM iceberg.gold.daily_transaction_volume
    GROUP BY 1
)
SELECT coalesce(c.country_code, t.country_code) AS pays,
       coalesce(c.evenements, 0) AS declarables_batch,
       coalesce(t.evenements, 0) AS declarables_temps_reel,
       coalesce(t.comptes, 0) AS comptes_concernes,
       coalesce(t.montant_eur, 0) AS montant_temps_reel_eur,
       coalesce(t.depassement_max, 0) AS depassement_max_du_seuil,
       coalesce(c.evenements, 0) - coalesce(t.evenements, 0) AS detenus_par_le_batch_seul
FROM consolide c
FULL OUTER JOIN temps_reel t ON t.country_code = c.country_code
ORDER BY 1;
