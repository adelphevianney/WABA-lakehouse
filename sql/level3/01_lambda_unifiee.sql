-- Requête Lambda unifiée : historique consolidé et flux temps réel (§3.4)
--
-- La promesse de l'architecture Lambda tient dans cette requête : un analyste
-- interroge d'un seul SQL la couche batch — les agrégats Gold recalculés par
-- Airflow — et la couche temps réel — les événements encore dans Kafka. Trino
-- lit les deux, l'un par le catalogue Iceberg, l'autre par le connecteur Kafka.
--
-- Le point délicat n'est pas la jointure, c'est le **recouvrement**. Le Job 1
-- écrit chaque événement dans la table Silver *et* dans le topic Silver : les
-- deux sources décrivent les mêmes transactions, et les additionner sans
-- précaution compterait deux fois tout ce que le batch a déjà consolidé.
--
-- La borne de partage est l'horodatage du dernier calcul Gold. Tout événement
-- traité par le streaming après cet instant est, par construction, absent de
-- l'agrégat batch — et lui seul doit venir s'y ajouter. C'est la « vue temps
-- réel » du modèle Lambda : le batch fait foi jusqu'à la borne, le flux prend
-- le relais au-delà.

WITH borne AS (
    SELECT coalesce(
               CAST(max(processed_at) AS timestamp(3)),
               TIMESTAMP '1970-01-01 00:00:00.000'
           ) AS calcule_jusqu_a
    FROM iceberg.gold.daily_transaction_volume
),

-- Couche batch : l'agrégat Gold, ramené à la maille pays × jour.
consolide AS (
    SELECT country_code,
           transaction_date AS jour,
           sum(nb_transactions) AS operations,
           sum(montant_total_eur) AS montant_eur
    FROM iceberg.gold.daily_transaction_volume
    GROUP BY 1, 2
),

-- Couche temps réel : les événements Silver publiés depuis la borne, agrégés à
-- la volée. Le connecteur Kafka relit le topic à chaque exécution ; c'est
-- acceptable sur une rétention courte, et c'est le prix de la fraîcheur.
temps_reel AS (
    SELECT s.country_code,
           CAST(s.event_time AS date) AS jour,
           count(*) AS operations,
           sum(s.amount_eur) AS montant_eur
    FROM kafka.default."silver-bank-transactions" s
    CROSS JOIN borne b
    WHERE s.transaction_id IS NOT NULL
      AND s.processed_at > b.calcule_jusqu_a
    GROUP BY 1, 2
)

SELECT coalesce(c.country_code, t.country_code) AS pays,
       coalesce(c.jour, t.jour) AS jour,
       coalesce(c.operations, 0) AS operations_batch,
       coalesce(t.operations, 0) AS operations_temps_reel,
       round(coalesce(c.montant_eur, 0), 2) AS montant_batch_eur,
       round(coalesce(t.montant_eur, 0), 2) AS montant_temps_reel_eur,
       round(coalesce(c.montant_eur, 0) + coalesce(t.montant_eur, 0), 2) AS montant_total_eur,
       CASE WHEN c.country_code IS NULL THEN 'temps réel seul'
            WHEN t.country_code IS NULL THEN 'batch seul'
            ELSE 'batch + temps réel' END AS provenance
FROM consolide c
FULL OUTER JOIN temps_reel t
  ON c.country_code = t.country_code
 AND c.jour = t.jour
ORDER BY 1, 2;
