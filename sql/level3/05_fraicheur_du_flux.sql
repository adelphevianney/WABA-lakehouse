-- Fraîcheur du flux : le temps que met un événement à traverser la chaîne
--
-- Le critère d'évaluation du niveau demande que les événements parviennent aux
-- topics « avec un lag maîtrisé (< 30 s) ». Cette requête le mesure sur les
-- données elles-mêmes, plutôt que sur une impression.
--
-- Trois horodatages jalonnent le parcours d'un événement :
--   * `ingestion_timestamp` — apposé par NiFi au moment où il découpe le fichier ;
--   * `processed_at`        — apposé par le Job 1 lorsqu'il produit la ligne Silver ;
--   * `_timestamp`          — colonne interne du connecteur Kafka, instant où le
--                             broker a écrit le message dans le topic Silver.
--
-- Leurs écarts décomposent la latence, et il faut savoir ce qu'ils mesurent.
--
-- `latence` inclut l'attente du prochain déclenchement du Job 1 : elle vaut
-- quelques dizaines de secondes en mode continu — micro-lots de 20 s — et
-- plusieurs minutes si le job a été lancé à la main après coup. C'est une mesure
-- de bout en bout, pas du temps de calcul.
--
-- `publication` mesure ce qui sépare la construction de la ligne Silver de son
-- arrivée dans le topic. Ce n'est pas du temps perdu : c'est le coût du double
-- récepteur, la fusion Iceberg s'exécutant avant la publication Kafka. L'ordre
-- est délibéré — la table est idempotente, le topic ne l'est pas.
--
-- `age` dit enfin depuis combien de temps le dernier événement est arrivé :
-- c'est la seule colonne qui indique si le flux est encore vivant.

SELECT country_code AS pays,
       count(*) AS evenements,
       max(_timestamp) AS dernier_message,

       -- Traversée du Job 1 : de la publication par NiFi à l'écriture Silver.
       round(avg(date_diff('millisecond', ingestion_timestamp, processed_at)) / 1000.0, 1)
           AS latence_moyenne_s,
       round(max(date_diff('millisecond', ingestion_timestamp, processed_at)) / 1000.0, 1)
           AS latence_max_s,

       -- Publication du résultat dans le topic Silver.
       round(avg(date_diff('millisecond', processed_at, _timestamp)) / 1000.0, 1)
           AS publication_moyenne_s,

       -- Ancienneté du dernier message reçu.
       date_diff('second', max(_timestamp), CAST(current_timestamp AS timestamp(3)))
           AS age_dernier_message_s
FROM kafka.default."silver-bank-transactions"
WHERE transaction_id IS NOT NULL
GROUP BY 1
ORDER BY 1;
