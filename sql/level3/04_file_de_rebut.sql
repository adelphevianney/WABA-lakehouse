-- File de rebut : ce que le pipeline a refusé, et pourquoi (§3.3)
--
-- L'énoncé exige que les messages malformés soient « capturés et non
-- silencieusement ignorés ». Les capturer ne suffit pas : encore faut-il pouvoir
-- les lire. Le topic de rebut étant exposé en SQL, l'exploitation d'un incident
-- ne demande ni client Kafka ni accès au conteneur.
--
-- Deux étapes y déposent, et la colonne `stage` les distingue :
--   * `nifi-ingestion`  — le fichier lui-même est illisible, il n'a jamais été
--                         découpé en événements ;
--   * `raw_to_silver`   — le message est du JSON, mais il viole une règle de
--                         validation : champ obligatoire absent, valeur non
--                         convertible, devise incohérente avec le pays ;
--   * `silver_to_gold`  — un message Silver inexploitable, cas qui ne devrait
--                         pas se produire puisque le Job 1 l'a déjà validé.
--
-- Le message d'origine est conservé intact : il pourra être rejoué après
-- correction, là où un simple journal d'erreur imposerait de le reconstituer.

SELECT stage AS etape,
       coalesce(dataset, '(fichier)') AS jeu_de_donnees,
       rejection_reason AS motif,
       count(*) AS messages,
       min(received_at) AS premier,
       max(received_at) AS dernier,
       -- Un extrait suffit à reconnaître la nature du problème sans noyer la
       -- sortie ; le message complet reste dans le topic.
       substr(max(original_message), 1, 60) AS exemple
FROM kafka.default."dlq-financial-events"
WHERE rejection_reason IS NOT NULL
GROUP BY 1, 2, 3
ORDER BY 4 DESC, 1, 3;
