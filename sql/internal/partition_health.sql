-- Sonde utilisée par le test de fumée : nombre moyen de lignes par partition.
--
-- Rangée hors de sql/level1/, qui ne contient que les requêtes analytiques
-- exécutées et affichées par `make queries-l1`.
--
-- Les tables de métadonnées Iceberg exigent des identifiants entre guillemets,
-- lesquels ne survivent pas au passage en ligne de commande sous PowerShell.
-- D'où un fichier plutôt qu'un `--execute`.

SELECT CAST(round(avg(record_count)) AS BIGINT) AS lignes_par_partition
FROM iceberg.raw."bank_transactions$partitions";
