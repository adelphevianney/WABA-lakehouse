-- Santé du partitionnement
--
-- Diagnostic du morcellement des tables Iceberg. Les chiffres proviennent des
-- métadonnées, pas des données : aucune ligne n'est lue.
--
-- La colonne à surveiller est `octets_par_ligne`. Un partitionnement sain se
-- situe autour de 40 à 50 octets pour ces schémas ; une valeur qui s'envole
-- signale des fichiers trop petits pour que Parquet amortisse son en-tête, ses
-- dictionnaires et ses statistiques de colonnes.
--
-- Trois causes possibles quand la valeur dérive :
--   1. une granularité de partition trop fine pour la volumétrie — le cas
--      classique, un partitionnement journalier sur des données éparses ;
--   2. des ingestions successives dans les mêmes partitions, chacune ajoutant
--      ses propres fichiers sans réécrire les précédents ;
--   3. de petites tables de référence, où le morcellement est inévitable mais
--      sans conséquence puisque le volume total reste négligeable.
--
-- Le job `jobs.batch.compact` traite le cas 2 ; le cas 1 relève du schéma de
-- partitionnement ou de la volumétrie produite en amont.

SELECT 'bank_transactions'  AS table_brute, count(*) AS partitions, sum(file_count) AS fichiers,
       CAST(round(avg(record_count)) AS BIGINT) AS lignes_par_partition,
       CAST(round(avg(file_count), 1) AS DOUBLE) AS fichiers_par_partition
FROM iceberg.raw."bank_transactions$partitions"
UNION ALL SELECT 'insurance_operations', count(*), sum(file_count),
       CAST(round(avg(record_count)) AS BIGINT), CAST(round(avg(file_count), 1) AS DOUBLE)
FROM iceberg.raw."insurance_operations$partitions"
UNION ALL SELECT 'mobile_money_payments', count(*), sum(file_count),
       CAST(round(avg(record_count)) AS BIGINT), CAST(round(avg(file_count), 1) AS DOUBLE)
FROM iceberg.raw."mobile_money_payments$partitions"
UNION ALL SELECT 'loan_repayments', count(*), sum(file_count),
       CAST(round(avg(record_count)) AS BIGINT), CAST(round(avg(file_count), 1) AS DOUBLE)
FROM iceberg.raw."loan_repayments$partitions"
UNION ALL SELECT 'customers', count(*), sum(file_count),
       CAST(round(avg(record_count)) AS BIGINT), CAST(round(avg(file_count), 1) AS DOUBLE)
FROM iceberg.raw."customers$partitions"
UNION ALL SELECT 'accounts', count(*), sum(file_count),
       CAST(round(avg(record_count)) AS BIGINT), CAST(round(avg(file_count), 1) AS DOUBLE)
FROM iceberg.raw."accounts$partitions"
ORDER BY 4;
