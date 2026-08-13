-- Traçabilité de l'ingestion
--
-- État des huit tables de la couche brute : volumétrie, nombre de fichiers
-- sources distincts et date de dernière ingestion. Les colonnes techniques
-- `source_file` et `ingestion_timestamp`, ajoutées à chaque table, permettent de
-- remonter d'une ligne du lakehouse au fichier dont elle provient — matière
-- première du data lineage attendu au Level 4.
--
-- Un écart entre le nombre de lignes déposées et le nombre de lignes ingérées
-- s'explique par la table des rejets, interrogeable de la même façon.

SELECT 'customers'             AS table_brute, count(*) AS lignes,
       count(DISTINCT source_file) AS fichiers, max(ingestion_timestamp) AS derniere_ingestion
FROM iceberg.raw.customers
UNION ALL SELECT 'accounts', count(*), count(DISTINCT source_file), max(ingestion_timestamp)
FROM iceberg.raw.accounts
UNION ALL SELECT 'branches', count(*), count(DISTINCT source_file), max(ingestion_timestamp)
FROM iceberg.raw.branches
UNION ALL SELECT 'products', count(*), count(DISTINCT source_file), max(ingestion_timestamp)
FROM iceberg.raw.products
UNION ALL SELECT 'bank_transactions', count(*), count(DISTINCT source_file), max(ingestion_timestamp)
FROM iceberg.raw.bank_transactions
UNION ALL SELECT 'insurance_operations', count(*), count(DISTINCT source_file), max(ingestion_timestamp)
FROM iceberg.raw.insurance_operations
UNION ALL SELECT 'mobile_money_payments', count(*), count(DISTINCT source_file), max(ingestion_timestamp)
FROM iceberg.raw.mobile_money_payments
UNION ALL SELECT 'loan_repayments', count(*), count(DISTINCT source_file), max(ingestion_timestamp)
FROM iceberg.raw.loan_repayments
ORDER BY 1;
