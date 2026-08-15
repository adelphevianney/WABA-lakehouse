-- Zone Bronze de l'architecture médaillon (§2.2).
--
-- Le §1.3 impose des tables `raw.*` et le §2.2 décrit une zone `bronze.*` avec
-- exactement la même définition : « données brutes ingérées depuis raw-landing,
-- sans transformation métier ». Ce sont deux noms pour la même zone.
--
-- Dupliquer physiquement des centaines de milliers de lignes pour satisfaire
-- une convention de nommage coûterait le double du stockage et introduirait un
-- risque de divergence entre deux copies censées être identiques. Bronze est
-- donc exposée en vues, ce qui rend les deux vocabulaires de l'énoncé valides
-- sans dédoubler la donnée. Les trois zones du médaillon restent distinctes par
-- leur contenu : brut, nettoyé, agrégé.
--
-- Les vues sont créées par Trino et non par Spark : une vue Iceberg mémorise le
-- dialecte SQL qui l'a produite, et Trino refuse de lire celles écrites par
-- Spark (« Cannot read unsupported dialect 'spark' »). Trino étant la surface
-- d'interrogation de la plateforme, c'est lui qui les définit.
--
-- Exécution : make bronze-views

CREATE SCHEMA IF NOT EXISTS iceberg.bronze;

CREATE OR REPLACE VIEW iceberg.bronze.customers AS
    SELECT * FROM iceberg.raw.customers;

CREATE OR REPLACE VIEW iceberg.bronze.accounts AS
    SELECT * FROM iceberg.raw.accounts;

CREATE OR REPLACE VIEW iceberg.bronze.branches AS
    SELECT * FROM iceberg.raw.branches;

CREATE OR REPLACE VIEW iceberg.bronze.products AS
    SELECT * FROM iceberg.raw.products;

CREATE OR REPLACE VIEW iceberg.bronze.bank_transactions AS
    SELECT * FROM iceberg.raw.bank_transactions;

CREATE OR REPLACE VIEW iceberg.bronze.insurance_operations AS
    SELECT * FROM iceberg.raw.insurance_operations;

CREATE OR REPLACE VIEW iceberg.bronze.mobile_money_payments AS
    SELECT * FROM iceberg.raw.mobile_money_payments;

CREATE OR REPLACE VIEW iceberg.bronze.loan_repayments AS
    SELECT * FROM iceberg.raw.loan_repayments;
