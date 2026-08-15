-- Architecture médaillon : les trois zones et leur contenu (§2.2)
--
-- Bronze porte la donnée brute telle qu'ingérée, Silver la donnée nettoyée et
-- enrichie, Gold les agrégats. Les volumétries se lisent de haut en bas : Silver
-- reprend Bronze ligne pour ligne — le nettoyage ne supprime rien, il qualifie —
-- puis Gold condense en quelques centaines de lignes d'indicateurs.
--
-- `bronze.*` et `raw.*` désignent la même zone : l'énoncé emploie les deux
-- termes pour la même définition, et Bronze est exposée en vues plutôt que
-- dupliquée.

SELECT 'bronze' AS zone, 'bank_transactions' AS table_nom, count(*) AS lignes
FROM iceberg.bronze.bank_transactions
UNION ALL SELECT 'silver', 'bank_transactions', count(*) FROM iceberg.silver.bank_transactions
UNION ALL SELECT 'bronze', 'customers', count(*) FROM iceberg.bronze.customers
UNION ALL SELECT 'silver', 'customers', count(*) FROM iceberg.silver.customers
UNION ALL SELECT 'bronze', 'accounts', count(*) FROM iceberg.bronze.accounts
UNION ALL SELECT 'silver', 'accounts', count(*) FROM iceberg.silver.accounts
UNION ALL SELECT 'gold', 'daily_transaction_volume', count(*) FROM iceberg.gold.daily_transaction_volume
UNION ALL SELECT 'gold', 'npl_ratio_by_country', count(*) FROM iceberg.gold.npl_ratio_by_country
UNION ALL SELECT 'gold', 'customer_arpu_monthly', count(*) FROM iceberg.gold.customer_arpu_monthly
UNION ALL SELECT 'gold', 'loss_ratio_by_product', count(*) FROM iceberg.gold.loss_ratio_by_product
UNION ALL SELECT 'gold', 'claims_processing_time', count(*) FROM iceberg.gold.claims_processing_time
UNION ALL SELECT 'gold', 'mobile_money_daily_flow', count(*) FROM iceberg.gold.mobile_money_daily_flow
UNION ALL SELECT 'gold', 'cross_border_transfers', count(*) FROM iceberg.gold.cross_border_transfers
ORDER BY 1 DESC, 2;
