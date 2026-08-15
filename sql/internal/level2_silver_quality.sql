-- Sonde du test de fumée Level 2 : qualité des transformations Silver.
--
-- Chaque compteur correspond à un critère d'évaluation du §2.2 et doit valoir
-- zéro. Renvoyer des compteurs plutôt que des lignes permet au script appelant
-- de conclure sans analyser de résultat.

SELECT
    -- Déduplication sur la clé naturelle.
    (SELECT count(*) - count(DISTINCT transaction_id)
       FROM iceberg.silver.bank_transactions)                          AS doublons,

    -- Jointure avec les référentiels : aucune transaction ne doit rester
    -- orpheline de son client après enrichissement.
    (SELECT count(*) FROM iceberg.silver.bank_transactions
      WHERE customer_segment IS NULL)                                  AS enrichissement_manquant,

    -- Conversion en euros, vérifiée contre la parité de chaque devise.
    (SELECT count(*) FROM iceberg.silver.bank_transactions
      WHERE abs(amount_eur - amount / (CASE currency WHEN 'XOF' THEN 655.957
                                                     ELSE 17.5 END)) > 0.02)
                                                                       AS conversions_fausses,

    -- Gestion des nulls : l'IBAN est absent au Ghana, hors du registre IBAN,
    -- et présent partout ailleurs. Un null hors Ghana serait une perte.
    (SELECT count(*) FROM iceberg.silver.accounts
      WHERE iban_masked IS NULL AND country_code <> 'GH')              AS iban_nuls_inattendus,

    -- Données personnelles : les valeurs en clair ne doivent pas survivre à la
    -- couche Silver, seules leurs versions masquées sont diffusées.
    -- Le catalogue doit être qualifié : le client Trino n'a pas de catalogue de
    -- session, et `information_schema` seul est ambigu.
    (SELECT count(*) FROM iceberg.information_schema.columns
      WHERE table_schema = 'silver' AND table_name = 'accounts'
        AND column_name IN ('iban', 'account_number'))                 AS colonnes_en_clair;
