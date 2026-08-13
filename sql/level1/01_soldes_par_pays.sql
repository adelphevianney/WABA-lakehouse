-- Soldes par pays (§1.4 de l'énoncé)
--
-- Encours détenus par le groupe, ventilés par pays et par devise. La colonne en
-- euros rend les huit marchés comparables : sans elle, un encours ghanéen et un
-- encours ivoirien exprimés dans leur devise locale diffèrent d'un facteur 37 et
-- ne peuvent pas être additionnés.
--
-- La parité du franc CFA est une constante réglementaire, pas un cours de
-- marché. Le taux du cedi est ici une valeur figée : la conversion sera portée
-- par la couche Silver du Level 2, adossée à une table de taux historisée, car
-- convertir à la lecture interdit tout retraitement d'un cours passé.

-- Les montants sont convertis en entiers : un DOUBLE arrondi reste un DOUBLE,
-- que Trino affiche en notation scientifique dès le milliard.

SELECT
    a.country_code                                          AS pays,
    a.currency                                              AS devise,
    count(*)                                                AS comptes,
    count_if(a.status = 'ACTIVE')                           AS comptes_actifs,
    CAST(round(sum(a.balance)) AS BIGINT)                   AS encours_devise_locale,
    CAST(round(sum(a.balance) / CASE a.currency
                                    WHEN 'XOF' THEN 655.957
                                    ELSE 17.5
                                END) AS BIGINT)             AS encours_eur,
    CAST(round(avg(a.balance) / CASE a.currency
                                    WHEN 'XOF' THEN 655.957
                                    ELSE 17.5
                                END) AS BIGINT)             AS solde_moyen_eur
FROM iceberg.raw.accounts AS a
GROUP BY a.country_code, a.currency
ORDER BY encours_eur DESC;
