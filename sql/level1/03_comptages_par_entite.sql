-- Comptages par entité (§1.4 de l'énoncé)
--
-- Portefeuille du groupe par ligne métier. La colonne `pays_operes` rend visible
-- en SQL la matrice pays x entité du tableau des entités : le mobile money
-- n'apparaît qu'en Côte d'Ivoire, au Sénégal, au Burkina Faso et au Ghana, la
-- microfinance qu'au Mali, en Guinée et au Burkina Faso.
--
-- C'est la vérification la plus directe que l'implantation réelle du groupe a
-- été respectée : un générateur tirant l'entité uniformément sur les huit pays
-- ferait apparaître ici huit pays sur chaque ligne.

SELECT
    c.entity_type                                                   AS entite,
    count(DISTINCT c.country_code)                                  AS nb_pays,
    array_join(array_agg(DISTINCT c.country_code ORDER BY c.country_code), ', ')
                                                                    AS pays_operes,
    count(*)                                                        AS clients,
    count_if(c.is_active)                                           AS clients_actifs,
    count_if(c.kyc_level = 'ENHANCED')                              AS kyc_renforce,
    round(100.0 * count_if(c.segment IN ('CORPORATE', 'PREMIUM'))
          / count(*), 1)                                            AS part_haut_de_gamme_pct
FROM iceberg.raw.customers AS c
GROUP BY c.entity_type
ORDER BY clients DESC;
