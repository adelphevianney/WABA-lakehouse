-- Corridors transfrontaliers de mobile money (§2.3)
--
-- Chaque transfert apparaît deux fois : sortant depuis le pays émetteur,
-- entrant depuis le pays destinataire. Un responsable pays lit donc ses flux
-- dans les deux sens sans avoir à recomposer la symétrie.
--
-- L'émission n'est possible que depuis les quatre pays où WABA Mobile Money
-- opère — Côte d'Ivoire, Sénégal, Burkina Faso, Ghana — tandis que la réception
-- couvre les huit pays du groupe, le règlement passant par un opérateur
-- partenaire là où l'entité n'est pas implantée.

SELECT
    corridor,
    sens,
    sum(nb_transferts)                              AS transferts,
    CAST(round(sum(montant_total_eur)) AS BIGINT)   AS montant_total_eur,
    CAST(round(avg(montant_moyen_eur)) AS BIGINT)   AS montant_moyen_eur
FROM iceberg.gold.cross_border_transfers
GROUP BY corridor, sens
ORDER BY montant_total_eur DESC
LIMIT 15;
