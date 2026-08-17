"""Écriture d'un micro-lot dans une table Iceberg, depuis un job de streaming.

Le job 1 fait tourner quatre flux et le job 2 trois : chacun fusionne son
micro-lot dans sa propre table, et rien dans la logique métier ne les oppose.
C'est le catalogue qui les oppose.

Le catalogue REST de ce déploiement persiste ses métadonnées dans un SQLite,
choisi pour sa légèreté face à un PostgreSQL dédié. Or SQLite verrouille le
fichier entier, et le catalogue JDBC d'Iceberg valide un commit en ouvrant une
transaction en lecture — pour relire le pointeur de métadonnées courant — qu'il
promeut ensuite en écriture. Deux flux qui commitent en même temps se disputent
ce verrou : le second reçoit `SQLITE_BUSY`, le catalogue répond 500, et Iceberg
lève un `CommitStateUnknownException`. C'est le pire des échecs, puisqu'il
laisse ignorer si le commit a été appliqué.

Sérialiser les commits dans le pilote résout le conflit là où il naît. Le verrou
ne coûte rien : un commit Iceberg est une mise à jour de pointeur, les données
Parquet ayant déjà été écrites en parallèle. Sur un catalogue adossé à
PostgreSQL, qui accepte les écritures concurrentes, il deviendrait inutile — et
inoffensif.
"""

from __future__ import annotations

import threading

from pyspark.sql import DataFrame, SparkSession

from jobs.batch import layers

#: Un seul commit de catalogue à la fois pour l'ensemble des flux du pilote.
_COMMIT_LOCK = threading.Lock()


def merge_micro_batch(
    spark: SparkSession,
    frame: DataFrame,
    table: str,
    namespace: str,
    key,
    partitioning: str,
) -> str:
    """Fusionne un micro-lot dans sa table, en sérialisant l'accès au catalogue.

    Le `MERGE` porte sur la clé naturelle : un rejeu du même micro-lot, après un
    incident ou une reprise depuis le point de contrôle, ne duplique rien.
    """
    with _COMMIT_LOCK:
        identifier = layers.create_table(spark, frame, table, namespace, partitioning)
        layers.merge_into(spark, frame, identifier, key)
    return identifier
