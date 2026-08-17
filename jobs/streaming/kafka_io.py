"""Lecture et écriture Kafka, et emplacement des points de reprise.

Rassemblé ici pour que les deux jobs de streaming partagent la même
configuration du broker : une divergence d'adresse ou de sémantique de lecture
entre les deux ne se manifesterait que par un job qui ne consomme rien.
"""

from __future__ import annotations

import os
from typing import List, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

#: Vu depuis un conteneur de la stack, le broker répond sur son nom de service.
BROKERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")

#: Les points de reprise vivent sur un volume dédié plutôt que dans MinIO. Le
#: gestionnaire de points de reprise de Spark suppose un renommage atomique, que
#: S3 ne fournit pas : sur un stockage objet, une reprise après incident peut
#: lire un état partiellement écrit. Le volume survit à un `docker compose down`,
#: ce qui suffit à démontrer la reprise sans perte.
CHECKPOINT_ROOT = os.getenv("WABA_CHECKPOINT_DIR", "/checkpoints")

#: Borne le volume d'un micro-lot. Sans cela, un premier démarrage sur un topic
#: déjà rempli tenterait de tout traiter en une passe, et le pilote Spark — 2 Go
#: dans ce déploiement — n'y survivrait pas.
MAX_OFFSETS_PER_TRIGGER = int(os.getenv("WABA_MAX_OFFSETS_PER_TRIGGER", "20000"))


def checkpoint(name: str) -> str:
    return "{}/{}".format(CHECKPOINT_ROOT.rstrip("/"), name)


def read_topics(
    spark: SparkSession,
    topics: List[str],
    starting_offsets: str = "earliest",
) -> DataFrame:
    """Flux brut d'un ou plusieurs topics, métadonnées Kafka comprises.

    L'horodatage du broker est conservé sous un nom explicite : c'est lui qui
    porte le filigrane des opérations à état, et le confondre avec l'horodatage
    métier de la transaction conduirait à traiter comme tardives des données qui
    ne le sont pas.
    """
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", BROKERS)
        .option("subscribe", ",".join(topics))
        .option("startingOffsets", starting_offsets)
        .option("maxOffsetsPerTrigger", MAX_OFFSETS_PER_TRIGGER)
        # Un topic purgé pendant l'arrêt du job ne doit pas empêcher son
        # redémarrage : la perte est signalée dans les logs, pas fatale.
        .option("failOnDataLoss", "false")
        # Ne lire que ce qui est effectivement validé : NiFi publie dans une
        # transaction, et lire les messages non validés exposerait à des
        # écritures annulées.
        .option("kafka.isolation.level", "read_committed")
        .load()
        .select(
            F.col("topic"),
            F.col("partition"),
            F.col("offset"),
            F.col("key").cast("string").alias("kafka_key"),
            F.col("timestamp").alias("kafka_timestamp"),
            F.col("value").cast("string").alias("payload"),
        )
    )


def publish(
    frame: DataFrame,
    topic: str,
    key: Optional[str] = None,
    columns: Optional[List[str]] = None,
) -> None:
    """Publie un lot statique, chaque ligne devenant un message JSON.

    Appelée depuis `foreachBatch` : c'est ce qui permet à un même flux
    d'alimenter Kafka et Iceberg, là où une écriture en continu ne peut viser
    qu'un seul récepteur.
    """
    selected = columns or frame.columns
    payload = F.to_json(F.struct(*[F.col(c) for c in selected]))
    message = frame.select(
        (F.col(key).cast("string") if key else F.lit(None).cast("string")).alias("key"),
        payload.alias("value"),
    )
    (
        message.write.format("kafka")
        .option("kafka.bootstrap.servers", BROKERS)
        .option("topic", topic)
        .save()
    )
