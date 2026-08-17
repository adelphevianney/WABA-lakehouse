"""Job 1 — Raw vers Silver en streaming (§3.3).

Consomme les quatre topics `raw-*` alimentés par NiFi, valide chaque message,
applique les transformations Silver et écrit le résultat dans **deux
destinations** : les topics `silver-*` de Kafka et les tables Iceberg `silver.*`.

Trois choix structurent ce job.

**Les transformations Silver ne sont pas réécrites.** Les constructeurs de
`jobs.batch.silver` acceptent une source explicite ; le job leur passe le
micro-lot au lieu de la table brute et obtient exactement les mêmes colonnes. Le
chemin batch et le chemin streaming alimentent les mêmes tables : deux
définitions de Silver auraient divergé au premier ajustement de règle métier, et
la divergence ne se serait vue que dans les chiffres.

**La validation non plus.** `jobs.batch.quality` évalue ses huit règles sur des
colonnes encore textuelles — ce que NiFi publie précisément. Seul le signal
d'illisibilité structurelle change de nom.

**Le double récepteur passe par `foreachBatch`.** Une écriture en continu ne
vise qu'un récepteur ; `foreachBatch` reçoit chaque micro-lot comme un
DataFrame ordinaire, que le job fusionne dans Iceberg puis publie dans Kafka.
C'est aussi ce qui permet de réutiliser le `MERGE` du batch, donc de conserver
l'idempotence : un rejeu de Kafka ne duplique rien.

Exemples :
    python -m jobs.streaming.raw_to_silver              # continu
    python -m jobs.streaming.raw_to_silver --once       # traite l'existant et sort
    python -m jobs.streaming.raw_to_silver --datasets bank_txn
"""

from __future__ import annotations

import argparse
import logging
import sys
from dataclasses import dataclass
from typing import Callable, List, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from common import domain as dom
from jobs.batch import layers, quality
from jobs.batch import schemas as sch
from jobs.batch import silver
from jobs.batch.session import build_session
from jobs.streaming import iceberg_sink, kafka_io

logger = logging.getLogger("jobs.streaming.raw_to_silver")

#: Colonne portant le message brut lorsqu'il n'est pas du JSON analysable.
CORRUPT_JSON = "_corrupt_json"
#: Motif de rejet, null pour une ligne valide.
REJECTION = "_rejection"
#: Clé de déduplication : la clé naturelle quand elle existe, une empreinte du
#: message sinon — un message illisible n'a pas d'identifiant, et sans cela un
#: même message corrompu rejoué remplirait la file de rebut.
DEDUP_KEY = "_dedup_key"

#: Fenêtre de déduplication demandée au §3.3.
#:
#: Elle porte sur l'horodatage d'arrivée dans Kafka, non sur l'horodatage métier
#: de la transaction. Les fichiers rejoués contiennent des événements datés de
#: mars 2026 : un filigrane posé sur cette date considérerait comme tardif tout
#: événement du début de journée reçu après ceux de la fin, et les écarterait —
#: ce n'est pas une déduplication, c'est une perte. L'heure d'arrivée, elle,
#: progresse avec l'horloge et ne peut pas reculer.
DEDUP_WINDOW = "10 minutes"


@dataclass(frozen=True)
class StreamSpec:
    """Un topic brut, sa table Silver et son topic Silver."""

    dataset: str
    #: Table Silver alimentée, telle que définie par le chemin batch.
    silver_table: str
    builder: Callable[..., DataFrame]

    @property
    def spec(self) -> sch.DatasetSpec:
        return sch.BY_NAME[self.dataset]

    @property
    def raw_topic(self) -> str:
        return dom.RAW_TOPICS[self.dataset]

    @property
    def silver_topic(self) -> Optional[str]:
        """Topic Silver, absent pour les remboursements de crédit.

        L'énoncé n'en prévoit que trois : les remboursements n'alimentent que la
        table Iceberg. C'est cohérent avec le Job 2, dont aucune règle ne porte
        sur eux.
        """
        return dom.SILVER_TOPICS.get(self.dataset)

    @property
    def table_definition(self) -> silver.SilverTable:
        return silver.BY_NAME[self.silver_table]


STREAMS: List[StreamSpec] = [
    StreamSpec("bank_txn", "bank_transactions", silver.build_bank_transactions),
    StreamSpec("insurance_ops", "insurance_operations", silver.build_insurance_operations),
    StreamSpec("mobile_money", "mobile_money_payments", silver.build_mobile_money_payments),
    StreamSpec("loan_repayments", "loan_repayments", silver.build_loan_repayments),
]

BY_DATASET = {stream.dataset: stream for stream in STREAMS}


# =============================================================================
# Analyse et validation des messages
# =============================================================================


def json_schema(spec: sch.DatasetSpec) -> StructType:
    """Schéma de lecture du message, entièrement textuel.

    NiFi publie les colonnes telles qu'elles figurent dans le CSV, sans les
    typer. Les lire en texte puis convertir explicitement est ce qui permet de
    distinguer une valeur absente d'une valeur présente mais non convertible :
    après un `CAST`, les deux sont nulles et le motif de rejet serait perdu.

    Le schéma déclare en dernier la colonne de rebut. Sans elle, `from_json`
    rend un message illisible sous forme de champs tous nuls, indiscernable d'un
    message dont les champs seraient réellement vides : la file de rebut
    recevrait « transaction_id manquant » là où le vrai motif est que le message
    n'est pas du JSON. C'est l'exact pendant de `_corrupt_record` côté CSV.
    """
    return StructType(
        [StructField(column.name, StringType(), True) for column in spec.all_columns]
        + [StructField(CORRUPT_JSON, StringType(), True)]
    )


def _champ(name: str):
    """Champ textuel du message, chaîne vide ramenée à null.

    Une cellule CSV vide traverse NiFi sous forme de chaîne vide. La traiter
    comme une valeur présente ferait rejeter des nulls parfaitement métier : la
    date de paiement d'une échéance impayée, le délai de traitement d'une
    opération qui n'est pas un sinistre, l'IBAN d'un compte ghanéen.
    """
    valeur = F.col("_event.{}".format(name))
    return F.when(F.length(F.trim(valeur)) == 0, F.lit(None).cast("string")) \
            .otherwise(valeur).alias(name)


def parse(stream: StreamSpec, source: DataFrame) -> DataFrame:
    """Aplatit le message JSON en colonnes textuelles et lui attache son motif de rejet."""
    spec = stream.spec
    event = F.from_json(
        F.col("payload"), json_schema(spec),
        {"mode": "PERMISSIVE", "columnNameOfCorruptRecord": CORRUPT_JSON},
    )

    flattened = source.withColumn("_event", event).select(
        F.col("topic"), F.col("partition"), F.col("offset"),
        F.col("kafka_key"), F.col("kafka_timestamp"), F.col("payload"),
        # Le message brut n'est présent ici que s'il n'a pas pu être analysé.
        # La struct elle-même est nulle dans les cas les plus dégénérés — un
        # message vide, par exemple —, d'où le second terme.
        F.coalesce(
            F.col("_event.{}".format(CORRUPT_JSON)),
            F.when(F.col("_event").isNull(), F.col("payload")),
        ).alias(CORRUPT_JSON),
        *[_champ(column.name) for column in spec.all_columns],
    )

    horodatage_absent = (
        F.col(sch.INGESTION_TIMESTAMP).isNull()
        | quality.cast_expression(sch.INGESTION_TIMESTAMP, "TIMESTAMP").isNull()
    )
    rejection = F.coalesce(
        quality.rejection_reason(spec, CORRUPT_JSON, "message JSON illisible"),
        # Traçabilité : une ligne sans horodatage d'ingestion ne peut pas être
        # départagée d'un doublon, ni rattachée à son fichier d'origine.
        F.when(horodatage_absent, F.lit("ingestion_timestamp manquant ou non convertible")),
    )

    return flattened.withColumn(REJECTION, rejection).withColumn(
        DEDUP_KEY,
        F.coalesce(F.col(spec.key), F.sha2(F.coalesce(F.col("payload"), F.lit("")), 256)),
    )


def deduplicate(frame: DataFrame) -> DataFrame:
    """Déduplication sur fenêtre glissante, sans état non borné.

    `dropDuplicatesWithinWatermark` retient les clés vues pendant la durée du
    filigrane et libère l'état au-delà. C'est ce qui distingue l'opération d'un
    `dropDuplicates` classique, dont l'état croîtrait indéfiniment — sur un flux
    continu, cela finit par mettre le job à genoux.

    Elle ne remplace pas le `MERGE` d'Iceberg, qui rattrape les doublons plus
    espacés : les deux jouent à des échelles de temps différentes.
    """
    return frame.withWatermark("kafka_timestamp", DEDUP_WINDOW) \
                .dropDuplicatesWithinWatermark([DEDUP_KEY])


def typed(spec: sch.DatasetSpec, frame: DataFrame) -> DataFrame:
    """Colonnes converties vers leur type cible, dans l'ordre de la table brute."""
    return frame.select(
        *[quality.cast_expression(column.name, column.type).alias(column.name)
          for column in spec.all_columns]
    )


# =============================================================================
# Traitement d'un micro-lot
# =============================================================================


def publish_rejects(stream: StreamSpec, rejects: DataFrame) -> int:
    """Envoie les messages écartés dans la file de rebut, avec leur motif.

    Le message d'origine est conservé intact : c'est ce qui permettra de le
    rejouer après correction, là où un simple journal d'erreur imposerait de le
    reconstituer.
    """
    payload = rejects.select(
        F.lit(stream.dataset).alias("dataset"),
        F.col("topic").alias("source_topic"),
        F.col("partition").alias("source_partition"),
        F.col("offset").alias("source_offset"),
        F.col("kafka_timestamp").alias("received_at"),
        F.col(REJECTION).alias("rejection_reason"),
        F.col(sch.SOURCE_FILE).alias("source_file"),
        F.lit("raw_to_silver").alias("stage"),
        F.col("payload").alias("original_message"),
    ).cache()
    try:
        count = payload.count()
        if count:
            kafka_io.publish(payload, dom.DLQ_TOPIC, key="dataset")
        return count
    finally:
        payload.unpersist()


def process_batch(stream: StreamSpec, batch: DataFrame, batch_id: int) -> None:
    """Double écriture d'un micro-lot : Iceberg d'abord, Kafka ensuite.

    L'ordre n'est pas indifférent. La table Iceberg est fusionnée sur la clé
    naturelle, donc insensible à un rejeu ; le topic Silver ne l'est pas. En
    fusionnant d'abord, un incident entre les deux écritures fait rejouer le
    micro-lot entier sans dupliquer la table — seul le topic reçoit un doublon,
    que le Job 2 dédupliquera à son tour sur sa propre fenêtre.

    La session est prise sur le micro-lot, jamais celle qui a démarré le flux :
    Structured Streaming clone la session à chaque déclenchement, et une vue
    temporaire enregistrée dans l'une reste invisible depuis l'autre. Le cache
    des référentiels, lui, est partagé entre les deux.
    """
    if batch.isEmpty():
        return

    spark = batch.sparkSession
    batch = batch.persist()
    definition = stream.table_definition
    retenus = 0
    try:
        rejetes = publish_rejects(stream, batch.filter(F.col(REJECTION).isNotNull()))

        valides = typed(stream.spec, batch.filter(F.col(REJECTION).isNull()))
        frame = stream.builder(spark, None, source=valides).persist()
        try:
            retenus = frame.count()
            if retenus:
                iceberg_sink.merge_micro_batch(
                    spark, frame, definition.name, layers.SILVER_NAMESPACE,
                    definition.key, definition.partitioning,
                )
                if stream.silver_topic:
                    kafka_io.publish(frame, stream.silver_topic, key="country_code")
        finally:
            frame.unpersist()

        logger.info(
            "%s lot %d — %d validé(s), %d rejeté(s)",
            stream.dataset, batch_id, retenus, rejetes,
        )
    finally:
        batch.unpersist()


# =============================================================================
# Orchestration
# =============================================================================


def start(spark: SparkSession, stream: StreamSpec, once: bool, starting_offsets: str):
    source = kafka_io.read_topics(spark, [stream.raw_topic], starting_offsets)
    prepared = deduplicate(parse(stream, source))

    writer = (
        prepared.writeStream
        .queryName("raw_to_silver_{}".format(stream.dataset))
        .foreachBatch(
            lambda batch, batch_id: process_batch(stream, batch, batch_id)
        )
        .option("checkpointLocation", kafka_io.checkpoint("raw_to_silver/" + stream.dataset))
        # La déduplication est une opération à état : le mode « append » est le
        # seul qui en garantisse la sémantique, une ligne n'étant émise qu'une
        # fois le filigrane franchi.
        .outputMode("append")
    )
    # `availableNow` traite tout ce qui est disponible puis s'arrête : c'est ce
    # qui rend le test de fumée déterministe, là où un déclenchement périodique
    # obligerait à deviner combien de temps attendre.
    writer = writer.trigger(availableNow=True) if once else writer.trigger(processingTime="20 seconds")
    return writer.start()


def run(datasets: Optional[List[str]] = None, once: bool = False,
        starting_offsets: str = "earliest") -> int:
    selected = [BY_DATASET[name] for name in datasets] if datasets else STREAMS
    spark = build_session("waba-raw-to-silver")

    # Les référentiels sont relus à chaque micro-lot par les jointures
    # d'enrichissement. Les garder en mémoire évite de balayer 500 000 clients
    # toutes les vingt secondes ; le prix est qu'un référentiel modifié n'est
    # pris en compte qu'au redémarrage du job, ce qui correspond à leur rythme
    # de mise à jour réel.
    for table in ("customers", "accounts", "branches"):
        layers.read(spark, table).cache().count()

    requetes = [start(spark, stream, once, starting_offsets) for stream in selected]
    logger.info("%d flux démarré(s) : %s", len(requetes),
                ", ".join(s.raw_topic for s in selected))

    try:
        if once:
            for requete in requetes:
                requete.awaitTermination()
        else:
            spark.streams.awaitAnyTermination()
    except KeyboardInterrupt:
        logger.info("arrêt demandé, fermeture des flux")
        for requete in requetes:
            requete.stop()

    echecs = [r for r in requetes if r.exception() is not None]
    for requete in echecs:
        logger.error("%s a échoué : %s", requete.name, requete.exception())

    spark.stop()
    return 1 if echecs else 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m jobs.streaming.raw_to_silver",
        description="Consomme les topics raw-*, alimente les topics silver-* et les "
                    "tables Iceberg silver.*.",
    )
    parser.add_argument("--datasets", nargs="*", choices=sorted(BY_DATASET),
                        help="jeux de données à traiter (défaut : les quatre)")
    parser.add_argument("--once", action="store_true",
                        help="traiter ce qui est disponible puis s'arrêter")
    parser.add_argument("--starting-offsets", default="earliest",
                        choices=["earliest", "latest"],
                        help="position de lecture au premier démarrage (défaut : earliest)")
    return parser


def main(argv: Optional[List[str]] = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s — %(message)s")
    args = build_parser().parse_args(argv)
    return run(datasets=args.datasets, once=args.once,
               starting_offsets=args.starting_offsets)


if __name__ == "__main__":
    sys.exit(main())
