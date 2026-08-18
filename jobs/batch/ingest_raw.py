"""Ingestion de la zone d'atterrissage vers les tables Iceberg `raw.*` (Level 1.3).

Un seul job, piloté par les `DatasetSpec`, alimente les huit tables. Écrire un
job par table aurait multiplié les occasions de divergence entre schéma déclaré
et schéma écrit, pour un gain nul : les huit traitements ne diffèrent que par
leur schéma et leur clé.

L'idempotence — critère d'évaluation explicite — repose sur un `MERGE INTO` sur
la clé naturelle. Deux précautions la rendent effective :

* la source est **dédupliquée sur la clé avant le MERGE**. Iceberg refuse qu'une
  ligne cible soit appariée plusieurs fois, et sans cette déduplication un
  doublon interne au lot serait inséré deux fois puisque aucune des deux lignes
  ne correspond encore à la cible ;
* le prédicat d'appariement inclut `country_code`, qui est une colonne de
  partition. Sans lui, chaque MERGE balaierait l'intégralité de la table au lieu
  des seules partitions concernées.

Exemples :
    python -m jobs.batch.ingest_raw
    python -m jobs.batch.ingest_raw --datasets bank_txn --countries CI SN
    python -m jobs.batch.ingest_raw --no-archive
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
import time
from dataclasses import dataclass
from typing import Dict, List, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType
from pyspark.sql.window import Window

from jobs.batch import landing, layers, quality
from jobs.batch import schemas as sch
from jobs.batch.session import RAW_NAMESPACE, build_session, ensure_namespace, table_name

logger = logging.getLogger("jobs.ingest_raw")


@dataclass
class DatasetOutcome:
    """Compte rendu d'ingestion d'un jeu de données."""

    dataset: str
    files: int = 0
    rows_read: int = 0
    rows_rejected: int = 0
    rows_before: int = 0
    rows_after: int = 0
    archived: int = 0
    error: Optional[str] = None

    @property
    def inserted(self) -> int:
        return self.rows_after - self.rows_before

    @property
    def ok(self) -> bool:
        return self.error is None


def _reading_schema(spec: sch.DatasetSpec) -> StructType:
    """Schéma de lecture : tout en texte, plus la colonne des lignes illisibles.

    Lire d'abord en texte est délibéré. Laisser Spark convertir à la lecture
    transformerait toute valeur invalide en null, sans distinction possible
    entre une donnée absente et une donnée présente mais corrompue — or
    l'énoncé demande de rejeter les secondes, pas de les ingérer vides.
    """
    fields = [StructField(column.name, StringType(), True) for column in spec.columns]
    fields.append(StructField(sch.CORRUPT_RECORD, StringType(), True))
    return StructType(fields)


def _read_landing(spark: SparkSession, spec: sch.DatasetSpec, uris: List[str]) -> DataFrame:
    return (
        spark.read
        .option("header", "true")
        # `enforceSchema=false` fait vérifier à Spark que l'en-tête du fichier
        # correspond au schéma. Avec la valeur par défaut, les colonnes sont
        # appariées par position et un fichier réordonné serait ingéré de
        # travers, silencieusement.
        .option("enforceSchema", "false")
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", sch.CORRUPT_RECORD)
        .option("timestampFormat", "yyyy-MM-dd'T'HH:mm:ss")
        .schema(_reading_schema(spec))
        .csv(uris)
    )


def _typed_projection(spec: sch.DatasetSpec) -> List:
    """Colonnes converties vers leur type cible, dans l'ordre de la table."""
    projection = [
        quality.cast_expression(column.name, column.type).alias(column.name)
        for column in spec.columns
    ]
    projection.append(F.col(sch.INGESTION_TIMESTAMP))
    projection.append(F.col(sch.SOURCE_FILE))
    return projection


def _deduplicate(frame: DataFrame, key: str) -> DataFrame:
    """Une seule ligne par clé naturelle, choisie de façon déterministe.

    `dropDuplicates` conviendrait fonctionnellement mais laisserait le choix de
    la ligne conservée à l'ordre d'exécution : deux exécutions du même lot
    pourraient retenir des lignes différentes.
    """
    ranked = Window.partitionBy(key).orderBy(F.col(sch.SOURCE_FILE), F.col(key))
    return (
        frame.withColumn("_rank", F.row_number().over(ranked))
        .filter(F.col("_rank") == 1)
        .drop("_rank")
    )


def _write_rejects(
    spark: SparkSession, spec: sch.DatasetSpec, rejected: DataFrame, rejects_table: str
) -> None:
    """Conserve les lignes écartées avec leur motif.

    Les rejeter sans les garder rendrait tout écart de volumétrie entre la
    source et la cible impossible à expliquer.
    """
    payload = rejected.select(
        F.lit(spec.name).alias("dataset"),
        F.col(sch.SOURCE_FILE).alias("source_file"),
        F.col(sch.INGESTION_TIMESTAMP).alias("ingestion_timestamp"),
        F.col("_reject_reason").alias("reject_reason"),
        F.concat_ws(
            " | ", *[F.coalesce(F.col(c.name), F.lit("")) for c in spec.columns]
        ).alias("raw_record"),
    )
    payload.writeTo(rejects_table).append()


#: Nombre maximal de fichiers lus en une passe. Une zone d'atterrissage
#: alimentée en continu peut en accumuler des milliers ; les charger d'un bloc
#: fait exploser le driver, qui doit tenir en mémoire le plan et les statistiques
#: de chaque fichier. Traiter par lots bornés rend le coût prévisible quel que
#: soit le retard accumulé.
DEFAULT_BATCH_SIZE = 150

#: Délai de grâce avant archivage, en minutes.
#:
#: Au Level 3, NiFi surveille la même zone d'atterrissage que le job batch et la
#: recense toutes les 30 secondes. Archiver un fichier dès son ingestion crée une
#: course : archivé avant recensement, il disparaît du chemin streaming sans
#: laisser de trace ; archivé entre le recensement et le téléchargement, il
#: produit un rejet qui n'en est pas un.
#:
#: Laisser mûrir les fichiers supprime la course sans renoncer à l'archivage
#: exigé au §1.2. Le coût est nul aux niveaux inférieurs : les fichiers sont
#: simplement archivés à la passe suivante, et les réingérer entre-temps est
#: sans effet grâce au MERGE.
DEFAULT_ARCHIVE_AFTER_MINUTES = int(os.getenv("WABA_ARCHIVE_AFTER_MINUTES", "10"))


def _chunks(items: List[str], size: int):
    for start in range(0, len(items), size):
        yield items[start:start + size]


def ingest_dataset(
    spark: SparkSession,
    spec: sch.DatasetSpec,
    keys: List[str],
    raw_bucket: str,
    rejects_table: str,
    batch_size: int = DEFAULT_BATCH_SIZE,
) -> DatasetOutcome:
    """Ingère tous les fichiers en attente d'un jeu de données, par lots bornés."""
    outcome = DatasetOutcome(dataset=spec.name, files=len(keys))
    target = table_name(spec.table)

    spark.sql(sch.create_table_ddl(spec, target))
    # Une colonne ajoutée au schéma du dépôt doit apparaître dans une table déjà
    # créée, faute de quoi l'écriture échouerait sur un schéma incompatible.
    ajoutees = layers.evolve_schema(
        spark, target, [(c.name, c.type) for c in spec.all_columns]
    )
    if ajoutees:
        logger.info("%s : colonne(s) ajoutée(s) au schéma — %s",
                    spec.table, ", ".join(ajoutees))

    # Les comptages encadrent l'ensemble des lots : les répéter à chaque lot
    # coûterait un balayage complet de la table par passe.
    outcome.rows_before = spark.table(target).count()
    for batch, chunk in enumerate(_chunks(keys, batch_size), start=1):
        if len(keys) > batch_size:
            logger.info(
                "%s : lot %d/%d (%d fichiers)",
                spec.name, batch, -(-len(keys) // batch_size), len(chunk),
            )
        _ingest_chunk(spark, spec, chunk, raw_bucket, rejects_table, target, outcome)
    outcome.rows_after = spark.table(target).count()
    return outcome


def _ingest_chunk(
    spark: SparkSession,
    spec: sch.DatasetSpec,
    keys: List[str],
    raw_bucket: str,
    rejects_table: str,
    target: str,
    outcome: DatasetOutcome,
) -> None:
    """Lit, valide et fusionne un lot de fichiers."""
    raw = _read_landing(spark, spec, landing.to_uris(raw_bucket, keys))
    raw = (
        raw.withColumn(sch.SOURCE_FILE, F.input_file_name())
        .withColumn(sch.INGESTION_TIMESTAMP, F.current_timestamp())
        .withColumn("_reject_reason", quality.rejection_reason(spec))
        # Le lot est relu plusieurs fois — comptages, rejets, MERGE — et sans
        # cache Spark rejouerait la lecture S3 à chaque action.
        .cache()
    )

    outcome.rows_read += raw.count()
    rejected = raw.filter(F.col("_reject_reason").isNotNull())
    rejected_count = rejected.count()
    if rejected_count:
        outcome.rows_rejected += rejected_count
        logger.warning("%s : %d ligne(s) rejetée(s) dans ce lot", spec.name, rejected_count)
        _write_rejects(spark, spec, rejected, rejects_table)

    valid = (
        raw.filter(F.col("_reject_reason").isNull())
        .select(*_typed_projection(spec))
    )
    valid = _deduplicate(valid, spec.key)

    view = "source_{}".format(spec.name)
    valid.createOrReplaceTempView(view)
    spark.sql(
        "MERGE INTO {target} AS t\n"
        "USING {view} AS s\n"
        # `country_code` est une colonne de partition : l'inclure dans le
        # prédicat permet à Iceberg d'élaguer les partitions au lieu de balayer
        # toute la table à chaque lot.
        "  ON t.{key} = s.{key} AND t.country_code = s.country_code\n"
        "WHEN NOT MATCHED THEN INSERT *".format(target=target, view=view, key=spec.key)
    )
    raw.unpersist()


def run(
    datasets: Optional[List[str]] = None,
    countries: Optional[List[str]] = None,
    archive: bool = True,
    batch_size: int = DEFAULT_BATCH_SIZE,
    archive_after_minutes: int = DEFAULT_ARCHIVE_AFTER_MINUTES,
) -> List[DatasetOutcome]:
    raw_bucket = os.getenv("BUCKET_RAW", "raw-landing")
    archive_bucket = os.getenv("BUCKET_ARCHIVE", "archive")

    client = landing.s3_client()
    pending = landing.list_pending(client, raw_bucket, datasets, countries)
    if not pending:
        logger.info("aucun fichier en attente dans s3://%s", raw_bucket)
        return []

    spark = build_session("waba-ingest-raw")
    ensure_namespace(spark)
    rejects_table = table_name(sch.REJECTS_TABLE)
    spark.sql(sch.create_rejects_ddl(rejects_table))

    outcomes: List[DatasetOutcome] = []
    # Les référentiels sont traités en premier : `SPECS` respecte l'ordre de
    # dépendance rappelé par l'annexe A.8.
    for spec in sch.SPECS:
        keys = pending.get(spec.name)
        if not keys:
            continue

        started = time.perf_counter()
        try:
            outcome = ingest_dataset(
                spark, spec, keys, raw_bucket, rejects_table, batch_size
            )
        except Exception as exc:  # noqa: BLE001 — un jeu en échec ne doit pas
            # interrompre les autres, et les fichiers concernés ne seront pas
            # archivés : ils repasseront à la prochaine exécution.
            logger.exception("échec de l'ingestion de %s", spec.name)
            outcomes.append(DatasetOutcome(dataset=spec.name, files=len(keys), error=str(exc)))
            continue

        # Les référentiels ne sont pas archivés. Ce ne sont pas des flux
        # consommés une fois mais des données de référence partagées : les
        # retirer de la zone d'atterrissage priverait le générateur des clés
        # qu'il doit continuer à référencer, et la génération suivante
        # produirait des transactions pointant vers des comptes inexistants.
        # Les réingérer à chaque passe est sans effet grâce au MERGE.
        if archive and not spec.is_referential:
            # Délai de grâce : un fichier tout juste déposé peut ne pas encore
            # avoir été recensé par NiFi, qui surveille la même zone. Il sera
            # archivé à la passe suivante — le réingérer entre-temps est sans
            # effet grâce au MERGE.
            murs = landing.older_than(client, raw_bucket, keys, archive_after_minutes)
            en_attente = len(keys) - len(murs)
            moved, failed = landing.archive(client, raw_bucket, archive_bucket, murs)
            outcome.archived = moved
            if en_attente:
                logger.info("%s : %d fichier(s) laissé(s) au chemin streaming (délai de %d min)",
                            spec.name, en_attente, archive_after_minutes)
            if failed:
                logger.warning("%s : %d fichier(s) non archivé(s)", spec.name, failed)

        logger.info(
            "%s : %d fichier(s), %d ligne(s) lues, %d rejetée(s), %d insérée(s) en %.1f s",
            spec.name, outcome.files, outcome.rows_read, outcome.rows_rejected,
            outcome.inserted, time.perf_counter() - started,
        )
        outcomes.append(outcome)

    spark.stop()
    return outcomes


def _render(outcomes: List[DatasetOutcome]) -> None:
    header = "{:<16} {:>7} {:>10} {:>9} {:>10} {:>9}".format(
        "jeu de données", "fichiers", "lues", "rejetées", "insérées", "archivés"
    )
    print("\n" + header)
    print("-" * len(header))
    for outcome in outcomes:
        if not outcome.ok:
            print("{:<16} ÉCHEC — {}".format(outcome.dataset, outcome.error))
            continue
        print("{:<16} {:>7} {:>10,} {:>9} {:>10,} {:>9}".format(
            outcome.dataset, outcome.files, outcome.rows_read,
            outcome.rows_rejected, outcome.inserted, outcome.archived,
        ))
    print()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m jobs.batch.ingest_raw",
        description="Ingère la zone d'atterrissage vers les tables Iceberg raw.*",
    )
    parser.add_argument("--datasets", nargs="*", choices=sorted(sch.BY_NAME),
                        help="jeux de données à traiter (défaut : tous)")
    parser.add_argument("--countries", nargs="*", metavar="CC",
                        help="restreindre à certains pays (sans effet sur les référentiels)")
    parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE,
                        help="nombre de fichiers lus par passe (défaut : %(default)s). "
                             "Borne la mémoire du driver quel que soit le retard accumulé "
                             "dans la zone d'atterrissage.")
    parser.add_argument("--archive-after", type=int, metavar="MINUTES",
                        default=DEFAULT_ARCHIVE_AFTER_MINUTES,
                        help="délai de grâce avant archivage (défaut : %(default)s min). "
                             "Au Level 3, NiFi surveille la même zone d'atterrissage : "
                             "archiver un fichier avant qu'il ne l'ait recensé le ferait "
                             "disparaître du chemin streaming sans laisser de trace.")
    parser.add_argument("--no-archive", action="store_true",
                        help="ne pas archiver du tout. Utile pour rejouer une ingestion "
                             "sur les mêmes fichiers pendant une mise au point.")
    return parser


def main(argv: Optional[List[str]] = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s — %(message)s")
    args = build_parser().parse_args(argv)

    outcomes = run(
        datasets=args.datasets,
        countries=args.countries,
        archive=not args.no_archive,
        batch_size=args.batch_size,
        archive_after_minutes=args.archive_after,
    )
    if not outcomes:
        print("\nAucun fichier à ingérer.\n")
        return 0

    _render(outcomes)
    return 0 if all(outcome.ok for outcome in outcomes) else 1


if __name__ == "__main__":
    sys.exit(main())
