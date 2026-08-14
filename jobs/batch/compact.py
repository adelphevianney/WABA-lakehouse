"""Compaction des tables Iceberg de la couche brute.

Une table alimentée par `MERGE INTO` accumule mécaniquement de petits fichiers :
chaque exécution écrit de nouveaux fichiers dans les partitions qu'elle touche,
sans jamais réécrire les précédents. Après quelques dizaines de passes, une
partition contient des dizaines de fichiers de quelques kilo-octets.

Le coût n'est pas théorique. Mesuré sur ce projet, une table morcelée occupait
**308 octets par ligne contre 29** pour la même donnée écrite en fichiers pleins,
soit un facteur dix. Trois mécanismes se cumulent :

* Parquet compresse par blocs et construit des dictionnaires par colonne. Sur
  quelques dizaines de lignes, l'en-tête et les statistiques pèsent plus lourd
  que les données qu'ils décrivent.
* Sur un stockage objet, ouvrir un fichier est une requête HTTP. Lire mille
  fichiers d'un kilo-octet coûte mille allers-retours réseau, là où la latence
  domine totalement le temps de transfert.
* Iceberg référence chaque fichier dans ses manifestes, que le moteur lit à la
  planification. Le coût de planification croît avec le nombre de fichiers,
  avant même d'avoir lu une ligne.

Ce job est volontairement séparé de l'ingestion : produire de la donnée et
entretenir le stockage sont deux responsabilités distinctes, avec des rythmes
distincts. Au Level 2, il deviendra un DAG de maintenance.

Exemples :
    python -m jobs.batch.compact
    python -m jobs.batch.compact --tables bank_transactions --target-size-mb 64
"""

from __future__ import annotations

import argparse
import logging
import sys
import time
from dataclasses import dataclass
from typing import List, Optional

from pyspark.sql import SparkSession

from common import domain as dom
from jobs.batch.session import build_session, table_name

logger = logging.getLogger("jobs.compact")

#: Taille de fichier visée. En production on parle de centaines de méga-octets ;
#: à l'échelle de ce projet, viser 128 Mo reviendrait à produire un fichier
#: unique par table et à supprimer tout intérêt au partitionnement.
DEFAULT_TARGET_MB = 32

#: En deçà de ce nombre de fichiers, une partition n'est pas réécrite : la
#: réécriture coûte une lecture et une écriture complètes, qu'il est inutile de
#: payer pour fusionner deux fichiers déjà corrects.
DEFAULT_MIN_INPUT_FILES = 2


@dataclass
class CompactionOutcome:
    table: str
    files_before: int = 0
    files_after: int = 0
    bytes_before: int = 0
    bytes_after: int = 0
    rows: int = 0
    error: Optional[str] = None

    @property
    def ok(self) -> bool:
        return self.error is None

    @property
    def bytes_per_row_before(self) -> float:
        return self.bytes_before / self.rows if self.rows else 0.0

    @property
    def bytes_per_row_after(self) -> float:
        return self.bytes_after / self.rows if self.rows else 0.0


def _file_stats(spark: SparkSession, table: str) -> tuple:
    """Nombre de fichiers, octets et lignes, lus dans les métadonnées Iceberg.

    La table `$files` évite de scanner les données : ces chiffres proviennent
    des manifestes, pas des fichiers eux-mêmes.
    """
    row = spark.sql(
        "SELECT count(*) AS files, "
        "       coalesce(sum(file_size_in_bytes), 0) AS bytes, "
        "       coalesce(sum(record_count), 0) AS rows "
        f"FROM {table}.files"
    ).collect()[0]
    return int(row["files"]), int(row["bytes"]), int(row["rows"])


def compact_table(
    spark: SparkSession,
    table: str,
    target_mb: int = DEFAULT_TARGET_MB,
    min_input_files: int = DEFAULT_MIN_INPUT_FILES,
    rewrite_manifests: bool = True,
) -> CompactionOutcome:
    """Fusionne les petits fichiers d'une table, partition par partition."""
    identifier = table_name(table)
    outcome = CompactionOutcome(table=table)

    outcome.files_before, outcome.bytes_before, outcome.rows = _file_stats(spark, identifier)
    if outcome.files_before == 0:
        outcome.files_after = 0
        return outcome

    # `rewrite_data_files` réécrit les données dans un nouvel instantané, de
    # façon atomique : les lecteurs en cours continuent de voir l'ancien état et
    # bascule sur le nouveau à la fin, sans interruption ni verrou.
    spark.sql(
        "CALL iceberg.system.rewrite_data_files("
        f"  table => '{table_name(table).split('.', 1)[1]}',"
        "  options => map("
        f"    'min-input-files', '{min_input_files}',"
        f"    'target-file-size-bytes', '{target_mb * 1024 * 1024}'"
        "  )"
        ")"
    )

    if rewrite_manifests:
        # Les manifestes se fragmentent eux aussi au fil des instantanés ; les
        # réécrire réduit le coût de planification des requêtes.
        try:
            spark.sql(
                "CALL iceberg.system.rewrite_manifests("
                f"table => '{table_name(table).split('.', 1)[1]}')"
            )
        except Exception as exc:  # noqa: BLE001 — accessoire, ne doit pas faire échouer
            logger.warning("réécriture des manifestes ignorée pour %s : %s", table, exc)

    outcome.files_after, outcome.bytes_after, _ = _file_stats(spark, identifier)
    return outcome


def run(
    tables: Optional[List[str]] = None,
    target_mb: int = DEFAULT_TARGET_MB,
    min_input_files: int = DEFAULT_MIN_INPUT_FILES,
) -> List[CompactionOutcome]:
    targets = tables or list(dom.RAW_TABLES)
    spark = build_session("waba-compact")

    outcomes: List[CompactionOutcome] = []
    for table in targets:
        started = time.perf_counter()
        try:
            outcome = compact_table(spark, table, target_mb, min_input_files)
        except Exception as exc:  # noqa: BLE001 — une table en échec n'arrête pas les autres
            logger.exception("échec de la compaction de %s", table)
            outcomes.append(CompactionOutcome(table=table, error=str(exc)))
            continue

        if outcome.files_before != outcome.files_after:
            logger.info(
                "%s : %d fichiers -> %d en %.1f s",
                table, outcome.files_before, outcome.files_after,
                time.perf_counter() - started,
            )
        outcomes.append(outcome)

    spark.stop()
    return outcomes


def _render(outcomes: List[CompactionOutcome]) -> None:
    header = "{:<24} {:>9} {:>8} {:>12} {:>12}".format(
        "table", "fichiers", "après", "octets/ligne", "après"
    )
    print("\n" + header)
    print("-" * len(header))
    for outcome in outcomes:
        if not outcome.ok:
            print("{:<24} ÉCHEC — {}".format(outcome.table, outcome.error))
            continue
        print("{:<24} {:>9} {:>8} {:>12.0f} {:>12.0f}".format(
            outcome.table, outcome.files_before, outcome.files_after,
            outcome.bytes_per_row_before, outcome.bytes_per_row_after,
        ))

    gained = sum(o.files_before - o.files_after for o in outcomes if o.ok)
    print(f"\n{gained} fichier(s) éliminé(s) par fusion\n")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m jobs.batch.compact",
        description="Fusionne les petits fichiers Parquet des tables Iceberg raw.*",
    )
    parser.add_argument("--tables", nargs="*", choices=sorted(dom.RAW_TABLES),
                        help="tables à compacter (défaut : les 8)")
    parser.add_argument("--target-size-mb", type=int, default=DEFAULT_TARGET_MB,
                        help="taille de fichier visée en Mo (défaut : %(default)s)")
    parser.add_argument("--min-input-files", type=int, default=DEFAULT_MIN_INPUT_FILES,
                        help="nombre de fichiers à partir duquel une partition est "
                             "réécrite (défaut : %(default)s)")
    return parser


def main(argv: Optional[List[str]] = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s — %(message)s")
    args = build_parser().parse_args(argv)

    outcomes = run(
        tables=args.tables,
        target_mb=args.target_size_mb,
        min_input_files=args.min_input_files,
    )
    _render(outcomes)
    return 0 if all(outcome.ok for outcome in outcomes) else 1


if __name__ == "__main__":
    sys.exit(main())
