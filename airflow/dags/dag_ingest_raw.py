"""Ingestion de la zone d'atterrissage vers la couche brute (§2.1).

Premier maillon de la chaîne : détecte les fichiers déposés dans MinIO et les
charge dans les tables Iceberg `raw.*`.

L'énoncé propose un déclenchement sur détection de nouveaux fichiers ou une
planification régulière. La seconde option est retenue : une détection par
capteur maintiendrait un emplacement de tâche occupé en attente, alors que le
job sait déjà ne rien faire quand la zone est vide, et que son idempotence rend
un passage à vide sans conséquence. Un quart d'heure est un compromis entre
fraîcheur de la donnée et nombre d'exécutions.
"""

from __future__ import annotations

import pendulum
from airflow.decorators import dag

from waba_common import COUNTRY_PARAM, DEFAULT_ARGS, LAKEHOUSE_RAW, spark_job


@dag(
    dag_id="dag_ingest_raw",
    description="Charge les fichiers de raw-landing vers les tables Iceberg raw.*",
    schedule="*/15 * * * *",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    # Une seule exécution à la fois : deux ingestions concurrentes sur les mêmes
    # partitions se disputeraient l'écriture Iceberg et l'une échouerait sur un
    # conflit de commit.
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    params={"countries": COUNTRY_PARAM},
    tags=["waba", "level2", "bronze"],
)
def ingest_raw():
    ingestion = spark_job(
        task_id="ingerer_zone_atterrissage",
        module="jobs.batch.ingest_raw",
        outlets=[LAKEHOUSE_RAW],
    )

    # La compaction suit l'ingestion mais lui reste distincte : elle entretient
    # le stockage sans produire de donnée. Chaque passage écrit de nouveaux
    # fichiers dans les partitions touchées sans réécrire les précédents, et
    # sans cette étape le nombre de fichiers croît indéfiniment.
    compaction = spark_job(
        task_id="compacter_couche_brute",
        module="jobs.batch.compact",
        trigger_rule="all_success",
    )

    ingestion >> compaction


ingest_raw()
