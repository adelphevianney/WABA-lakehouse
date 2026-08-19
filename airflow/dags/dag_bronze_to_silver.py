"""Transformations Bronze vers Silver (§2.1).

Déclenché par la mise à jour de la couche brute, et non par une planification :
recalculer Silver quand rien n'a été ingéré ne produirait aucun changement tout
en consommant un conteneur Spark.

Les référentiels sont traités avant les flux, parce que ces derniers s'y
joignent pour s'enrichir. La dépendance est explicite plutôt qu'implicite dans
l'ordre d'un script : elle est ainsi visible dans le graphe, et une reprise
partielle respecte l'ordre.
"""

from __future__ import annotations

import pendulum
from airflow.decorators import dag

from waba_common import (
    COUNTRY_PARAM,
    WINDOW_PARAM,
    DEFAULT_ARGS,
    LAKEHOUSE_RAW,
    LAKEHOUSE_SILVER,
    spark_job,
)


@dag(
    dag_id="dag_bronze_to_silver",
    description="Nettoie, convertit en euros et enrichit la couche brute",
    schedule=[LAKEHOUSE_RAW],
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    params={"countries": COUNTRY_PARAM, "window_days": WINDOW_PARAM},
    tags=["waba", "level2", "silver"],
)
def bronze_to_silver():
    referentiels = spark_job(
        task_id="referentiels",
        module="jobs.batch.silver",
        extra_args="--tables customers accounts",
    )

    flux = spark_job(
        task_id="flux_transactionnels",
        module="jobs.batch.silver",
        extra_args=(
            "--tables bank_transactions insurance_operations "
            "mobile_money_payments loan_repayments"
        ),
        outlets=[LAKEHOUSE_SILVER],
    )

    referentiels >> flux


bronze_to_silver()
