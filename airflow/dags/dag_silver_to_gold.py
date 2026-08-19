"""Calcul des KPIs financiers et réglementaires (§2.1).

Déclenché par la mise à jour de Silver. Les sept KPIs sont répartis en trois
tâches par domaine métier — banque, assurance, mobile money — plutôt qu'en une
tâche unique : l'échec d'un indicateur assurance ne doit pas priver les équipes
bancaires de leurs chiffres, et le graphe montre alors précisément ce qui a
échoué.

Les trois tâches sont indépendantes et s'exécutent en parallèle, chacune ne
lisant que les tables Silver de son domaine.
"""

from __future__ import annotations

import pendulum
from airflow.decorators import dag

from waba_common import (
    COUNTRY_PARAM,
    WINDOW_PARAM,
    DEFAULT_ARGS,
    LAKEHOUSE_GOLD,
    LAKEHOUSE_SILVER,
    spark_job,
)


@dag(
    dag_id="dag_silver_to_gold",
    description="Calcule les 7 tables de KPIs gold.* depuis la couche Silver",
    schedule=[LAKEHOUSE_SILVER],
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    params={"countries": COUNTRY_PARAM, "window_days": WINDOW_PARAM},
    tags=["waba", "level2", "gold"],
)
def silver_to_gold():
    spark_job(
        task_id="kpis_bancaires",
        module="jobs.batch.gold",
        extra_args=(
            "--tables daily_transaction_volume npl_ratio_by_country "
            "customer_arpu_monthly"
        ),
        outlets=[LAKEHOUSE_GOLD],
    )

    spark_job(
        task_id="kpis_assurance",
        module="jobs.batch.gold",
        extra_args="--tables loss_ratio_by_product claims_processing_time",
        outlets=[LAKEHOUSE_GOLD],
    )

    spark_job(
        task_id="kpis_mobile_money",
        module="jobs.batch.gold",
        extra_args="--tables mobile_money_daily_flow cross_border_transfers",
        outlets=[LAKEHOUSE_GOLD],
    )


silver_to_gold()
