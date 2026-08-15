"""Reporting réglementaire BCEAO et CIMA (§2.1).

Planifié quotidiennement à 00h30 UTC pour la journée écoulée, conformément à
l'énoncé. Le décalage d'une demi-heure après minuit n'est pas arbitraire : il
laisse le temps aux dernières ingestions de la journée d'être traitées et
propagées jusqu'à Gold avant que le reporting ne fige ses chiffres.

Airflow n'exécute une période qu'une fois celle-ci révolue : le déclenchement du
31 mars à 00h30 porte donc sur la journée du 30 mars. C'est exactement ce que
désigne `{{ ds }}`, et c'est pourquoi le job reçoit cette date plutôt que celle
du jour — une confusion entre les deux décalerait toute la déclaration d'un jour.

Le rattrapage automatique est **désactivé**, et c'est un arbitrage assumé. Une
déclaration réglementaire manquée reste due : la logique voudrait `catchup=True`.
Mais avec une date de départ antérieure de plusieurs mois, activer le rattrapage
met en file une exécution par journée écoulée dès le premier démarrage — plus de
cent trente ici, chacune lançant un conteneur Spark. La plateforme se noie avant
d'avoir produit le moindre rapport.

Le rattrapage se fait donc explicitement, sur la période voulue :

    airflow dags backfill dag_regulatory_report --start-date 2026-03-29 --end-date 2026-03-31

C'est aussi ce qu'un exploitant fait en pratique, plutôt que de laisser
l'ordonnanceur décider seul de l'ampleur d'une reprise. En production, la date de
départ serait celle de la mise en service et le rattrapage automatique
redeviendrait sans danger.
"""

from __future__ import annotations

import pendulum
from airflow.decorators import dag

from waba_common import COUNTRY_PARAM, DEFAULT_ARGS, spark_job


@dag(
    dag_id="dag_regulatory_report",
    description="Agrégats réglementaires BCEAO et CIMA, produits à J+1 00h30 UTC",
    schedule="30 0 * * *",
    start_date=pendulum.datetime(2026, 3, 29, tz="UTC"),
    # Voir l'explication en tête de module : le rattrapage est explicite.
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    params={"countries": COUNTRY_PARAM},
    tags=["waba", "level2", "reglementaire"],
)
def regulatory_report():
    spark_job(
        task_id="rapport_reglementaire",
        module="jobs.batch.regulatory",
        # `{{ ds }}` est la date de la période traitée, soit la veille du
        # déclenchement — exactement le J+1 attendu par l'énoncé.
        extra_args="--report-date {{ ds }}",
    )


regulatory_report()
