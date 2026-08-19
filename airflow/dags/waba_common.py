"""Socle commun des DAGs de la plateforme WABA.

Regroupe ce que les quatre DAGs partagent : lancement d'un job Spark, gestion
des reprises, alerte sur échec, et paramétrage par pays.

**Aucun identifiant n'apparaît dans le code des DAGs.** Les accès à MinIO sont
déclarés comme une *Connection* Airflow et référencés par des expressions Jinja
résolues à l'exécution de la tâche. Les résoudre au moment de l'analyse du DAG
les inscrirait dans le code compilé et dans les journaux de l'ordonnanceur, qui
relit chaque fichier toutes les quelques secondes.

Les jobs Spark ne s'exécutent pas dans Airflow : chaque tâche démarre un
conteneur `waba/spark` éphémère. Ce découplage est celui que le Level 4
reprendra en remplaçant `DockerOperator` par `SparkKubernetesOperator` — même
principe, l'ordonnanceur soumet un travail à un exécutant externe au lieu de le
porter lui-même.
"""

from __future__ import annotations

import logging
from datetime import timedelta
from typing import Any, Dict

from airflow.datasets import Dataset
from airflow.models.param import Param
from airflow.providers.docker.operators.docker import DockerOperator

logger = logging.getLogger(__name__)

# =============================================================================
# Jeux de données : dépendances déclaratives entre DAGs
# =============================================================================
# Chaîner les DAGs par des jeux de données plutôt que par des déclenchements
# explicites décrit ce que chacun produit et consomme, au lieu de coder en dur
# qui appelle qui. Ajouter demain un DAG consommant Silver ne demandera aucune
# modification du DAG amont.

LAKEHOUSE_RAW = Dataset("iceberg://waba/raw")
LAKEHOUSE_SILVER = Dataset("iceberg://waba/silver")
LAKEHOUSE_GOLD = Dataset("iceberg://waba/gold")

#: Réseau Compose sur lequel les conteneurs Spark doivent être attachés pour
#: joindre MinIO, le catalogue Iceberg et Trino par leur nom de service.
NETWORK = "waba"

#: Restriction géographique, transmise à chaque job. Vide, elle laisse le job
#: traiter les huit pays ; renseignée, elle permet un retraitement sélectif —
#: rejouer le seul Sénégal après une correction sans toucher au reste.
COUNTRY_PARAM = Param(
    default=[],
    type="array",
    title="Pays à traiter",
    description="Codes ISO à retraiter. Vide, tous les pays du groupe sont traités.",
)

_COUNTRY_FLAG = "{% if params.countries %}--countries {{ params.countries | join(' ') }}{% endif %}"


def alerte_echec(context: Dict[str, Any]) -> None:
    """Trace un échec de tâche sous une forme exploitable.

    L'énoncé demande des alertes en cas d'échec. Le canal reste ici le journal
    applicatif : au Level 4, Promtail collectera ces lignes vers Loki et Grafana
    déclenchera l'alerte. Écrire dès maintenant un message structuré évite d'avoir
    à reprendre les DAGs à ce moment-là.
    """
    instance = context.get("task_instance")
    logger.error(
        "ALERTE WABA — dag=%s tache=%s execution=%s tentative=%s/%s erreur=%s",
        context.get("dag").dag_id if context.get("dag") else "?",
        instance.task_id if instance else "?",
        context.get("logical_date"),
        instance.try_number if instance else "?",
        instance.max_tries if instance else "?",
        context.get("exception"),
    )


DEFAULT_ARGS: Dict[str, Any] = {
    "owner": "waba-data-engineering",
    # Deux reprises espacées : les échecs rencontrés en pratique sont
    # transitoires — catalogue REST momentanément indisponible, conteneur Spark
    # en cours de démarrage — et se résolvent d'eux-mêmes.
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "on_failure_callback": alerte_echec,
    "depends_on_past": False,
}


#: Pool à un seul emplacement, partagé par toutes les tâches Spark.
#:
#: Le catalogue Iceberg de ce déploiement persiste ses métadonnées dans un
#: SQLite, qui n'accepte qu'un écrivain à la fois. `max_active_runs` sérialise
#: les exécutions d'un même DAG, mais deux DAGs distincts peuvent commiter
#: simultanément — et un verrou dans le pilote Spark n'y peut rien, chaque tâche
#: tournant dans son propre conteneur. Le pool est le seul endroit où la
#: contrainte s'exprime à l'échelle de l'ordonnanceur.
#:
#: La conséquence est assumée : les tâches Spark s'exécutent en série. Sur cette
#: machine elles le faisaient déjà de fait, chaque conteneur réclamant 2 Go. Un
#: catalogue adossé à PostgreSQL accepterait les écritures concurrentes et
#: rendrait ce pool inutile — c'est le déploiement que vise le Level 4.
CATALOG_POOL = "waba_catalogue"


def spark_job(task_id: str, module: str, extra_args: str = "", **kwargs: Any) -> DockerOperator:
    """Tâche exécutant un module de `jobs.batch` dans un conteneur Spark éphémère."""
    command = " ".join(part for part in ("python3 -m", module, _COUNTRY_FLAG, extra_args) if part)
    kwargs.setdefault("pool", CATALOG_POOL)

    return DockerOperator(
        task_id=task_id,
        image="{{ var.value.waba_spark_image }}",
        command=command,
        network_mode=NETWORK,
        # Les identifiants proviennent de la Connection Airflow, résolue à
        # l'exécution : le code du DAG n'en contient aucun.
        environment={
            "AWS_ACCESS_KEY_ID": "{{ conn.waba_minio.login }}",
            "AWS_SECRET_ACCESS_KEY": "{{ conn.waba_minio.password }}",
            # MinIO ignore la région, mais le SDK AWS refuse de s'initialiser
            # sans elle : sans cette variable, tout accès Iceberg échoue sur
            # « Unable to load region from any of the providers in the chain ».
            "AWS_REGION": "us-east-1",
            "MINIO_ENDPOINT": "{{ var.value.waba_minio_endpoint }}",
            "ICEBERG_REST_URI": "{{ var.value.waba_iceberg_rest_uri }}",
            "ICEBERG_WAREHOUSE": "s3://lakehouse/",
            "BUCKET_RAW": "raw-landing",
            "BUCKET_ARCHIVE": "archive",
            # Taux de change et clé de pseudonymisation doivent être identiques
            # à ceux du reste de la plateforme : un écart produirait des
            # montants convertis incohérents et des jetons non joignables.
            "WABA_GHS_PER_EUR": "{{ var.value.get('waba_ghs_per_eur', '17.5') }}",
            "WABA_PII_KEY": "{{ var.value.get('waba_pii_key', '') }}",
            "SPARK_DRIVER_MEMORY": "2g",
        },
        auto_remove="force",
        # Le conteneur Spark n'a pas besoin du répertoire temporaire monté par
        # défaut, et ce montage échoue sur certaines configurations hôtes.
        mount_tmp_dir=False,
        docker_url="unix://var/run/docker.sock",
        **kwargs,
    )
