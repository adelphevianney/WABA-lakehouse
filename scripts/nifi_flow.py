"""Construction du flux d'ingestion temps réel NiFi, par l'API REST.

Le flux surveille le bucket `raw-landing`, télécharge chaque fichier déposé, le
découpe en événements JSON, y ajoute la traçabilité (`ingestion_timestamp`,
`source_file`) et publie chaque événement dans le topic Kafka correspondant à
son jeu de données.

    ListS3 -> UpdateAttribute -> RouteOnAttribute -> FetchS3Object
           -> SplitRecord -> UpdateRecord -> PublishKafka

Pourquoi construire le flux par script plutôt que l'exporter d'une session
manuelle : un template exporté est un document opaque de plusieurs milliers de
lignes, illisible en revue et impossible à comparer d'une version à l'autre. Ce
script tient dans un fichier relisible, se rejoue à l'identique sur une
installation vierge, et surtout tire les noms de topics de `common.domain` — la
même source de vérité que les jobs Spark qui les consomment. Une divergence
entre producteur et consommateur ne se manifesterait autrement que par un topic
vide, sans la moindre erreur.

Usage :
    python scripts/nifi_flow.py               # construit (ou reconstruit) et démarre
    python scripts/nifi_flow.py --stop        # arrête sans détruire
    python scripts/nifi_flow.py --start       # redémarre
    python scripts/nifi_flow.py --delete      # supprime le flux et ses paramètres
    python scripts/nifi_flow.py --reset-state # rejoue l'ingestion depuis le début
    python scripts/nifi_flow.py --status      # état des composants et des files
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from common.domain import DLQ_TOPIC, RAW_TOPICS  # noqa: E402
from scripts.nifi_client import NiFiClient, NiFiError, load_env_file  # noqa: E402

# =============================================================================
# Nommage
# =============================================================================

GROUP_NAME = "WABA - Ingestion temps reel"
CONTEXT_NAME = "waba-ingestion"

#: Tous les topics de la couche brute partagent ce préfixe. Le routage s'appuie
#: dessus pour distinguer un fichier de transactions d'un référentiel ; une
#: renomination qui le romprait rendrait le flux silencieusement inopérant, d'où
#: la vérification explicite au démarrage.
TOPIC_PREFIX = "raw-"

TYPE_LIST_S3 = "org.apache.nifi.processors.aws.s3.ListS3"
TYPE_FETCH_S3 = "org.apache.nifi.processors.aws.s3.FetchS3Object"
TYPE_UPDATE_ATTRIBUTE = "org.apache.nifi.processors.attributes.UpdateAttribute"
TYPE_ROUTE_ON_ATTRIBUTE = "org.apache.nifi.processors.standard.RouteOnAttribute"
TYPE_SPLIT_RECORD = "org.apache.nifi.processors.standard.SplitRecord"
TYPE_UPDATE_RECORD = "org.apache.nifi.processors.standard.UpdateRecord"
TYPE_PUBLISH_KAFKA = "org.apache.nifi.kafka.processors.PublishKafka"

TYPE_AWS_CREDENTIALS = (
    "org.apache.nifi.processors.aws.credentials.provider.service."
    "AWSCredentialsProviderControllerService"
)
TYPE_CSV_READER = "org.apache.nifi.csv.CSVReader"
TYPE_JSON_READER = "org.apache.nifi.json.JsonTreeReader"
TYPE_JSON_WRITER = "org.apache.nifi.json.JsonRecordSetWriter"
TYPE_KAFKA_CONNECTION = "org.apache.nifi.kafka.service.Kafka3ConnectionService"


# =============================================================================
# Expressions
# =============================================================================


def dataset_expression() -> str:
    """Jeu de données déduit du chemin de l'objet.

    Les fichiers de transactions suivent l'arborescence `PAYS/type/fichier.csv`
    fixée par l'énoncé ; les référentiels, partagés entre pays, vivent sous
    `referentials/`. Prendre l'avant-dernier segment donne le type pour les
    premiers, et `referentials` pour les seconds — qui ne seront donc rattachés
    à aucun topic.
    """
    return "${filename:substringBeforeLast('/'):substringAfterLast('/')}"


def topic_expression() -> str:
    """Topic Kafka cible, dérivé du jeu de données.

    La correspondance n'est pas une transformation de chaîne (`bank_txn` donne
    `raw-bank-transactions`) : elle est énumérée dans `common.domain` et
    traduite ici en une chaîne de substitutions. Un chemin qui ne correspond à
    aucun jeu de données ressort inchangé, donc sans le préfixe `raw-` que le
    routage exige.
    """
    hors_convention = [t for t in RAW_TOPICS.values() if not t.startswith(TOPIC_PREFIX)]
    if hors_convention:
        raise RuntimeError(
            "les topics {} ne portent pas le préfixe « {} » sur lequel repose le "
            "routage du flux NiFi".format(hors_convention, TOPIC_PREFIX)
        )
    substitutions = "".join(
        ":replace('{}', '{}')".format(dataset, topic) for dataset, topic in RAW_TOPICS.items()
    )
    return "${filename:substringBeforeLast('/'):substringAfterLast('/')" + substitutions + "}"


# =============================================================================
# Constructeur
# =============================================================================


class FlowBuilder:
    """Crée, démarre et détruit le groupe de traitement d'ingestion."""

    def __init__(self, client: NiFiClient) -> None:
        self.client = client
        self.root = client.root_process_group_id()
        self.group_id = ""
        self.services: Dict[str, str] = {}
        self.processors: Dict[str, str] = {}

    # -- Recherche -----------------------------------------------------------

    def find_group(self) -> Optional[Dict[str, Any]]:
        groups = self.client.get(
            "/process-groups/{}/process-groups".format(self.root)
        )["processGroups"]
        for group in groups:
            if group["component"]["name"] == GROUP_NAME:
                return group
        return None

    def find_context(self) -> Optional[Dict[str, Any]]:
        contexts = self.client.get("/flow/parameter-contexts")["parameterContexts"]
        for context in contexts:
            if context["component"]["name"] == CONTEXT_NAME:
                return context
        return None

    # -- Destruction ---------------------------------------------------------

    def teardown(self) -> None:
        """Supprime le flux existant pour permettre une reconstruction propre.

        Reconstruire plutôt que rapiécer : c'est ce qui garantit qu'un dépôt
        cloné produit exactement le même flux qu'une installation déjà en
        service, sans état résiduel d'une version antérieure du script.
        """
        group = self.find_group()
        if group:
            gid = group["id"]
            print("  suppression du groupe existant {}".format(gid))
            self._set_group_state(gid, "STOPPED")
            self._set_services_state(gid, "DISABLED")
            self._empty_queues(gid)
            revision = self.client.revision_of("/process-groups/{}".format(gid))
            self.client.delete("/process-groups/{}".format(gid), revision["version"])

        context = self.find_context()
        if context:
            print("  suppression du contexte de paramètres {}".format(context["id"]))
            self.client.delete(
                "/parameter-contexts/{}".format(context["id"]),
                context["revision"]["version"],
            )

    def _empty_queues(self, group_id: str) -> None:
        """Vide les files : NiFi refuse de supprimer un groupe qui retient des données."""
        try:
            demande = self.client.post(
                "/process-groups/{}/empty-all-connections-requests".format(group_id), {}
            )["dropRequest"]
        except NiFiError:
            return
        for _ in range(30):
            if demande.get("finished"):
                break
            time.sleep(1)
            demande = self.client.get(
                "/process-groups/{}/empty-all-connections-requests/{}".format(
                    group_id, demande["id"])
            )["dropRequest"]
        try:
            self.client.request(
                "DELETE",
                "/process-groups/{}/empty-all-connections-requests/{}".format(
                    group_id, demande["id"]),
            )
        except NiFiError:
            pass

    # -- Paramètres ----------------------------------------------------------

    def create_parameter_context(self) -> str:
        """Contexte de paramètres : point d'entrée unique des secrets et des adresses.

        Les identifiants MinIO y sont déclarés « sensibles » : NiFi les chiffre
        dans sa configuration et son API ne les restitue jamais. Ils ne
        figurent donc ni dans ce script, ni dans le flux exporté — ils viennent
        de l'environnement, comme partout ailleurs dans le projet.
        """
        access_key = os.getenv("MINIO_ROOT_USER") or os.getenv("AWS_ACCESS_KEY_ID")
        secret_key = os.getenv("MINIO_ROOT_PASSWORD") or os.getenv("AWS_SECRET_ACCESS_KEY")
        if not access_key or not secret_key:
            raise NiFiError(
                "Identifiants MinIO absents : définir MINIO_ROOT_USER / "
                "MINIO_ROOT_PASSWORD. Voir README.env.example."
            )

        parametres = [
            ("minio.access.key", access_key, True, "Identifiant d'accès MinIO"),
            ("minio.secret.key", secret_key, True, "Clé secrète MinIO"),
            # Vue depuis le conteneur NiFi, MinIO répond sur son nom de service.
            ("minio.endpoint", os.getenv("NIFI_MINIO_ENDPOINT", "http://minio:9000"),
             False, "Adresse de MinIO sur le réseau Compose"),
            ("raw.bucket", os.getenv("BUCKET_RAW", "raw-landing"),
             False, "Bucket surveillé (zone d'atterrissage)"),
            ("kafka.brokers", os.getenv("NIFI_KAFKA_BROKERS", "kafka:9092"),
             False, "Amorçage du cluster Kafka"),
            ("split.size", os.getenv("NIFI_SPLIT_SIZE", "500"),
             False, "Nombre d'événements par lot en sortie de découpage"),
            ("dlq.topic", DLQ_TOPIC, False, "Topic de rebut de l'ingestion"),
        ]

        entity = self.client.post("/parameter-contexts", {
            "revision": self.client.new_revision(),
            "component": {
                "name": CONTEXT_NAME,
                "description": "Adresses et secrets du flux d'ingestion WABA",
                "parameters": [
                    {"parameter": {"name": nom, "value": valeur,
                                   "sensitive": sensible, "description": description}}
                    for nom, valeur, sensible, description in parametres
                ],
            },
        })
        return entity["id"]

    # -- Groupe --------------------------------------------------------------

    def create_group(self, context_id: str) -> str:
        entity = self.client.post("/process-groups/{}/process-groups".format(self.root), {
            "revision": self.client.new_revision(),
            "component": {
                "name": GROUP_NAME,
                "position": {"x": 0.0, "y": 0.0},
                "comments": "Level 3.1 — ingestion temps réel de raw-landing vers Kafka. "
                            "Construit par scripts/nifi_flow.py.",
                "parameterContext": {"id": context_id},
            },
        })
        self.group_id = entity["id"]
        return self.group_id

    # -- Services ------------------------------------------------------------

    def create_service(self, name: str, type_: str,
                       properties: Dict[str, Any], comments: str = "") -> str:
        entity = self.client.post(
            "/process-groups/{}/controller-services".format(self.group_id), {
                "revision": self.client.new_revision(),
                "component": {"type": type_, "name": name,
                              "comments": comments, "properties": properties},
            })
        self.services[name] = entity["id"]
        return entity["id"]

    def create_services(self) -> None:
        self.create_service(
            "identifiants-minio", TYPE_AWS_CREDENTIALS,
            {"Access Key": "#{minio.access.key}", "Secret Key": "#{minio.secret.key}"},
            "Résolus depuis le contexte de paramètres, jamais écrits dans le flux.",
        )
        self.create_service(
            "lecteur-csv", TYPE_CSV_READER,
            {
                # L'en-tête des fichiers déposés porte les noms de colonnes de
                # l'annexe A : le schéma en est dérivé, ce qui évite de figer
                # quatre schémas Avro dans le flux.
                #
                # Le schéma est dérivé de l'en-tête et non inféré à la lecture,
                # pour deux raisons. La première est de robustesse : l'inférence
                # parcourt le fichier entier avant de produire le moindre
                # enregistrement, et un fichier malformé y lève une exception
                # que SplitRecord ne rattrape pas — le lot est alors annulé et
                # rejoué indéfiniment, sans jamais atteindre la file de rebut.
                # Dérivé de l'en-tête, le lecteur se construit sans lire le
                # corps, et l'erreur survient là où elle est gérée.
                # La seconde est de cohérence : le typage appartient à la couche
                # Silver, pas à l'ingestion. Tout arrive en texte, exactement
                # comme dans la chaîne batch où les schémas sont explicites et
                # jamais devinés — un montant inférable en entier dans un
                # fichier et en décimal dans le suivant produirait deux schémas
                # pour un même topic.
                "Skip Header Line": "true",
                "schema-access-strategy": "csv-header-derived",
                "csvutils-character-set": "UTF-8",
            },
            "Un seul lecteur pour les quatre jeux de données : le schéma vient de l'en-tête.",
        )
        self.create_service("lecteur-json", TYPE_JSON_READER, {})
        self.create_service(
            "ecrivain-json-lot", TYPE_JSON_WRITER,
            {"output-grouping": "output-array", "suppress-nulls": "never-suppress"},
            "Sortie du découpage : un tableau JSON par lot, relu ensuite.",
        )
        self.create_service(
            "ecrivain-json-evenement", TYPE_JSON_WRITER,
            # Un message Kafka porte un événement, pas un tableau d'un élément :
            # sans ce regroupement, chaque message serait encapsulé dans `[...]`
            # et le consommateur devrait défaire l'emballage.
            {"output-grouping": "output-oneline", "suppress-nulls": "never-suppress"},
            "Sortie vers Kafka : un objet JSON par message.",
        )
        self.create_service(
            "connexion-kafka", TYPE_KAFKA_CONNECTION,
            {"bootstrap.servers": "#{kafka.brokers}", "security.protocol": "PLAINTEXT"},
            "Broker unique en KRaft ; le chiffrement du transport relève du Level 4.",
        )

    def enable_services(self) -> None:
        self.client.put(
            "/flow/process-groups/{}/controller-services".format(self.group_id),
            {"id": self.group_id, "state": "ENABLED"},
        )
        for _ in range(30):
            etats = self.client.get(
                "/flow/process-groups/{}/controller-services".format(self.group_id)
            )["controllerServices"]
            if all(s["component"]["state"] == "ENABLED" for s in etats):
                return
            invalides = [
                "{} : {}".format(s["component"]["name"],
                                 "; ".join(s["component"].get("validationErrors") or []))
                for s in etats if s["component"]["validationStatus"] == "INVALID"
            ]
            if invalides:
                raise NiFiError("services invalides —\n  " + "\n  ".join(invalides))
            time.sleep(1)
        raise NiFiError("les services de contrôle ne se sont pas activés")

    # -- Processeurs ---------------------------------------------------------

    def create_processor(
        self,
        name: str,
        type_: str,
        position: Tuple[float, float],
        properties: Dict[str, Any],
        *,
        auto_terminate: Sequence[str] = (),
        scheduling_period: Optional[str] = None,
        comments: str = "",
    ) -> str:
        config: Dict[str, Any] = {
            "properties": properties,
            "autoTerminatedRelationships": list(auto_terminate),
            "comments": comments,
        }
        if scheduling_period:
            config["schedulingStrategy"] = "TIMER_DRIVEN"
            config["schedulingPeriod"] = scheduling_period
        entity = self.client.post("/process-groups/{}/processors".format(self.group_id), {
            "revision": self.client.new_revision(),
            "component": {"type": type_, "name": name,
                          "position": {"x": position[0], "y": position[1]},
                          "config": config},
        })
        self.processors[name] = entity["id"]
        return entity["id"]

    def create_processors(self) -> None:
        region = os.getenv("AWS_REGION", "us-east-1")
        creds = self.services["identifiants-minio"]

        self.create_processor(
            "recense-la-zone-d-atterrissage", TYPE_LIST_S3, (0, 0),
            {
                "Bucket": "#{raw.bucket}",
                # La région est une énumération fermée côté NiFi : elle ne peut
                # pas être paramétrée, et MinIO l'ignore de toute façon.
                "Region": region,
                "Endpoint Override URL": "#{minio.endpoint}",
                "AWS Credentials Provider service": creds,
                # NiFi retient l'horodatage du dernier objet vu : un fichier
                # déjà recensé ne l'est pas deux fois, même après redémarrage.
                "listing-strategy": "timestamps",
                # Un objet en cours d'écriture ne doit pas être recensé : MinIO
                # publie l'objet atomiquement, mais la marge protège d'une
                # horloge légèrement décalée entre le générateur et NiFi.
                "min-age": "5 sec",
            },
            scheduling_period="30 sec",
            comments="Recense sans télécharger : seuls les fichiers retenus par le "
                     "routage seront effectivement lus.",
        )

        self.create_processor(
            "identifie-le-jeu-de-donnees", TYPE_UPDATE_ATTRIBUTE, (0, 200),
            {
                "waba.country": "${filename:substringBefore('/')}",
                "waba.dataset": dataset_expression(),
                "waba.topic": topic_expression(),
            },
            comments="Le topic cible est déduit du chemin, à partir de la table de "
                     "correspondance de common.domain.",
        )

        self.create_processor(
            "ecarte-les-referentiels", TYPE_ROUTE_ON_ATTRIBUTE, (0, 400),
            {"transactions": "${waba.topic:startsWith('" + TOPIC_PREFIX + "')}"},
            auto_terminate=["unmatched"],
            comments="Les référentiels (clients, comptes, agences, produits) sont des "
                     "données de référence, pas des événements : le batch les charge, "
                     "le streaming les ignore.",
        )

        self.create_processor(
            "telecharge-le-fichier", TYPE_FETCH_S3, (0, 600),
            {
                "Bucket": "#{raw.bucket}",
                "Object Key": "${filename}",
                "Region": region,
                "Endpoint Override URL": "#{minio.endpoint}",
                "AWS Credentials Provider service": creds,
            },
        )

        self.create_processor(
            "decoupe-en-evenements", TYPE_SPLIT_RECORD, (0, 800),
            {
                "Record Reader": self.services["lecteur-csv"],
                "Record Writer": self.services["ecrivain-json-lot"],
                "Records Per Split": "#{split.size}",
            },
            auto_terminate=["original"],
            comments="Découper borne l'empreinte mémoire d'un lot et permet à la "
                     "contre-pression de s'exercer à la granularité de l'événement.",
        )

        self.create_processor(
            "ajoute-la-tracabilite", TYPE_UPDATE_RECORD, (0, 1000),
            {
                "Record Reader": self.services["lecteur-json"],
                "Record Writer": self.services["ecrivain-json-lot"],
                "Replacement Value Strategy": "literal-value",
                # Mêmes noms et même format que les deux colonnes ajoutées par le
                # job d'ingestion batch : c'est ce qui rendra la requête unifiée
                # du §3.4 possible sans réconciliation.
                "/ingestion_timestamp": "${now():format(\"yyyy-MM-dd'T'HH:mm:ss.SSS'Z'\", \"UTC\")}",
                "/source_file": "s3a://${s3.bucket}/${filename}",
            },
        )

        self.create_processor(
            "publie-vers-kafka", TYPE_PUBLISH_KAFKA, (0, 1200),
            {
                "Kafka Connection Service": self.services["connexion-kafka"],
                "Topic Name": "${waba.topic}",
                "Record Reader": self.services["lecteur-json"],
                "Record Writer": self.services["ecrivain-json-evenement"],
                # Partitionner par pays : les huit pays se répartissent sur les
                # partitions, et l'ordre des événements d'un même pays est
                # préservé — ce dont dépendent les fenêtres temporelles du §3.3.
                "Message Key Field": "country_code",
                # Un broker indisponible n'est pas une donnée invalide : plutôt
                # que de router vers le rebut, on annule le lot et on le
                # rejouera. Rien n'est perdu, et la contre-pression finit par
                # remonter jusqu'au recensement, qui s'arrête de lui-même.
                "Failure Strategy": "Rollback",
                "Transactions Enabled": "true",
                "compression.type": "snappy",
                "acks": "all",
            },
            auto_terminate=["success", "failure"],
        )

        # -- Branche de rebut ------------------------------------------------
        self.create_processor(
            "qualifie-le-rebut", TYPE_UPDATE_ATTRIBUTE, (600, 800),
            {
                # PublishKafka reprend l'attribut `kafka.key` comme clé du
                # message : les rebuts d'un même fichier restent groupés sur une
                # partition, donc dans l'ordre où ils ont été rejetés.
                "kafka.key": "${filename}",
                "waba.dlq.stage": "nifi-ingestion",
                "waba.dlq.reason": "fichier illisible ou non conforme au format attendu",
                "waba.dlq.timestamp": "${now():format(\"yyyy-MM-dd'T'HH:mm:ss.SSS'Z'\", \"UTC\")}",
            },
        )

        self.create_processor(
            "publie-le-rebut", TYPE_PUBLISH_KAFKA, (600, 1000),
            {
                "Kafka Connection Service": self.services["connexion-kafka"],
                "Topic Name": "#{dlq.topic}",
                # Pas de lecteur d'enregistrements ici : le contenu est
                # justement celui qui n'a pas pu être lu. Il est publié tel
                # quel, accompagné des attributs qui en donnent l'origine.
                "FlowFile Attribute Header Pattern": "waba\\..*|filename|s3\\.bucket",
                "Failure Strategy": "Rollback",
                "Transactions Enabled": "true",
                "acks": "all",
            },
            auto_terminate=["success", "failure"],
            comments="Le fichier rejeté est conservé dans Kafka avec son motif, plutôt "
                     "que silencieusement écarté.",
        )

    # -- Connexions ----------------------------------------------------------

    def connect(self, source: str, relationships: Iterable[str], destination: str,
                *, objects: int = 10000, size: str = "1 GB") -> str:
        entity = self.client.post("/process-groups/{}/connections".format(self.group_id), {
            "revision": self.client.new_revision(),
            "component": {
                "source": {"id": self.processors[source], "groupId": self.group_id,
                           "type": "PROCESSOR"},
                "destination": {"id": self.processors[destination], "groupId": self.group_id,
                                "type": "PROCESSOR"},
                "selectedRelationships": list(relationships),
                "backPressureObjectThreshold": objects,
                "backPressureDataSizeThreshold": size,
                "flowFileExpiration": "0 sec",
            },
        })
        return entity["id"]

    def create_connections(self) -> None:
        """Relie les processeurs et calibre la contre-pression.

        Une file pleine suspend le processeur qui l'alimente ; en calibrant la
        file d'entrée du producteur Kafka bien en deçà des valeurs par défaut
        (10 000 objets, 1 Go), la saturation remonte de proche en proche
        jusqu'au recensement, qui cesse de lister. Le broker n'est jamais
        sollicité au-delà de ce qu'il absorbe, et rien n'est perdu : les
        fichiers non traités restent dans MinIO.
        """
        self.connect("recense-la-zone-d-atterrissage", ["success"],
                     "identifie-le-jeu-de-donnees", objects=2000, size="16 MB")
        self.connect("identifie-le-jeu-de-donnees", ["success"],
                     "ecarte-les-referentiels", objects=2000, size="16 MB")
        # Les descripteurs de fichiers sont légers, mais chacun déclenchera un
        # téléchargement : c'est le nombre d'objets qui borne ici, pas le volume.
        self.connect("ecarte-les-referentiels", ["transactions"],
                     "telecharge-le-fichier", objects=200, size="16 MB")
        self.connect("telecharge-le-fichier", ["success"],
                     "decoupe-en-evenements", objects=200, size="256 MB")
        self.connect("decoupe-en-evenements", ["splits"],
                     "ajoute-la-tracabilite", objects=1000, size="128 MB")
        # File d'entrée du producteur : la plus étroite du flux, c'est elle qui
        # protège le broker.
        self.connect("ajoute-la-tracabilite", ["success"],
                     "publie-vers-kafka", objects=500, size="64 MB")

        for source in ("telecharge-le-fichier", "decoupe-en-evenements", "ajoute-la-tracabilite"):
            self.connect(source, ["failure"], "qualifie-le-rebut", objects=1000, size="64 MB")
        self.connect("qualifie-le-rebut", ["success"], "publie-le-rebut",
                     objects=1000, size="64 MB")

    # -- Cycle de vie --------------------------------------------------------

    def _set_group_state(self, group_id: str, state: str) -> None:
        self.client.put("/flow/process-groups/{}".format(group_id),
                        {"id": group_id, "state": state})

    def _set_services_state(self, group_id: str, state: str) -> None:
        try:
            self.client.put("/flow/process-groups/{}/controller-services".format(group_id),
                            {"id": group_id, "state": state})
        except NiFiError as exc:
            print("  (services : {})".format(exc))

    def assert_valid(self) -> None:
        """Refuse de démarrer un flux invalide, en disant précisément pourquoi."""
        for _ in range(15):
            processeurs = self.client.get(
                "/process-groups/{}/processors".format(self.group_id))["processors"]
            en_attente = [p for p in processeurs
                          if p["component"]["validationStatus"] == "VALIDATING"]
            if not en_attente:
                break
            time.sleep(1)

        invalides: List[str] = []
        for processeur in processeurs:
            composant = processeur["component"]
            if composant["validationStatus"] != "VALID":
                invalides.append("{} : {}".format(
                    composant["name"], "; ".join(composant.get("validationErrors") or [])))
        if invalides:
            raise NiFiError("processeurs invalides —\n  " + "\n  ".join(invalides))

    def start(self) -> None:
        self._set_group_state(self.group_id, "RUNNING")

    def reset_state(self) -> None:
        """Efface l'état du recensement pour rejouer tous les fichiers présents.

        ListS3 mémorise l'horodatage du dernier objet listé. Sans cet effacement,
        une démonstration sur un bucket déjà rempli ne produirait aucun message.
        """
        processeur_id = self.processors["recense-la-zone-d-atterrissage"]
        revision = self.client.revision_of("/processors/{}".format(processeur_id))
        self.client.put("/processors/{}/run-status".format(processeur_id),
                        {"revision": revision, "state": "STOPPED"})
        self.client.post("/processors/{}/state/clear-requests".format(processeur_id), {})
        revision = self.client.revision_of("/processors/{}".format(processeur_id))
        self.client.put("/processors/{}/run-status".format(processeur_id),
                        {"revision": revision, "state": "RUNNING"})


# =============================================================================
# Commandes
# =============================================================================


def _attach(builder: FlowBuilder) -> None:
    """Recharge les identifiants d'un flux déjà construit."""
    group = builder.find_group()
    if not group:
        raise NiFiError(
            "aucun groupe « {} » : construire le flux avec "
            "`python scripts/nifi_flow.py`".format(GROUP_NAME))
    builder.group_id = group["id"]
    for processeur in builder.client.get(
            "/process-groups/{}/processors".format(builder.group_id))["processors"]:
        builder.processors[processeur["component"]["name"]] = processeur["id"]


def build(builder: FlowBuilder) -> None:
    print("Construction du flux « {} »".format(GROUP_NAME))
    builder.teardown()

    context_id = builder.create_parameter_context()
    print("  contexte de paramètres     {}".format(context_id))
    print("  groupe de traitement       {}".format(builder.create_group(context_id)))

    builder.create_services()
    builder.enable_services()
    print("  services de contrôle       {} activés".format(len(builder.services)))

    builder.create_processors()
    builder.create_connections()
    print("  processeurs                {}".format(len(builder.processors)))

    builder.assert_valid()
    builder.start()
    print("\nFlux démarré. Topics alimentés :")
    for dataset, topic in RAW_TOPICS.items():
        print("  {:<16} -> {}".format(dataset, topic))
    print("  {:<16} -> {}".format("(illisible)", DLQ_TOPIC))


def status(builder: FlowBuilder) -> None:
    _attach(builder)
    groupe = builder.client.get("/flow/process-groups/{}".format(builder.group_id))
    flux = groupe["processGroupFlow"]["flow"]

    print("Processeurs")
    for processeur in sorted(flux["processors"], key=lambda p: p["component"]["name"]):
        statut = processeur["status"]["aggregateSnapshot"]
        print("  {:<34} {:<8} {}".format(
            processeur["component"]["name"], statut["runStatus"], statut["input"]))

    print("\nFiles d'attente (contre-pression)")
    for connexion in flux["connections"]:
        composant = connexion["component"]
        statut = connexion["status"]["aggregateSnapshot"]
        print("  {:<26} -> {:<26} {:>12}  seuil {} / {}".format(
            composant["source"]["name"][:26], composant["destination"]["name"][:26],
            statut["queued"], composant["backPressureObjectThreshold"],
            composant["backPressureDataSizeThreshold"]))


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    action = parser.add_mutually_exclusive_group()
    action.add_argument("--stop", action="store_true", help="arrête le flux sans le détruire")
    action.add_argument("--start", action="store_true", help="redémarre le flux")
    action.add_argument("--delete", action="store_true", help="supprime le flux et ses paramètres")
    action.add_argument("--reset-state", action="store_true",
                        help="rejoue l'ingestion de tous les fichiers présents")
    action.add_argument("--status", action="store_true", help="état des composants et des files")
    args = parser.parse_args(argv)

    load_env_file()
    client = NiFiClient()
    try:
        client.login()
        builder = FlowBuilder(client)

        if args.status:
            status(builder)
        elif args.delete:
            builder.teardown()
            print("Flux supprimé.")
        elif args.stop:
            _attach(builder)
            builder._set_group_state(builder.group_id, "STOPPED")
            print("Flux arrêté.")
        elif args.start:
            _attach(builder)
            builder._set_group_state(builder.group_id, "RUNNING")
            print("Flux démarré.")
        elif args.reset_state:
            _attach(builder)
            builder.reset_state()
            print("État du recensement effacé : les fichiers déjà déposés seront rejoués.")
        else:
            build(builder)
    except NiFiError as exc:
        print("Échec : {}".format(exc), file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
