"""Recensement et archivage des fichiers de la zone d'atterrissage.

Ces opérations portent sur des objets, pas sur des données : lister un bucket ou
déplacer un fichier sont des appels d'API, et mobiliser un moteur distribué pour
les exécuter n'apporterait rien. Spark n'intervient qu'une fois les chemins
connus.
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Tuple

import boto3
from botocore.client import Config

logger = logging.getLogger(__name__)

REFERENTIALS_PREFIX = "referentials"


def s3_client():
    access_key = os.getenv("AWS_ACCESS_KEY_ID") or os.getenv("MINIO_ROOT_USER")
    secret_key = os.getenv("AWS_SECRET_ACCESS_KEY") or os.getenv("MINIO_ROOT_PASSWORD")
    return boto3.client(
        "s3",
        endpoint_url=os.getenv("MINIO_ENDPOINT", "http://minio:9000"),
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name=os.getenv("AWS_REGION", "us-east-1"),
        config=Config(s3={"addressing_style": "path"}, retries={"max_attempts": 3}),
    )


def _dataset_of(key: str) -> str:
    """Déduit le jeu de données du chemin de l'objet.

    Deux conventions coexistent, conformément à l'énoncé : les référentiels sont
    partagés entre pays et vivent à la racine, les transactions sont rangées en
    sous-dossiers pays / type.
    """
    parts = key.split("/")
    if parts[0] == REFERENTIALS_PREFIX:
        return parts[-1].rsplit(".", 1)[0]
    if len(parts) == 3:
        return parts[1]
    return ""


def list_pending(client, bucket: str, datasets=None, countries=None) -> Dict[str, List[str]]:
    """Fichiers présents dans la zone d'atterrissage, groupés par jeu de données."""
    paginator = client.get_paginator("list_objects_v2")
    grouped: Dict[str, List[str]] = {}

    for page in paginator.paginate(Bucket=bucket):
        for item in page.get("Contents", []):
            key = item["Key"]
            if not key.endswith(".csv"):
                continue
            dataset = _dataset_of(key)
            if not dataset:
                logger.warning("chemin non reconnu, ignoré : %s", key)
                continue
            if datasets and dataset not in datasets:
                continue
            if countries and not key.startswith(REFERENTIALS_PREFIX):
                if key.split("/")[0] not in countries:
                    continue
            grouped.setdefault(dataset, []).append(key)

    return grouped


def to_uris(bucket: str, keys: List[str]) -> List[str]:
    """Chemins s3a:// consommables par Spark."""
    return ["s3a://{}/{}".format(bucket, key) for key in keys]


def older_than(client, bucket: str, keys: List[str], minutes: int) -> List[str]:
    """Restreint une liste aux objets déposés depuis plus de `minutes`.

    Sert de délai de grâce avant archivage. Au Level 3, NiFi surveille la même
    zone d'atterrissage que le job batch : un fichier archivé avant que NiFi ne
    l'ait recensé disparaît du chemin streaming sans laisser de trace, et un
    fichier archivé entre le recensement et le téléchargement produit un rejet
    qui n'en est pas un. Laisser mûrir les fichiers quelques minutes supprime la
    course, sans renoncer à l'archivage.
    """
    if minutes <= 0:
        return list(keys)

    limite = datetime.now(timezone.utc) - timedelta(minutes=minutes)
    mûrs: List[str] = []
    for key in keys:
        try:
            depose = client.head_object(Bucket=bucket, Key=key)["LastModified"]
        except Exception as exc:  # noqa: BLE001 — un objet illisible n'est pas archivé
            logger.warning("âge indéterminé pour %s : %s", key, exc)
            continue
        if depose <= limite:
            mûrs.append(key)
    return mûrs


def archive(client, raw_bucket: str, archive_bucket: str, keys: List[str]) -> Tuple[int, int]:
    """Déplace les fichiers traités vers le bucket d'archive (§1.2).

    Copie puis suppression : S3 ne connaît pas le déplacement atomique. En cas
    d'échec de la copie, le fichier reste dans la zone d'atterrissage et sera
    repris au prochain passage — l'idempotence du MERGE rend ce rejeu inoffensif.
    """
    moved = failed = 0
    for key in keys:
        try:
            client.copy_object(
                Bucket=archive_bucket,
                Key=key,
                CopySource={"Bucket": raw_bucket, "Key": key},
            )
            client.delete_object(Bucket=raw_bucket, Key=key)
            moved += 1
        except Exception as exc:  # noqa: BLE001 — on veut poursuivre l'archivage
            logger.error("archivage impossible pour %s : %s", key, exc)
            failed += 1
    return moved, failed
