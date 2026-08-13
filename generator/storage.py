"""Dépôt des fichiers générés dans MinIO.

L'énoncé fixe deux choses : la nomenclature des fichiers
(`bank_txn_[CC]_YYYYMMDD_NN.csv`) et l'organisation du bucket `raw-landing` en
sous-dossiers pays / type. Ce module est le seul endroit du générateur qui
connaît S3 ; les modules de génération produisent des DataFrames et ignorent
tout de leur destination, ce qui les rend testables sans infrastructure.

Le numéro de séquence `NN` est déterminé en interrogeant le bucket : deux lots
générés le même jour pour le même pays ne s'écrasent pas. C'est aussi ce qui
permet au mode continu de déposer un fichier par cycle.
"""

from __future__ import annotations

import io
import logging
import os
import re
from dataclasses import dataclass
from datetime import date
from typing import Final

import boto3
import pandas as pd
from botocore.client import Config
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)

#: Préfixe des référentiels, partagés entre pays et donc hors arborescence pays.
REFERENTIALS_PREFIX: Final[str] = "referentials"

#: Colonnes à charger en texte : ce sont des identifiants, pas des nombres.
#: Sans cela pandas convertirait `account_number` en entier et perdrait ses
#: zéros de tête, brisant la correspondance avec les fichiers déjà déposés.
_STRING_COLUMNS: Final[dict[str, tuple[str, ...]]] = {
    "customers": ("customer_id",),
    "accounts": ("account_id", "customer_id", "account_number", "iban"),
    "branches": ("branch_id",),
    "products": ("product_id",),
}

_SEQUENCE_PATTERN: Final[re.Pattern[str]] = re.compile(r"_(\d{2})\.csv$")


@dataclass(frozen=True)
class StorageSettings:
    """Paramètres de connexion, résolus depuis l'environnement.

    Aucune valeur n'est codée en dur : la contrainte « aucun credential
    hardcodé » s'applique au générateur comme au reste de la plateforme.
    """

    endpoint: str
    access_key: str
    secret_key: str
    region: str
    raw_bucket: str

    @classmethod
    def from_env(cls) -> "StorageSettings":
        access_key = os.getenv("AWS_ACCESS_KEY_ID") or os.getenv("MINIO_ROOT_USER")
        secret_key = os.getenv("AWS_SECRET_ACCESS_KEY") or os.getenv("MINIO_ROOT_PASSWORD")
        if not access_key or not secret_key:
            raise RuntimeError(
                "Identifiants MinIO absents : définir AWS_ACCESS_KEY_ID / "
                "AWS_SECRET_ACCESS_KEY (ou MINIO_ROOT_USER / MINIO_ROOT_PASSWORD). "
                "Voir README.env.example."
            )
        return cls(
            endpoint=os.getenv("MINIO_ENDPOINT", "http://localhost:9000"),
            access_key=access_key,
            secret_key=secret_key,
            region=os.getenv("AWS_REGION", "us-east-1"),
            raw_bucket=os.getenv("BUCKET_RAW", "raw-landing"),
        )


class RawLandingStore:
    """Accès au bucket d'atterrissage des fichiers bruts."""

    def __init__(self, settings: StorageSettings | None = None) -> None:
        self.settings = settings or StorageSettings.from_env()
        self._client = boto3.client(
            "s3",
            endpoint_url=self.settings.endpoint,
            aws_access_key_id=self.settings.access_key,
            aws_secret_access_key=self.settings.secret_key,
            region_name=self.settings.region,
            # MinIO n'expose pas de sous-domaines par bucket : l'adressage doit
            # rester en style « chemin ».
            config=Config(s3={"addressing_style": "path"}, retries={"max_attempts": 3}),
        )

    # -- Diagnostics ---------------------------------------------------------

    def is_reachable(self) -> tuple[bool, str]:
        """Teste la connexion sans lever, pour affichage dans l'interface."""
        try:
            self._client.head_bucket(Bucket=self.settings.raw_bucket)
            return True, f"bucket « {self.settings.raw_bucket} » accessible"
        except ClientError as exc:
            return False, f"{exc.response['Error'].get('Code', 'ERREUR')} sur {self.settings.endpoint}"
        except Exception as exc:  # connexion refusée, DNS, ...
            return False, f"{type(exc).__name__} sur {self.settings.endpoint}"

    def _list_keys(self, prefix: str) -> list[str]:
        paginator = self._client.get_paginator("list_objects_v2")
        keys: list[str] = []
        for page in paginator.paginate(Bucket=self.settings.raw_bucket, Prefix=prefix):
            keys.extend(item["Key"] for item in page.get("Contents", []))
        return keys

    # -- Écriture ------------------------------------------------------------

    def _put_csv(self, frame: pd.DataFrame, key: str) -> str:
        buffer = io.BytesIO()
        # `date_format` garantit un horodatage ISO 8601 quelle que soit la
        # locale de la machine, ce que le schéma explicite de Spark attend.
        frame.to_csv(buffer, index=False, date_format="%Y-%m-%dT%H:%M:%S")
        buffer.seek(0)
        self._client.put_object(
            Bucket=self.settings.raw_bucket,
            Key=key,
            Body=buffer.getvalue(),
            ContentType="text/csv",
        )
        logger.info("déposé s3://%s/%s (%d lignes)", self.settings.raw_bucket, key, len(frame))
        return key

    def next_sequence(self, kind: str, country_code: str, day: date) -> int:
        """Prochain numéro de lot pour un couple pays / type / jour."""
        prefix = f"{country_code}/{kind}/"
        stamp = day.strftime("%Y%m%d")
        used = [
            int(match.group(1))
            for key in self._list_keys(prefix)
            if f"_{stamp}_" in key and (match := _SEQUENCE_PATTERN.search(key))
        ]
        return max(used, default=0) + 1

    def put_dataset(
        self, frame: pd.DataFrame, kind: str, country_code: str, day: date
    ) -> str:
        """Dépose un lot de transactions en respectant la nomenclature de l'énoncé."""
        sequence = self.next_sequence(kind, country_code, day)
        filename = f"{kind}_{country_code}_{day.strftime('%Y%m%d')}_{sequence:02d}.csv"
        return self._put_csv(frame, f"{country_code}/{kind}/{filename}")

    def put_referential(self, frame: pd.DataFrame, name: str) -> str:
        """Dépose un référentiel, hors arborescence pays puisqu'il est partagé."""
        return self._put_csv(frame, f"{REFERENTIALS_PREFIX}/{name}.csv")

    # -- Lecture -------------------------------------------------------------

    def referentials_present(self) -> bool:
        expected = {f"{REFERENTIALS_PREFIX}/{name}.csv" for name in _STRING_COLUMNS}
        return expected.issubset(set(self._list_keys(f"{REFERENTIALS_PREFIX}/")))

    def load_referentials(self) -> dict[str, pd.DataFrame]:
        """Recharge les référentiels déjà déposés.

        Indispensable au redémarrage de l'application : sans cela, une nouvelle
        génération de référentiels invaliderait toutes les clés des fichiers de
        transactions déjà présents dans le bucket.
        """
        frames: dict[str, pd.DataFrame] = {}
        for name, string_columns in _STRING_COLUMNS.items():
            response = self._client.get_object(
                Bucket=self.settings.raw_bucket, Key=f"{REFERENTIALS_PREFIX}/{name}.csv"
            )
            frames[name] = pd.read_csv(
                io.BytesIO(response["Body"].read()),
                dtype={column: "string" for column in string_columns},
            )
        return frames

    def inventory(self) -> pd.DataFrame:
        """Inventaire des fichiers déposés, par pays et par type."""
        rows = [
            {"country_code": parts[0], "kind": parts[1], "key": key}
            for key in self._list_keys("")
            if len(parts := key.split("/")) == 3 and parts[0] != REFERENTIALS_PREFIX
        ]
        if not rows:
            return pd.DataFrame(columns=["country_code", "kind", "fichiers"])
        return (
            pd.DataFrame(rows)
            .groupby(["country_code", "kind"], as_index=False)
            .size()
            .rename(columns={"size": "fichiers"})
        )
