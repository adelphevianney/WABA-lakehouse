"""Orchestration d'un cycle de génération.

Sépare la logique métier de l'interface : `app.py` ne fait que de l'affichage et
de la saisie, tout ce qui décide de quoi générer et où le déposer vit ici. Cette
frontière permet de tester un cycle complet sans lancer Streamlit, et de
réutiliser le même code depuis un script ou un DAG Airflow.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import date, datetime, time as dtime, timedelta

import numpy as np
import pandas as pd

from generator import anomalies as ano
from generator import config as cfg
from generator import referentials as ref
from generator import storage as st
from generator import transactions as txn

logger = logging.getLogger(__name__)

#: Entités susceptibles de produire chaque type de fichier. Sert à déterminer
#: les pays éligibles : un fichier mobile money n'a de sens que dans les quatre
#: pays où l'entité opère.
KIND_ENTITIES: dict[str, tuple[str, ...]] = {
    "bank_txn": ("BANK", "MICROFINANCE"),
    "loan_repayments": ("BANK", "MICROFINANCE"),
    "insurance_ops": ("INSURANCE",),
    "mobile_money": ("MOBILE_MONEY",),
}

KIND_LABELS: dict[str, str] = {
    "bank_txn": "Transactions bancaires",
    "insurance_ops": "Opérations d'assurance",
    "mobile_money": "Paiements mobile money",
    "loan_repayments": "Remboursements de crédit",
}


def eligible_countries(kind: str, entity_types: tuple[str, ...] | None = None) -> tuple[str, ...]:
    """Pays où ce type de données peut exister, compte tenu des entités retenues.

    Empêche de proposer des paiements mobile money au Mali, où le groupe n'a pas
    d'entité mobile money : la génération échouerait faute de clients à
    référencer.
    """
    entities = set(KIND_ENTITIES[kind])
    if entity_types:
        entities &= set(entity_types)
    return tuple(
        country for country in cfg.COUNTRY_CODES
        if any(country in cfg.countries_of(entity) for entity in entities)
    )


@dataclass(frozen=True)
class GenerationRequest:
    """Paramètres saisis dans l'interface.

    `rows` s'entend **par fichier**, c'est-à-dire par pays et par journée : c'est
    la lecture qui donne son sens aux valeurs par défaut de l'énoncé, un système
    source produisant un fichier par jour et non un fichier par trimestre.
    """

    kinds: tuple[str, ...]
    countries: tuple[str, ...]
    entity_types: tuple[str, ...]
    rows: dict[str, int]
    start: datetime
    end: datetime
    anomaly_rate: float = cfg.DEFAULT_ANOMALY_RATE

    def days(self) -> list[date]:
        """Journées couvertes par la période, une par fichier produit."""
        first, last = self.start.date(), self.end.date()
        return [first + timedelta(days=offset) for offset in range((last - first).days + 1)]

    def file_count(self) -> int:
        """Nombre de fichiers que produira la demande, pour affichage préalable."""
        return sum(
            len(self.days()) * len(set(self.countries) & set(eligible_countries(kind, self.entity_types)))
            for kind in self.kinds
        )


@dataclass
class BatchResult:
    """Compte rendu des fichiers déposés pour un couple type / pays.

    Les journées sont agrégées : une demande sur un trimestre produit 90
    fichiers par pays, et en détailler chacun rendrait le compte rendu
    illisible.
    """

    kind: str
    country_code: str
    rows: int
    key: str
    files: int = 1
    anomalies: list[ano.AnomalyReport] = field(default_factory=list)
    skipped_reason: str | None = None

    @property
    def ok(self) -> bool:
        return self.skipped_reason is None


# =============================================================================
# Référentiels
# =============================================================================


def build_referentials(
    store: st.RawLandingStore,
    sizes: dict[str, int] | None = None,
    seed: int | None = None,
) -> dict[str, pd.DataFrame]:
    """Génère les quatre référentiels et les dépose dans le bucket."""
    frames = ref.generate_all(sizes, seed)
    for name, frame in frames.items():
        store.put_referential(frame, name)
    return frames


def load_index(store: st.RawLandingStore) -> txn.ReferentialIndex:
    """Recharge les référentiels du bucket et construit l'index de génération.

    Les référentiels ne sont volontairement pas régénérés : les fichiers de
    transactions déjà déposés référencent leurs clés, et les remplacer
    créerait exactement les clés orphelines que l'énoncé interdit.
    """
    return txn.ReferentialIndex(**store.load_referentials())


# =============================================================================
# Cycle de génération
# =============================================================================


def run_batch(
    index: txn.ReferentialIndex,
    store: st.RawLandingStore,
    request: GenerationRequest,
    rng: np.random.Generator,
) -> list[BatchResult]:
    """Génère, altère et dépose **un fichier par pays et par journée**.

    Le découpage journalier n'est pas cosmétique. La nomenclature imposée,
    `bank_txn_CI_20260101_01.csv`, désigne le jour des transactions contenues :
    un fichier unique couvrant un trimestre entier respecterait la forme du nom
    mais pas son sens. Il produit surtout un partitionnement Iceberg pathologique
    — 8 pays x 90 jours donnent 720 partitions se partageant le volume d'un seul
    fichier, soit quelques dizaines de lignes chacune, là où une journée de
    données remplit correctement sa partition.
    """
    results: list[BatchResult] = []
    days = request.days()

    for kind in request.kinds:
        allowed = eligible_countries(kind, request.entity_types)
        n_rows = request.rows.get(kind, cfg.DEFAULT_ROWS[kind])

        for country in request.countries:
            if country not in allowed:
                results.append(BatchResult(
                    kind, country, 0, "", files=0,
                    skipped_reason=f"le groupe n'opère pas de {KIND_LABELS[kind].lower()} dans ce pays",
                ))
                continue

            total_rows = 0
            reports: list[ano.AnomalyReport] = []
            last_key = ""
            failure: str | None = None

            for day in days:
                # Les horodatages sont bornés à la journée du fichier : c'est ce
                # qui aligne le nom du fichier, son contenu et la partition
                # Iceberg qui l'accueillera.
                try:
                    frame = txn.GENERATORS[kind](
                        index, country, n_rows,
                        datetime.combine(day, dtime.min),
                        datetime.combine(day, dtime.max),
                        rng, entity_types=request.entity_types or None,
                    )
                except ValueError as exc:
                    failure = str(exc)
                    break

                frame, day_reports = ano.inject(
                    kind, frame, index, country, request.anomaly_rate, rng
                )
                last_key = store.put_dataset(frame, kind, country, day)
                total_rows += len(frame)
                reports.extend(day_reports)

            if failure is not None:
                results.append(BatchResult(kind, country, 0, "", files=0, skipped_reason=failure))
                continue

            results.append(BatchResult(
                kind, country, total_rows, last_key, files=len(days), anomalies=reports,
            ))

    return results
