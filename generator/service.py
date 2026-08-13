"""Orchestration d'un cycle de génération.

Sépare la logique métier de l'interface : `app.py` ne fait que de l'affichage et
de la saisie, tout ce qui décide de quoi générer et où le déposer vit ici. Cette
frontière permet de tester un cycle complet sans lancer Streamlit, et de
réutiliser le même code depuis un script ou un DAG Airflow.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import date, datetime

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
    """Paramètres saisis dans l'interface."""

    kinds: tuple[str, ...]
    countries: tuple[str, ...]
    entity_types: tuple[str, ...]
    rows: dict[str, int]
    start: datetime
    end: datetime
    anomaly_rate: float = cfg.DEFAULT_ANOMALY_RATE
    file_date: date | None = None

    def resolved_file_date(self) -> date:
        """Date portée par le nom de fichier, à défaut celle de fin de période."""
        return self.file_date or self.end.date()


@dataclass
class BatchResult:
    """Compte rendu d'un fichier déposé."""

    kind: str
    country_code: str
    rows: int
    key: str
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
    """Génère, altère et dépose un fichier par couple type / pays demandé."""
    results: list[BatchResult] = []
    file_date = request.resolved_file_date()

    for kind in request.kinds:
        allowed = eligible_countries(kind, request.entity_types)
        for country in request.countries:
            if country not in allowed:
                results.append(BatchResult(
                    kind, country, 0, "",
                    skipped_reason=f"le groupe n'opère pas de {KIND_LABELS[kind].lower()} dans ce pays",
                ))
                continue

            n_rows = request.rows.get(kind, cfg.DEFAULT_ROWS[kind])
            try:
                frame = txn.GENERATORS[kind](
                    index, country, n_rows, request.start, request.end, rng,
                    entity_types=request.entity_types or None,
                )
            except ValueError as exc:
                results.append(BatchResult(kind, country, 0, "", skipped_reason=str(exc)))
                continue

            frame, reports = ano.inject(
                kind, frame, index, country, request.anomaly_rate, rng
            )
            key = store.put_dataset(frame, kind, country, file_date)
            results.append(BatchResult(kind, country, len(frame), key, reports))

    return results
