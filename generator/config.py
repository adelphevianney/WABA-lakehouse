"""Paramètres de génération du WABA Group.

Le **domaine métier** — périmètre pays, devises, matrice pays x entité, seuils
réglementaires, nomenclatures — vit dans `common/domain.py`, parce que les jobs
PySpark en dépendent autant que le générateur. Ce module n'ajoute que ce qui est
propre à la simulation : distributions, poids, volumétries et cibles de
calibration, dont le pipeline n'a aucune raison d'avoir connaissance.

Les noms du domaine sont réexportés ici pour que les modules du générateur
gardent un point d'accès unique.
"""

from __future__ import annotations

from typing import Final

# Réexport du domaine : `from generator import config as cfg` donne accès à
# l'ensemble du vocabulaire métier sans avoir à jongler entre deux modules.
from common.domain import (  # noqa: F401
    ACCOUNT_STATUSES,
    ACCOUNT_TYPES,
    AML_THRESHOLD,
    BRANCH_TYPES,
    CHANNELS,
    CLAIM_OPERATIONS,
    CLAIM_STATUSES,
    COUNTRIES,
    COUNTRY_CODES,
    CURRENCIES,
    CURRENCY_BY_COUNTRY,
    DATASETS,
    ENTITY_COUNTRIES,
    ENTITY_TYPES,
    FRAUD_BURST_AMOUNT,
    FRAUD_BURST_MIN_COUNT,
    FRAUD_BURST_WINDOW_MINUTES,
    FRAUD_CLAIM_PREMIUM_RATIO,
    FX_PER_EUR,
    GHS_PER_EUR,
    INSURANCE_OPERATIONS,
    KYC_LEVELS,
    LOAN_TYPE_COUNTRIES,
    LOAN_TYPES,
    LOSS_RATIO_ALERT,
    MOBILE_OPERATORS,
    MOBILE_STATUSES,
    NPL_OVERDUE_DAYS,
    NPL_REGULATORY_CEILING,
    PAYMENT_TYPES,
    PREMIUM_OPERATIONS,
    PRODUCT_LINE_COUNTRIES,
    PRODUCT_LINES,
    RAW_TABLES,
    REFERENTIALS,
    REPAYMENT_STATUSES,
    SEGMENTS,
    TRANSACTION_STATUSES,
    TRANSACTION_TYPES,
    UEMOA_CODES,
    WIRE_TRANSACTION_TYPES,
    XOF_PER_EUR,
    Country,
    Locality,
    countries_of,
    entities_in,
    to_eur,
)

# =============================================================================
# Répartition géographique du portefeuille
# =============================================================================

# Poids commerciaux du groupe par pays. Une répartition uniforme donnerait au
# Togo le même portefeuille qu'au Ghana, ce qui fausserait tous les agrégats
# des tableaux de bord. Ces poids s'appuient sur les ordres de grandeur
# démographiques de chaque marché.
COUNTRY_WEIGHTS: Final[dict[str, float]] = {
    "CI": 0.187, "SN": 0.108, "ML": 0.139, "BF": 0.139,
    "GN": 0.084, "TG": 0.054, "BJ": 0.084, "GH": 0.205,
}

# Poids relatifs des entités, avant restriction géographique. Renormalisés par
# pays dans `entity_weights` : un pays sans mobile money redistribue son poids
# sur les entités réellement présentes.
ENTITY_BASE_WEIGHTS: Final[dict[str, float]] = {
    "BANK": 0.55,
    "INSURANCE": 0.20,
    "MOBILE_MONEY": 0.18,
    "MICROFINANCE": 0.07,
}


def entity_weights(country_code: str) -> tuple[tuple[str, ...], tuple[float, ...]]:
    """Entités présentes dans un pays et leurs probabilités, renormalisées."""
    entities = entities_in(country_code)
    weights = [ENTITY_BASE_WEIGHTS[e] for e in entities]
    total = sum(weights)
    return entities, tuple(w / total for w in weights)


# =============================================================================
# Distributions des nomenclatures
# =============================================================================

SEGMENT_WEIGHTS: Final[tuple[float, ...]] = (0.65, 0.20, 0.10, 0.05)
KYC_WEIGHTS: Final[tuple[float, ...]] = (0.30, 0.50, 0.20)
ACCOUNT_STATUS_WEIGHTS: Final[tuple[float, ...]] = (0.85, 0.03, 0.06, 0.06)
BRANCH_TYPE_WEIGHTS: Final[tuple[float, ...]] = (0.40, 0.15, 0.30, 0.15)
TRANSACTION_TYPE_WEIGHTS: Final[tuple[float, ...]] = (0.35, 0.30, 0.15, 0.15, 0.05)
CHANNEL_WEIGHTS: Final[tuple[float, ...]] = (0.20, 0.15, 0.35, 0.20, 0.10)
TRANSACTION_STATUS_WEIGHTS: Final[tuple[float, ...]] = (0.92, 0.05, 0.03)
PAYMENT_TYPE_WEIGHTS: Final[tuple[float, ...]] = (0.40, 0.25, 0.15, 0.12, 0.08)
MOBILE_STATUS_WEIGHTS: Final[tuple[float, ...]] = (0.94, 0.04, 0.02)

# =============================================================================
# Cibles de calibration
# =============================================================================
# L'énoncé impose que les KPIs réglementaires tombent dans des fourchettes
# réalistes. Ces cibles ne sont pas décoratives : elles pilotent activement la
# génération, faute de quoi les tables Gold du Level 2 produiraient des ratios
# aberrants.
NPL_TARGET_RANGE: Final[tuple[float, float]] = (0.03, 0.08)
LOSS_RATIO_TARGET_RANGE: Final[tuple[float, float]] = (0.50, 0.85)

# Le NPL réalisé est pondéré par les encours : il s'écarte de sa cible de
# quelques dixièmes de point, les comptes tirés en défaut n'ayant pas exactement
# le solde moyen du portefeuille. Viser l'intérieur de la fourchette absorbe
# cette dispersion et garantit que l'indicateur mesuré reste conforme.
NPL_SAMPLING_MARGIN: Final[float] = 0.006

# Part des opérations volontairement anormales, par défaut. Sans injection
# explicite, aucune des trois règles de fraude du Level 3 ne se déclencherait
# sur des données purement aléatoires.
DEFAULT_ANOMALY_RATE: Final[float] = 0.02

# =============================================================================
# Volumes et périodes par défaut (§1.1 de l'énoncé)
# =============================================================================

DEFAULT_ROWS: Final[dict[str, int]] = {
    "bank_txn": 10_000,
    "insurance_ops": 5_000,
    "mobile_money": 20_000,
    "loan_repayments": 5_000,
}

REFERENTIAL_SIZES: Final[dict[str, int]] = {
    "customers": 500_000,
    "accounts": 800_000,
    "branches": 200,
    "products": 50,
}

# Préfixe des identifiants métier : WABA-{CC}-{X}-{séquence}
ID_PREFIX: Final[str] = "WABA"
