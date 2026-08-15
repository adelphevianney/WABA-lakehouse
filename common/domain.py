"""Domaine métier du WABA Group.

Source de vérité unique du périmètre géographique, des devises, de l'implantation
des entités, des seuils réglementaires et des nomenclatures des schémas.

Ce module vit dans `common/` parce que trois exécutants distincts en dépendent :
le générateur, les jobs PySpark batch et, aux niveaux suivants, les jobs de
streaming. Un job d'ingestion qui importerait le générateur pour savoir quelle
devise attendre au Ghana inverserait la dépendance : le pipeline n'a pas à
connaître la manière dont ses données ont été produites.

Compatible Python 3.8 : c'est la version embarquée par l'image Spark 3.5, et ce
module doit pouvoir y être importé tel quel.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Dict, Final, Tuple

# =============================================================================
# Périmètre géographique
# =============================================================================


@dataclass(frozen=True)
class Locality:
    """Ville et région administrative de rattachement."""

    city: str
    region: str


@dataclass(frozen=True)
class Country:
    code: str
    name: str
    currency: str
    is_uemoa: bool
    localities: Tuple[Locality, ...]


def _loc(*pairs: Tuple[str, str]) -> Tuple[Locality, ...]:
    return tuple(Locality(city, region) for city, region in pairs)


COUNTRIES: Final[Dict[str, Country]] = {
    "CI": Country(
        "CI", "Côte d'Ivoire", "XOF", True,
        _loc(("Abidjan", "Lagunes"), ("Bouaké", "Vallée du Bandama"),
             ("Yamoussoukro", "Lacs"), ("San Pédro", "Bas-Sassandra"),
             ("Korhogo", "Savanes")),
    ),
    "SN": Country(
        "SN", "Sénégal", "XOF", True,
        _loc(("Dakar", "Dakar"), ("Thiès", "Thiès"), ("Ziguinchor", "Ziguinchor"),
             ("Saint-Louis", "Saint-Louis"), ("Kaolack", "Kaolack")),
    ),
    "ML": Country(
        "ML", "Mali", "XOF", True,
        _loc(("Bamako", "District de Bamako"), ("Sikasso", "Sikasso"),
             ("Ségou", "Ségou"), ("Mopti", "Mopti"), ("Tombouctou", "Tombouctou")),
    ),
    "BF": Country(
        "BF", "Burkina Faso", "XOF", True,
        _loc(("Ouagadougou", "Centre"), ("Bobo-Dioulasso", "Hauts-Bassins"),
             ("Koudougou", "Centre-Ouest"), ("Banfora", "Cascades")),
    ),
    # L'énoncé range la Guinée dans la zone UEMOA avec le XOF comme devise.
    # Dans la réalité, la Guinée-Conakry n'est pas membre de l'UEMOA et utilise
    # le franc guinéen (GNF) ; le huitième membre de l'union est la
    # Guinée-Bissau. On suit l'énoncé, dont dépend la grille d'évaluation.
    "GN": Country(
        "GN", "Guinée", "XOF", True,
        _loc(("Conakry", "Conakry"), ("Nzérékoré", "Nzérékoré"),
             ("Kindia", "Kindia"), ("Kankan", "Kankan")),
    ),
    "TG": Country(
        "TG", "Togo", "XOF", True,
        _loc(("Lomé", "Maritime"), ("Sokodé", "Centrale"),
             ("Kara", "Kara"), ("Atakpamé", "Plateaux")),
    ),
    "BJ": Country(
        "BJ", "Bénin", "XOF", True,
        _loc(("Cotonou", "Littoral"), ("Porto-Novo", "Ouémé"),
             ("Parakou", "Borgou"), ("Abomey-Calavi", "Atlantique")),
    ),
    "GH": Country(
        "GH", "Ghana", "GHS", False,
        _loc(("Accra", "Greater Accra"), ("Kumasi", "Ashanti"),
             ("Tamale", "Northern"), ("Cape Coast", "Central"),
             ("Sunyani", "Bono")),
    ),
}

COUNTRY_CODES: Final[Tuple[str, ...]] = tuple(COUNTRIES)
UEMOA_CODES: Final[Tuple[str, ...]] = tuple(c for c, v in COUNTRIES.items() if v.is_uemoa)
CURRENCY_BY_COUNTRY: Final[Dict[str, str]] = {c: v.currency for c, v in COUNTRIES.items()}
CURRENCIES: Final[Tuple[str, ...]] = ("XOF", "GHS")

# =============================================================================
# Matrice pays × entité
# =============================================================================
# Dérivée du tableau des entités de l'énoncé :
#   Banque Retail        UEMOA (7 pays)      -> BANK
#   Corporate Banking    UEMOA + Ghana       -> BANK
#   Mobile Money         CI, SN, BF, GH      -> MOBILE_MONEY
#   Assurance Vie        UEMOA (7 pays)      -> INSURANCE
#   Assurance IARD       UEMOA + Ghana       -> INSURANCE
#   Microfinance         ML, GN, BF          -> MICROFINANCE

ENTITY_TYPES: Final[Tuple[str, ...]] = ("BANK", "INSURANCE", "MOBILE_MONEY", "MICROFINANCE")

ENTITY_COUNTRIES: Final[Dict[str, Tuple[str, ...]]] = {
    "BANK": COUNTRY_CODES,
    "INSURANCE": COUNTRY_CODES,
    "MOBILE_MONEY": ("CI", "SN", "BF", "GH"),
    "MICROFINANCE": ("ML", "GN", "BF"),
}


def entities_in(country_code: str) -> Tuple[str, ...]:
    """Entités du groupe réellement présentes dans un pays."""
    return tuple(e for e in ENTITY_TYPES if country_code in ENTITY_COUNTRIES[e])


def countries_of(entity_type: str) -> Tuple[str, ...]:
    """Pays où une entité opère."""
    return ENTITY_COUNTRIES[entity_type]


# =============================================================================
# Conversion de devises
# =============================================================================
# Le franc CFA de l'UEMOA a une parité fixe avec l'euro depuis 1999. Ce n'est
# pas un taux de marché : c'est une constante réglementaire, et l'utiliser comme
# telle évite d'introduire une volatilité qui n'existe pas.
XOF_PER_EUR: Final[float] = 655.957

# Le cedi ghanéen flotte. Valeur par défaut surchargeable par variable
# d'environnement ; en production, ce taux viendrait d'une table de référence
# historisée ou d'un fournisseur de cours, pas d'une constante.
GHS_PER_EUR: Final[float] = float(os.getenv("WABA_GHS_PER_EUR") or 17.5)

FX_PER_EUR: Final[Dict[str, float]] = {"XOF": XOF_PER_EUR, "GHS": GHS_PER_EUR}


def to_eur(amount: float, currency: str) -> float:
    """Convertit un montant en devise locale vers l'euro."""
    return amount / FX_PER_EUR[currency]


# =============================================================================
# Seuils réglementaires
# =============================================================================
# Seuil de déclaration des transactions suspectes (Level 3.3), exprimé dans la
# devise locale de chaque zone.
AML_THRESHOLD: Final[Dict[str, float]] = {"XOF": 1_000_000.0, "GHS": 5_000.0}

# Détection de fraude par rafale (Level 3.3) : plusieurs transactions au-dessus
# de ce montant depuis un même compte dans une fenêtre courte.
FRAUD_BURST_AMOUNT: Final[Dict[str, float]] = {"XOF": 500_000.0, "GHS": 2_500.0}
FRAUD_BURST_WINDOW_MINUTES: Final[int] = 5
FRAUD_BURST_MIN_COUNT: Final[int] = 3

# Un sinistre supérieur à ce multiple de la prime annuelle est suspect.
FRAUD_CLAIM_PREMIUM_RATIO: Final[float] = 3.0

# Seuils de pilotage utilisés par les tableaux de bord du Level 4.
NPL_REGULATORY_CEILING: Final[float] = 0.05   # BCEAO : NPL < 5 %
LOSS_RATIO_ALERT: Final[float] = 0.70         # CIMA : vigilance au-delà de 70 %

# La CIMA ne régule que la zone franc : le Ghana relève de la National
# Insurance Commission. Le seuil est néanmoins appliqué aux 8 pays, l'énoncé
# ne prévoyant pas de distinction.

# Une créance est classée douteuse au-delà de ce nombre de jours d'impayé.
NPL_OVERDUE_DAYS: Final[int] = 90

# =============================================================================
# Nomenclatures des schémas (annexe A)
# =============================================================================
# Servent à la génération comme à la validation : une valeur hors nomenclature
# dans un fichier reçu est une ligne à rejeter, pas une donnée à ingérer.

SEGMENTS: Final[Tuple[str, ...]] = ("RETAIL", "SME", "CORPORATE", "PREMIUM")
KYC_LEVELS: Final[Tuple[str, ...]] = ("BASIC", "STANDARD", "ENHANCED")

ACCOUNT_TYPES: Final[Tuple[str, ...]] = (
    "CURRENT", "SAVINGS", "LOAN", "MOBILE_WALLET", "INSURANCE_POLICY",
)
ACCOUNT_STATUSES: Final[Tuple[str, ...]] = ("ACTIVE", "FROZEN", "CLOSED", "DORMANT")

BRANCH_TYPES: Final[Tuple[str, ...]] = (
    "FULL_SERVICE", "DIGITAL_ONLY", "AGENCY_BANKING", "ATM_POINT",
)

TRANSACTION_TYPES: Final[Tuple[str, ...]] = (
    "TRANSFER", "PAYMENT", "WITHDRAWAL", "DEPOSIT", "INTERNATIONAL_WIRE",
)
#: Opérations relevant de l'obligation déclarative AML : l'énoncé vise les
#: virements, pas les retraits ni les paiements marchands.
WIRE_TRANSACTION_TYPES: Final[Tuple[str, ...]] = ("TRANSFER", "INTERNATIONAL_WIRE")

CHANNELS: Final[Tuple[str, ...]] = (
    "BRANCH", "ATM", "MOBILE_APP", "INTERNET_BANKING", "USSD",
)
TRANSACTION_STATUSES: Final[Tuple[str, ...]] = ("SUCCESS", "FAILED", "REVERSED")

INSURANCE_OPERATIONS: Final[Tuple[str, ...]] = (
    "PREMIUM_PAYMENT", "CLAIM_SUBMISSION", "CLAIM_PAYMENT",
    "POLICY_RENEWAL", "POLICY_CANCELLATION",
)
CLAIM_OPERATIONS: Final[Tuple[str, ...]] = ("CLAIM_SUBMISSION", "CLAIM_PAYMENT")
PREMIUM_OPERATIONS: Final[Tuple[str, ...]] = ("PREMIUM_PAYMENT", "POLICY_RENEWAL")

PRODUCT_LINES: Final[Tuple[str, ...]] = (
    "VIE", "IARD_AUTO", "IARD_HABITATION", "IARD_SANTE", "PREVOYANCE",
)
# L'assurance Vie et la Prévoyance sont portées par WABA Assurance Vie, qui
# n'opère pas au Ghana ; les branches IARD couvrent les 8 pays.
PRODUCT_LINE_COUNTRIES: Final[Dict[str, Tuple[str, ...]]] = {
    "VIE": UEMOA_CODES,
    "PREVOYANCE": UEMOA_CODES,
    "IARD_AUTO": COUNTRY_CODES,
    "IARD_HABITATION": COUNTRY_CODES,
    "IARD_SANTE": COUNTRY_CODES,
}
CLAIM_STATUSES: Final[Tuple[str, ...]] = ("PENDING", "APPROVED", "REJECTED", "PAID")

PAYMENT_TYPES: Final[Tuple[str, ...]] = (
    "P2P", "MERCHANT_PAYMENT", "BILL_PAYMENT", "AIRTIME", "CROSS_BORDER_TRANSFER",
)
MOBILE_OPERATORS: Final[Tuple[str, ...]] = (
    "WABA_PAY", "ORANGE_MONEY_PARTNER", "MTN_PARTNER",
)
MOBILE_STATUSES: Final[Tuple[str, ...]] = ("SUCCESS", "FAILED", "PENDING")

LOAN_TYPES: Final[Tuple[str, ...]] = (
    "CONSUMER", "MORTGAGE", "SME", "AGRICULTURAL", "MICROCREDIT",
)
# Le microcrédit et le crédit agricole sont l'activité de WABA Microfinance,
# présente uniquement au Mali, en Guinée et au Burkina Faso.
LOAN_TYPE_COUNTRIES: Final[Dict[str, Tuple[str, ...]]] = {
    "CONSUMER": COUNTRY_CODES,
    "MORTGAGE": COUNTRY_CODES,
    "SME": COUNTRY_CODES,
    "AGRICULTURAL": ENTITY_COUNTRIES["MICROFINANCE"],
    "MICROCREDIT": ENTITY_COUNTRIES["MICROFINANCE"],
}
REPAYMENT_STATUSES: Final[Tuple[str, ...]] = ("ON_TIME", "LATE", "DEFAULT")

#: Taux d'intérêt annuels par type de prêt, ordres de grandeur observés en
#: Afrique de l'Ouest. Le microcrédit est nettement plus cher que le crédit
#: immobilier : coûts de distribution élevés, faibles montants, absence de
#: garantie réelle. Ces taux servent à isoler la part d'intérêt d'une échéance,
#: sans laquelle le revenu par client de l'énoncé — « commissions + intérêts
#: perçus » — n'est pas calculable, l'annexe A.7 ne distinguant pas capital et
#: intérêts dans le montant remboursé.
LOAN_ANNUAL_RATE: Final[Dict[str, float]] = {
    "MORTGAGE": 0.08,
    "AGRICULTURAL": 0.10,
    "SME": 0.12,
    "CONSUMER": 0.18,
    "MICROCREDIT": 0.24,
}

# =============================================================================
# Jeux de données de la plateforme
# =============================================================================
#: Nom du jeu de données -> table Iceberg cible et clé naturelle servant à
#: l'idempotence. Partagé par le générateur (nommage des fichiers) et par le
#: job d'ingestion (cible du MERGE).
DATASETS: Final[Dict[str, Dict[str, str]]] = {
    "bank_txn": {"table": "bank_transactions", "key": "transaction_id"},
    "insurance_ops": {"table": "insurance_operations", "key": "operation_id"},
    "mobile_money": {"table": "mobile_money_payments", "key": "payment_id"},
    "loan_repayments": {"table": "loan_repayments", "key": "repayment_id"},
}

REFERENTIALS: Final[Dict[str, Dict[str, str]]] = {
    "customers": {"table": "customers", "key": "customer_id"},
    "accounts": {"table": "accounts", "key": "account_id"},
    "branches": {"table": "branches", "key": "branch_id"},
    "products": {"table": "products", "key": "product_id"},
}

#: Les huit tables `raw.*` attendues par l'énoncé.
RAW_TABLES: Final[Tuple[str, ...]] = tuple(
    spec["table"] for spec in list(DATASETS.values()) + list(REFERENTIALS.values())
)
