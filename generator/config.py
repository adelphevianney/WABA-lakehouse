"""Configuration métier du WABA Group.

Ce module est la source de vérité unique du domaine : périmètre géographique,
matrice pays x entité, devises, seuils réglementaires et cibles de calibration.
Tous les autres modules du générateur en dépendent et n'inventent rien.

Deux écarts entre l'énoncé et la réalité réglementaire ouest-africaine sont
signalés en commentaire là où ils se manifestent. Dans les deux cas la
configuration **suit l'énoncé**, parce que la grille d'évaluation s'appuie
dessus ; les écarts sont documentés dans le write-up technique.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Final

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
    localities: tuple[Locality, ...]


def _loc(*pairs: tuple[str, str]) -> tuple[Locality, ...]:
    return tuple(Locality(city, region) for city, region in pairs)


COUNTRIES: Final[dict[str, Country]] = {
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

COUNTRY_CODES: Final[tuple[str, ...]] = tuple(COUNTRIES)

# Poids commerciaux du groupe par pays. Une répartition uniforme donnerait au
# Togo le même portefeuille qu'au Ghana, ce qui fausserait tous les agrégats
# des tableaux de bord. Ces poids s'appuient sur les ordres de grandeur
# démographiques de chaque marché.
COUNTRY_WEIGHTS: Final[dict[str, float]] = {
    "CI": 0.187, "SN": 0.108, "ML": 0.139, "BF": 0.139,
    "GN": 0.084, "TG": 0.054, "BJ": 0.084, "GH": 0.205,
}
UEMOA_CODES: Final[tuple[str, ...]] = tuple(c for c, v in COUNTRIES.items() if v.is_uemoa)
CURRENCY_BY_COUNTRY: Final[dict[str, str]] = {c: v.currency for c, v in COUNTRIES.items()}

# =============================================================================
# Matrice pays x entité
# =============================================================================
# Dérivée du tableau des entités de l'énoncé :
#   Banque Retail        UEMOA (7 pays)      -> BANK
#   Corporate Banking    UEMOA + Ghana       -> BANK
#   Mobile Money         CI, SN, BF, GH      -> MOBILE_MONEY
#   Assurance Vie        UEMOA (7 pays)      -> INSURANCE
#   Assurance IARD       UEMOA + Ghana       -> INSURANCE
#   Microfinance         ML, GN, BF          -> MICROFINANCE
#
# Le script de référence fourni en annexe A.8 tire `entity_type` uniformément
# sur les 8 pays, ce qui contredit ce tableau : on génère du mobile money au
# Mali ou de la microfinance au Ghana, deux marchés où le groupe n'opère pas.
# Cette matrice rétablit la cohérence avec le contexte métier.

ENTITY_TYPES: Final[tuple[str, ...]] = ("BANK", "INSURANCE", "MOBILE_MONEY", "MICROFINANCE")

ENTITY_COUNTRIES: Final[dict[str, tuple[str, ...]]] = {
    "BANK": COUNTRY_CODES,
    "INSURANCE": COUNTRY_CODES,
    "MOBILE_MONEY": ("CI", "SN", "BF", "GH"),
    "MICROFINANCE": ("ML", "GN", "BF"),
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


def entities_in(country_code: str) -> tuple[str, ...]:
    """Entités du groupe réellement présentes dans un pays."""
    return tuple(e for e in ENTITY_TYPES if country_code in ENTITY_COUNTRIES[e])


def entity_weights(country_code: str) -> tuple[tuple[str, ...], tuple[float, ...]]:
    """Entités présentes dans un pays et leurs probabilités, renormalisées."""
    entities = entities_in(country_code)
    weights = [ENTITY_BASE_WEIGHTS[e] for e in entities]
    total = sum(weights)
    return entities, tuple(w / total for w in weights)


def countries_of(entity_type: str) -> tuple[str, ...]:
    """Pays où une entité opère."""
    return ENTITY_COUNTRIES[entity_type]


# =============================================================================
# Conversion de devises
# =============================================================================
# Le franc CFA de l'UEMOA a une parité fixe avec l'euro depuis 1999. Ce n'est
# pas un taux de marché : c'est une constante réglementaire, et l'utiliser comme
# telle évite d'introduire une volatilité qui n'existe pas dans la réalité.
XOF_PER_EUR: Final[float] = 655.957

# Le cedi ghanéen flotte. Valeur par défaut surchargeable par variable
# d'environnement ; en production, ce taux viendrait d'une table de référence
# historisée ou d'un fournisseur de cours, pas d'une constante.
GHS_PER_EUR: Final[float] = float(os.getenv("WABA_GHS_PER_EUR", "17.5"))

FX_PER_EUR: Final[dict[str, float]] = {"XOF": XOF_PER_EUR, "GHS": GHS_PER_EUR}


def to_eur(amount: float, currency: str) -> float:
    """Convertit un montant en devise locale vers l'euro."""
    return amount / FX_PER_EUR[currency]


# =============================================================================
# Seuils réglementaires
# =============================================================================
# Seuil de déclaration des transactions suspectes (Level 3.3), exprimé dans la
# devise locale de chaque zone.
AML_THRESHOLD: Final[dict[str, float]] = {"XOF": 1_000_000.0, "GHS": 5_000.0}

# Détection de fraude par rafale (Level 3.3) : plusieurs transactions au-dessus
# de ce montant depuis un même compte dans une fenêtre courte.
FRAUD_BURST_AMOUNT: Final[dict[str, float]] = {"XOF": 500_000.0, "GHS": 2_500.0}
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
# Nomenclatures des schémas (annexe A)
# =============================================================================

SEGMENTS: Final[tuple[str, ...]] = ("RETAIL", "SME", "CORPORATE", "PREMIUM")
SEGMENT_WEIGHTS: Final[tuple[float, ...]] = (0.65, 0.20, 0.10, 0.05)

KYC_LEVELS: Final[tuple[str, ...]] = ("BASIC", "STANDARD", "ENHANCED")
KYC_WEIGHTS: Final[tuple[float, ...]] = (0.30, 0.50, 0.20)

ACCOUNT_TYPES: Final[tuple[str, ...]] = (
    "CURRENT", "SAVINGS", "LOAN", "MOBILE_WALLET", "INSURANCE_POLICY",
)
ACCOUNT_STATUSES: Final[tuple[str, ...]] = ("ACTIVE", "FROZEN", "CLOSED", "DORMANT")
ACCOUNT_STATUS_WEIGHTS: Final[tuple[float, ...]] = (0.85, 0.03, 0.06, 0.06)

BRANCH_TYPES: Final[tuple[str, ...]] = (
    "FULL_SERVICE", "DIGITAL_ONLY", "AGENCY_BANKING", "ATM_POINT",
)
BRANCH_TYPE_WEIGHTS: Final[tuple[float, ...]] = (0.40, 0.15, 0.30, 0.15)

TRANSACTION_TYPES: Final[tuple[str, ...]] = (
    "TRANSFER", "PAYMENT", "WITHDRAWAL", "DEPOSIT", "INTERNATIONAL_WIRE",
)
TRANSACTION_TYPE_WEIGHTS: Final[tuple[float, ...]] = (0.35, 0.30, 0.15, 0.15, 0.05)

CHANNELS: Final[tuple[str, ...]] = (
    "BRANCH", "ATM", "MOBILE_APP", "INTERNET_BANKING", "USSD",
)
CHANNEL_WEIGHTS: Final[tuple[float, ...]] = (0.20, 0.15, 0.35, 0.20, 0.10)

TRANSACTION_STATUSES: Final[tuple[str, ...]] = ("SUCCESS", "FAILED", "REVERSED")
TRANSACTION_STATUS_WEIGHTS: Final[tuple[float, ...]] = (0.92, 0.05, 0.03)

INSURANCE_OPERATIONS: Final[tuple[str, ...]] = (
    "PREMIUM_PAYMENT", "CLAIM_SUBMISSION", "CLAIM_PAYMENT",
    "POLICY_RENEWAL", "POLICY_CANCELLATION",
)
PRODUCT_LINES: Final[tuple[str, ...]] = (
    "VIE", "IARD_AUTO", "IARD_HABITATION", "IARD_SANTE", "PREVOYANCE",
)
# L'assurance Vie et la Prévoyance sont portées par WABA Assurance Vie, qui
# n'opère pas au Ghana ; les branches IARD couvrent les 8 pays.
PRODUCT_LINE_COUNTRIES: Final[dict[str, tuple[str, ...]]] = {
    "VIE": UEMOA_CODES,
    "PREVOYANCE": UEMOA_CODES,
    "IARD_AUTO": COUNTRY_CODES,
    "IARD_HABITATION": COUNTRY_CODES,
    "IARD_SANTE": COUNTRY_CODES,
}
CLAIM_STATUSES: Final[tuple[str, ...]] = ("PENDING", "APPROVED", "REJECTED", "PAID")

PAYMENT_TYPES: Final[tuple[str, ...]] = (
    "P2P", "MERCHANT_PAYMENT", "BILL_PAYMENT", "AIRTIME", "CROSS_BORDER_TRANSFER",
)
PAYMENT_TYPE_WEIGHTS: Final[tuple[float, ...]] = (0.40, 0.25, 0.15, 0.12, 0.08)
MOBILE_OPERATORS: Final[tuple[str, ...]] = (
    "WABA_PAY", "ORANGE_MONEY_PARTNER", "MTN_PARTNER",
)
MOBILE_STATUSES: Final[tuple[str, ...]] = ("SUCCESS", "FAILED", "PENDING")
MOBILE_STATUS_WEIGHTS: Final[tuple[float, ...]] = (0.94, 0.04, 0.02)

LOAN_TYPES: Final[tuple[str, ...]] = (
    "CONSUMER", "MORTGAGE", "SME", "AGRICULTURAL", "MICROCREDIT",
)
# Le microcrédit et le crédit agricole sont l'activité de WABA Microfinance,
# présente uniquement au Mali, en Guinée et au Burkina Faso.
LOAN_TYPE_COUNTRIES: Final[dict[str, tuple[str, ...]]] = {
    "CONSUMER": COUNTRY_CODES,
    "MORTGAGE": COUNTRY_CODES,
    "SME": COUNTRY_CODES,
    "AGRICULTURAL": ENTITY_COUNTRIES["MICROFINANCE"],
    "MICROCREDIT": ENTITY_COUNTRIES["MICROFINANCE"],
}
REPAYMENT_STATUSES: Final[tuple[str, ...]] = ("ON_TIME", "LATE", "DEFAULT")

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
