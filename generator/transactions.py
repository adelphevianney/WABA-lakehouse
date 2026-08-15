"""Génération des flux transactionnels du WABA Group.

Quatre flux, correspondant aux schémas A.4 à A.7 : transactions bancaires,
opérations d'assurance, paiements mobile money et remboursements de crédit.

Toutes les clés étrangères proviennent des référentiels via `ReferentialIndex`,
qui pré-découpe les tables par pays une fois pour toutes. Le mode « continue »
de l'interface régénère un lot toutes les 10 à 60 secondes : refiltrer 800 000
comptes à chaque tour coûterait plus cher que la génération elle-même.

Les montants sont tirés en euros puis convertis, comme pour les référentiels,
et paramétrés **par type d'opération** plutôt que globalement. Un virement
international et un retrait au distributeur n'ont pas le même ordre de
grandeur ; les confondre placerait aléatoirement les alertes AML sur des
retraits de guichet, ce qui n'aurait aucun sens métier.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime

import numpy as np
import pandas as pd

from generator import calibration as calib
from generator import config as cfg

# =============================================================================
# Index des référentiels
# =============================================================================


@dataclass
class ReferentialIndex:
    """Vues pré-découpées des référentiels, indexées par pays.

    Construit une fois après la génération (ou le chargement) des référentiels,
    puis réutilisé pour tous les lots de transactions.
    """

    customers: pd.DataFrame
    accounts: pd.DataFrame
    branches: pd.DataFrame
    products: pd.DataFrame

    _by_country: dict[str, dict[str, pd.DataFrame]] = field(default_factory=dict, repr=False)
    _filtered: dict[tuple[str, str, tuple[str, ...]], pd.DataFrame] = field(
        default_factory=dict, repr=False
    )

    def __post_init__(self) -> None:
        for country in self.accounts["country_code"].unique():
            accounts = self.accounts[self.accounts["country_code"] == country]
            customers = self.customers[self.customers["country_code"] == country]
            self._by_country[country] = {
                # Comptes mouvementables par une transaction bancaire : ni
                # portefeuille mobile, ni police d'assurance.
                "bank": accounts[accounts["account_type"].isin(("CURRENT", "SAVINGS"))],
                "loan": accounts[accounts["account_type"] == "LOAN"],
                "policy": accounts[accounts["account_type"] == "INSURANCE_POLICY"],
                "wallet": accounts[accounts["account_type"] == "MOBILE_WALLET"],
                "all_accounts": accounts,
                "customers": customers,
                "wallet_customers": customers[customers["entity_type"] == "MOBILE_MONEY"],
                "insurance_customers": customers[customers["entity_type"] == "INSURANCE"],
                "branches": self.branches[self.branches["country_code"] == country],
            }

    def view(
        self,
        country_code: str,
        key: str,
        entity_types: tuple[str, ...] | None = None,
    ) -> pd.DataFrame:
        """Vue pré-découpée, éventuellement restreinte à certaines lignes métier.

        Le filtre par entité sert la sélection « ligne métier » de l'interface :
        restreindre à MICROFINANCE ne fait plus apparaître que les mouvements
        des comptes portés par cette entité.
        """
        try:
            frame = self._by_country[country_code][key]
        except KeyError as exc:
            raise KeyError(
                f"aucune vue '{key}' pour le pays {country_code} — "
                "les référentiels sont-ils bien chargés ?"
            ) from exc

        if not entity_types:
            return frame

        cache_key = (country_code, key, entity_types)
        if cache_key not in self._filtered:
            self._filtered[cache_key] = frame[frame["entity_type"].isin(entity_types)]
        return self._filtered[cache_key]

    @property
    def countries(self) -> tuple[str, ...]:
        return tuple(self._by_country)


# =============================================================================
# Utilitaires
# =============================================================================


def _timestamps(start: datetime, end: datetime, n: int, rng: np.random.Generator) -> np.ndarray:
    """Horodatages uniformes dans la période simulée, à la seconde près."""
    start64 = np.datetime64(start, "s")
    span = max(int((np.datetime64(end, "s") - start64).astype(int)), 1)
    return start64 + rng.integers(0, span, size=n).astype("timedelta64[s]")


def _round_currency(amounts: np.ndarray, currency: str) -> np.ndarray:
    return np.round(amounts, 0 if currency == "XOF" else 2)


def _local_amounts(
    amount_eur: np.ndarray, currency: str
) -> np.ndarray:
    return _round_currency(amount_eur * cfg.FX_PER_EUR[currency], currency)


def _sample_rows(frame: pd.DataFrame, n: int, rng: np.random.Generator) -> pd.DataFrame:
    """Tirage avec remise dans un référentiel, sans copie coûteuse."""
    if frame.empty:
        raise ValueError("référentiel vide : impossible de générer des clés valides")
    return frame.iloc[rng.integers(0, len(frame), size=n)]


def _fees(amounts: np.ndarray, statuses: np.ndarray, currency: str) -> np.ndarray:
    """Frais bancaires : 0,1 % du montant, uniquement sur les opérations abouties."""
    return np.where(statuses == "SUCCESS", _round_currency(amounts * 0.001, currency), 0.0)


# =============================================================================
# A.4 — Transactions bancaires
# =============================================================================

#: Paramètres lognormaux du montant en euros, par type de transaction. Les
#: virements internationaux concentrent naturellement les montants élevés :
#: c'est là que doivent se déclencher les alertes AML, pas sur des retraits.
_BANK_AMOUNT_EUR: dict[str, tuple[float, float]] = {
    "TRANSFER": (5.20, 1.15),
    "PAYMENT": (4.50, 1.00),
    "WITHDRAWAL": (4.20, 0.90),
    "DEPOSIT": (4.90, 1.10),
    "INTERNATIONAL_WIRE": (7.80, 0.90),
}


def generate_bank_transactions(
    index: ReferentialIndex,
    country_code: str,
    n_rows: int,
    start: datetime,
    end: datetime,
    rng: np.random.Generator,
    entity_types: tuple[str, ...] | None = None,
) -> pd.DataFrame:
    """Transactions bancaires d'un pays (schéma A.4)."""
    currency = cfg.CURRENCY_BY_COUNTRY[country_code]

    debit = _sample_rows(index.view(country_code, "bank", entity_types), n_rows, rng)

    # L'agence d'origine doit relever de la même entité que le compte débité :
    # une opération de microfinance ne peut pas être initiée depuis une agence
    # de la banque de détail. Un tirage global produirait des rapprochements
    # incohérents dès qu'on joint les transactions au référentiel des agences.
    debit_entities = debit["entity_type"].to_numpy()
    branch_ids = np.empty(n_rows, dtype=object)
    for entity in np.unique(debit_entities):
        mask = debit_entities == entity
        pool = index.view(country_code, "branches", (entity,))["branch_id"].to_numpy()
        branch_ids[mask] = pool[rng.integers(0, len(pool), size=int(mask.sum()))]

    txn_types = rng.choice(
        np.array(cfg.TRANSACTION_TYPES), size=n_rows, p=np.array(cfg.TRANSACTION_TYPE_WEIGHTS)
    )

    # Un virement international sort du pays : le bénéficiaire est tiré dans un
    # autre pays du groupe, les autres opérations restent domestiques.
    is_wire = txn_types == "INTERNATIONAL_WIRE"
    beneficiary = _sample_rows(index.view(country_code, "all_accounts"), n_rows, rng)["account_id"].to_numpy()
    if is_wire.any():
        others = [c for c in index.countries if c != country_code]
        foreign_country = rng.choice(np.array(others), size=int(is_wire.sum()))
        foreign_ids = np.empty(int(is_wire.sum()), dtype=object)
        for code in np.unique(foreign_country):
            mask = foreign_country == code
            pool = index.view(code, "all_accounts")["account_id"].to_numpy()
            foreign_ids[mask] = pool[rng.integers(0, len(pool), size=int(mask.sum()))]
        beneficiary = beneficiary.copy()
        beneficiary[is_wire] = foreign_ids

    mu = np.array([_BANK_AMOUNT_EUR[t][0] for t in txn_types])
    sigma = np.array([_BANK_AMOUNT_EUR[t][1] for t in txn_types])
    amounts = _local_amounts(rng.lognormal(mu, sigma), currency)
    # L'énoncé plancher les montants à 500 unités de devise locale.
    amounts = np.maximum(amounts, 500 if currency == "XOF" else 5)

    statuses = rng.choice(
        np.array(cfg.TRANSACTION_STATUSES), size=n_rows, p=np.array(cfg.TRANSACTION_STATUS_WEIGHTS)
    )

    return pd.DataFrame({
        "transaction_id": [str(u) for u in _uuids(n_rows, rng)],
        "timestamp": _timestamps(start, end, n_rows, rng),
        "account_id": debit["account_id"].to_numpy(),
        "beneficiary_account": beneficiary,
        "branch_id": branch_ids,
        "country_code": country_code,
        "transaction_type": txn_types,
        "amount": amounts,
        "currency": currency,
        "channel": rng.choice(np.array(cfg.CHANNELS), size=n_rows, p=np.array(cfg.CHANNEL_WEIGHTS)),
        "transaction_status": statuses,
        "fee_amount": _fees(amounts, statuses, currency),
        "entity_type": debit["entity_type"].to_numpy(),
    })


def _uuids(n: int, rng: np.random.Generator) -> np.ndarray:
    """UUID v4 déterministes vis-à-vis du générateur fourni.

    `uuid.uuid4()` ignore le générateur et empêcherait de rejouer une
    génération à l'identique, ce dont dépendent les tests et la démonstration
    de l'idempotence.
    """
    raw = rng.integers(0, 256, size=(n, 16), dtype=np.uint8)
    raw[:, 6] = (raw[:, 6] & 0x0F) | 0x40   # version 4
    raw[:, 8] = (raw[:, 8] & 0x3F) | 0x80   # variante RFC 4122
    hexes = np.array([bytes(row).hex() for row in raw])
    return np.array([
        f"{h[:8]}-{h[8:12]}-{h[12:16]}-{h[16:20]}-{h[20:]}" for h in hexes
    ])


# =============================================================================
# A.5 — Opérations d'assurance
# =============================================================================

_INSURANCE_OPERATION_WEIGHTS: tuple[float, ...] = (0.55, 0.15, 0.12, 0.13, 0.05)


def generate_insurance_operations(
    index: ReferentialIndex,
    country_code: str,
    n_rows: int,
    start: datetime,
    end: datetime,
    rng: np.random.Generator,
    entity_types: tuple[str, ...] | None = None,   # non utilisé : entité fixée
) -> pd.DataFrame:
    """Opérations d'assurance d'un pays (schéma A.5).

    Le montant des sinistres n'est pas tiré indépendamment : il est dimensionné
    à partir des primes réellement générées dans le lot, de façon à respecter le
    loss ratio cible du couple pays / branche.
    """
    currency = cfg.CURRENCY_BY_COUNTRY[country_code]
    policies = _sample_rows(index.view(country_code, "policy"), n_rows, rng)

    operations = rng.choice(
        np.array(cfg.INSURANCE_OPERATIONS), size=n_rows, p=np.array(_INSURANCE_OPERATION_WEIGHTS)
    )
    lines = np.array([
        line for line in cfg.PRODUCT_LINES if country_code in cfg.PRODUCT_LINE_COUNTRIES[line]
    ])
    product_lines = rng.choice(lines, size=n_rows)

    amounts = np.zeros(n_rows)

    # Prime annuelle de référence, attachée à la police et non à l'opération.
    annual_premium = calib.annual_premium_eur(policies["account_id"])

    # Versements de prime : échéances mensuelles de la cotisation annuelle.
    is_premium = np.isin(operations, ("PREMIUM_PAYMENT", "POLICY_RENEWAL"))
    amounts[is_premium] = (
        annual_premium[is_premium] / 12 * rng.uniform(0.95, 1.05, int(is_premium.sum()))
    )

    # Sinistres réglés : calibrés sur l'enveloppe de primes du lot.
    is_claim_paid = operations == "CLAIM_PAYMENT"
    amounts[is_claim_paid] = calib.claim_amounts_for_premiums(
        premium_amounts=amounts[is_premium],
        premium_lines=product_lines[is_premium],
        claim_lines=product_lines[is_claim_paid],
        claim_annual_premiums=annual_premium[is_claim_paid],
        country_code=country_code,
        rng=rng,
    )

    # Déclarations de sinistre : montant estimé, du même ordre que les règlements.
    is_claim_open = operations == "CLAIM_SUBMISSION"
    amounts[is_claim_open] = (
        annual_premium[is_claim_open] * 0.35 * rng.lognormal(0.0, 0.6, int(is_claim_open.sum()))
    )

    # Une résiliation ne porte pas de montant.
    is_claim = np.isin(operations, ("CLAIM_SUBMISSION", "CLAIM_PAYMENT"))

    claim_status = np.where(
        is_claim,
        rng.choice(np.array(cfg.CLAIM_STATUSES), size=n_rows, p=np.array([0.20, 0.25, 0.10, 0.45])),
        None,
    )
    # Le délai de traitement n'a de sens que pour un sinistre.
    processing = rng.integers(1, 46, size=n_rows).astype(float)
    processing_days = np.where(is_claim, processing, np.nan)

    return pd.DataFrame({
        "operation_id": _uuids(n_rows, rng),
        "timestamp": _timestamps(start, end, n_rows, rng),
        "customer_id": policies["customer_id"].to_numpy(),
        "account_id": policies["account_id"].to_numpy(),
        "country_code": country_code,
        "operation_type": operations,
        "product_line": product_lines,
        "amount": _local_amounts(amounts, currency),
        "currency": currency,
        "claim_status": claim_status,
        "processing_days": processing_days,
        "entity_type": "INSURANCE",
    })


# =============================================================================
# A.6 — Paiements mobile money
# =============================================================================


def generate_mobile_money(
    index: ReferentialIndex,
    country_code: str,
    n_rows: int,
    start: datetime,
    end: datetime,
    rng: np.random.Generator,
    entity_types: tuple[str, ...] | None = None,   # non utilisé : entité fixée
) -> pd.DataFrame:
    """Paiements mobile money d'un pays (schéma A.6).

    L'émetteur est toujours un client WABA Mobile Money, entité présente
    uniquement en Côte d'Ivoire, au Sénégal, au Burkina Faso et au Ghana. Le
    destinataire, lui, peut être un client du groupe dans n'importe quel pays :
    c'est ainsi que se forment les corridors transfrontaliers attendus par
    l'énoncé (CI vers ML par exemple) sans contredire la matrice pays x entité,
    le règlement dans le pays d'arrivée passant par un opérateur partenaire.
    """
    currency = cfg.CURRENCY_BY_COUNTRY[country_code]
    senders = _sample_rows(index.view(country_code, "wallet_customers"), n_rows, rng)

    payment_types = rng.choice(
        np.array(cfg.PAYMENT_TYPES), size=n_rows, p=np.array(cfg.PAYMENT_TYPE_WEIGHTS)
    )
    is_cross_border = payment_types == "CROSS_BORDER_TRANSFER"

    receiver_country = np.full(n_rows, country_code, dtype=object)
    others = [c for c in index.countries if c != country_code]
    if is_cross_border.any() and others:
        receiver_country[is_cross_border] = rng.choice(
            np.array(others), size=int(is_cross_border.sum())
        )

    receiver_id = np.empty(n_rows, dtype=object)
    for code in np.unique(receiver_country):
        mask = receiver_country == code
        pool = index.view(code, "customers")["customer_id"].to_numpy()
        receiver_id[mask] = pool[rng.integers(0, len(pool), size=int(mask.sum()))]

    # Les transferts transfrontaliers portent des montants plus élevés que les
    # paiements du quotidien.
    mu = np.where(is_cross_border, 4.8, 3.4)
    amounts = _local_amounts(rng.lognormal(mu, 1.0), currency)
    statuses = rng.choice(
        np.array(cfg.MOBILE_STATUSES), size=n_rows, p=np.array(cfg.MOBILE_STATUS_WEIGHTS)
    )

    return pd.DataFrame({
        "payment_id": _uuids(n_rows, rng),
        "timestamp": _timestamps(start, end, n_rows, rng),
        "sender_id": senders["customer_id"].to_numpy(),
        "receiver_id": receiver_id,
        "sender_country": country_code,
        "receiver_country": receiver_country,
        "amount": amounts,
        "currency": currency,
        "payment_type": payment_types,
        "operator": rng.choice(np.array(cfg.MOBILE_OPERATORS), size=n_rows, p=np.array([0.60, 0.22, 0.18])),
        "status": statuses,
        "fee_amount": _fees(amounts, statuses, currency),
        "entity_type": "MOBILE_MONEY",
        "country_code": country_code,
    })


# =============================================================================
# A.7 — Remboursements de crédit
# =============================================================================


def generate_loan_repayments(
    index: ReferentialIndex,
    country_code: str,
    n_rows: int,
    start: datetime,
    end: datetime,
    rng: np.random.Generator,
    entity_types: tuple[str, ...] | None = None,
) -> pd.DataFrame:
    """Remboursements de crédit d'un pays (schéma A.7).

    Le statut de remboursement n'est pas tiré au hasard : il découle de l'état
    du compte de prêt, lui-même déterminé par la cible de NPL du pays. C'est ce
    qui rend `gold.npl_ratio_by_country` interprétable.
    """
    currency = cfg.CURRENCY_BY_COUNTRY[country_code]
    loans = _sample_rows(
        index.view(country_code, "loan", entity_types), n_rows, rng
    ).reset_index(drop=True)

    defaulting = calib.is_defaulting_account(loans["account_id"], loans["country_code"])
    days_overdue = calib.overdue_days_for(loans["account_id"], defaulting)
    in_default = defaulting.to_numpy()

    # Parmi les comptes sains, une part reste en retard sans être douteuse.
    late = (~in_default) & (rng.random(n_rows) < 0.13)
    status = np.where(in_default, "DEFAULT", np.where(late, "LATE", "ON_TIME"))
    days_overdue = np.where(in_default, days_overdue, np.where(late, np.maximum(days_overdue, 1), 0))

    # Échéance mensuelle : fraction du capital restant dû.
    amount_due = _round_currency(
        loans["credit_limit"].to_numpy() * rng.uniform(0.01, 0.05, n_rows), currency
    )
    # Un compte en défaut ne paie rien ; un retard paie partiellement.
    ratio = np.where(in_default, 0.0, np.where(late, rng.uniform(0.3, 0.9, n_rows), 1.0))
    amount_paid = _round_currency(amount_due * ratio, currency)

    due_date = _timestamps(start, end, n_rows, rng).astype("datetime64[D]")
    payment_date = due_date + days_overdue.astype("timedelta64[D]")
    payment_date = np.where(in_default, np.datetime64("NaT"), payment_date)

    # Le type de prêt doit rester cohérent avec l'entité porteuse — le
    # microcrédit et le crédit agricole relèvent de WABA Microfinance — et
    # surtout rester stable pour un même compte : c'est une caractéristique du
    # contrat, pas de chaque échéance.
    entities = loans["entity_type"].to_numpy()
    loan_types = np.empty(n_rows, dtype=object)
    for entity in np.unique(entities):
        mask = entities == entity
        eligible = [
            t for t in cfg.LOAN_TYPES
            if country_code in cfg.LOAN_TYPE_COUNTRIES[t]
            and (entity == "MICROFINANCE") == (t in ("MICROCREDIT", "AGRICULTURAL"))
        ]
        if not eligible:
            eligible = [t for t in cfg.LOAN_TYPES if country_code in cfg.LOAN_TYPE_COUNTRIES[t]]
        indices = calib.loan_type_index(loans.loc[mask, "account_id"], len(eligible))
        loan_types[mask] = np.array(eligible)[indices]

    # Part d'intérêt réellement encaissée. Une échéance s'impute d'abord sur les
    # intérêts courus, puis sur le capital : on ne peut donc pas percevoir plus
    # d'intérêts que le montant effectivement payé.
    annual_rate = np.array([cfg.LOAN_ANNUAL_RATE[t] for t in loan_types])
    interet_du = loans["balance"].to_numpy() * annual_rate / 12.0
    interest_amount = _round_currency(np.minimum(interet_du, amount_paid), currency)

    return pd.DataFrame({
        "repayment_id": _uuids(n_rows, rng),
        "timestamp": _timestamps(start, end, n_rows, rng),
        "loan_account_id": loans["account_id"].to_numpy(),
        "customer_id": loans["customer_id"].to_numpy(),
        "country_code": country_code,
        "amount_due": amount_due,
        "amount_paid": amount_paid,
        # Colonne ajoutée au schéma A.7 : sans elle, le revenu par client de
        # l'énoncé — « commissions + intérêts perçus » — n'est pas calculable.
        "interest_amount": interest_amount,
        "currency": currency,
        "due_date": due_date,
        "payment_date": payment_date,
        "days_overdue": days_overdue.astype(int),
        "loan_type": loan_types,
        "repayment_status": status,
        "entity_type": entities,
    })


#: Aiguillage utilisé par l'interface et le service de génération.
GENERATORS = {
    "bank_txn": generate_bank_transactions,
    "insurance_ops": generate_insurance_operations,
    "mobile_money": generate_mobile_money,
    "loan_repayments": generate_loan_repayments,
}
