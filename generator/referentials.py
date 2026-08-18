"""Génération des référentiels du WABA Group.

Quatre référentiels, générés une seule fois et partagés entre pays :
customers, accounts, branches, products. Ils constituent le socle d'intégrité
référentielle : toute transaction générée ensuite ne référence que des clés
issues de ces tables.

Toute la génération est **vectorisée**. Le script de référence de l'annexe A.8
appelle `np.random.choice` ligne par ligne, ce qui coûte plusieurs minutes pour
500 000 clients ; en raisonnant par colonnes, la même volumétrie tient en
quelques secondes. La différence est visible en démonstration.

Les montants sont tirés en euros puis convertis vers la devise locale. Tirer
directement en devise locale imposerait des paramètres distincts par zone, et
un oubli produirait des soldes ghanéens à l'échelle du franc CFA, soit un
facteur 37 d'erreur.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from generator import calibration as calib
from generator import config as cfg

# =============================================================================
# Utilitaires vectorisés
# =============================================================================


def _draw_countries(n: int, rng: np.random.Generator) -> np.ndarray:
    """Tire n pays selon les poids commerciaux du groupe."""
    codes = np.array(cfg.COUNTRY_CODES)
    weights = np.array([cfg.COUNTRY_WEIGHTS[c] for c in cfg.COUNTRY_CODES])
    return rng.choice(codes, size=n, p=weights / weights.sum())


def _draw_entities(countries: np.ndarray, rng: np.random.Generator) -> np.ndarray:
    """Tire une entité par ligne, en ne proposant que celles présentes dans le pays.

    C'est ici que la matrice pays x entité est réellement appliquée : aucun
    client mobile money ne peut apparaître au Mali, aucune microfinance au Ghana.
    """
    out = np.empty(len(countries), dtype=object)
    for code in cfg.COUNTRY_CODES:
        mask = countries == code
        count = int(mask.sum())
        if count:
            entities, weights = cfg.entity_weights(code)
            out[mask] = rng.choice(np.array(entities), size=count, p=np.array(weights))
    return out


def _draw_localities(countries: np.ndarray, rng: np.random.Generator) -> tuple[np.ndarray, np.ndarray]:
    """Tire une ville et sa région administrative, cohérentes avec le pays."""
    cities = np.empty(len(countries), dtype=object)
    regions = np.empty(len(countries), dtype=object)
    for code in cfg.COUNTRY_CODES:
        mask = countries == code
        count = int(mask.sum())
        if count:
            localities = cfg.COUNTRIES[code].localities
            picked = rng.integers(0, len(localities), size=count)
            cities[mask] = [localities[i].city for i in picked]
            regions[mask] = [localities[i].region for i in picked]
    return cities, regions


def _build_ids(countries: np.ndarray, kind: str, width: int) -> pd.Series:
    """Construit des identifiants de la forme WABA-CI-C-000001.

    La séquence est globale et non remise à zéro par pays : elle garantit
    l'unicité sans avoir à gérer un compteur par pays.
    """
    sequence = pd.Series(np.arange(1, len(countries) + 1)).astype(str).str.zfill(width)
    return "WABA-" + pd.Series(countries).astype(str) + f"-{kind}-" + sequence


def _random_dates(
    start: str, end: str, n: int, rng: np.random.Generator
) -> np.ndarray:
    """Tire n dates uniformément entre deux bornes incluses."""
    start_d = np.datetime64(start, "D")
    span = int((np.datetime64(end, "D") - start_d).astype(int))
    return start_d + rng.integers(0, span + 1, size=n).astype("timedelta64[D]")


def _amounts_from_eur(
    amount_eur: np.ndarray, currencies: np.ndarray
) -> np.ndarray:
    """Convertit des montants en euros vers les devises locales.

    Le franc CFA n'a pas de subdivision en usage : les montants XOF sont
    arrondis à l'unité, les montants GHS au centime.
    """
    out = np.empty(len(amount_eur), dtype=float)
    for currency, rate in cfg.FX_PER_EUR.items():
        mask = currencies == currency
        if mask.any():
            decimals = 0 if currency == "XOF" else 2
            out[mask] = np.round(amount_eur[mask] * rate, decimals)
    return out


# =============================================================================
# Branches
# =============================================================================


def generate_branches(
    n: int = cfg.REFERENTIAL_SIZES["branches"],
    rng: np.random.Generator | None = None,
) -> pd.DataFrame:
    """Référentiel des agences (schéma A.3).

    Les 23 premières agences couvrent explicitement chaque couple pays x entité
    valide. Sans cette garantie, un tirage aléatoire sur 200 lignes peut laisser
    un couple sans aucune agence, et les transactions de ce périmètre se
    retrouveraient sans `branch_id` référençable — précisément la clé orpheline
    que l'énoncé interdit.
    """
    rng = rng or np.random.default_rng()

    covered = [
        (country, entity)
        for entity in cfg.ENTITY_TYPES
        for country in cfg.countries_of(entity)
    ]
    if n < len(covered):
        raise ValueError(
            f"{n} agences ne suffisent pas à couvrir les {len(covered)} couples pays x entité"
        )

    remaining = n - len(covered)
    random_countries = _draw_countries(remaining, rng)
    countries = np.concatenate([np.array([c for c, _ in covered]), random_countries])
    entities = np.concatenate([
        np.array([e for _, e in covered]),
        _draw_entities(random_countries, rng),
    ])
    cities, regions = _draw_localities(countries, rng)

    return pd.DataFrame({
        "branch_id": _build_ids(countries, "B", 3),
        "country_code": countries,
        "entity_type": entities,
        "city": cities,
        "region": regions,
        "branch_type": rng.choice(
            np.array(cfg.BRANCH_TYPES), size=n, p=np.array(cfg.BRANCH_TYPE_WEIGHTS)
        ),
        "is_active": rng.random(n) > 0.05,
    })


# =============================================================================
# Products
# =============================================================================

#: Catalogue de base, décliné ensuite par pays pour atteindre la volumétrie
#: demandée. Chaque produit reste rattaché à l'entité qui le commercialise.
_PRODUCT_TEMPLATES: tuple[tuple[str, str, str], ...] = (
    ("BANK", "COMPTE", "Compte Courant Particulier"),
    ("BANK", "COMPTE", "Compte Épargne Rémunéré"),
    ("BANK", "CREDIT", "Crédit Consommation"),
    ("BANK", "CREDIT", "Crédit Immobilier"),
    ("BANK", "CREDIT", "Crédit Investissement PME"),
    ("BANK", "SERVICE", "Pack Cash Management Entreprise"),
    ("INSURANCE", "VIE", "Assurance Vie Épargne"),
    ("INSURANCE", "PREVOYANCE", "Prévoyance Famille"),
    ("INSURANCE", "IARD_AUTO", "Assurance Automobile Tous Risques"),
    ("INSURANCE", "IARD_HABITATION", "Multirisque Habitation"),
    ("INSURANCE", "IARD_SANTE", "Complémentaire Santé"),
    ("MOBILE_MONEY", "WALLET", "Portefeuille WABA Pay"),
    ("MOBILE_MONEY", "TRANSFERT", "Transfert Transfrontalier UEMOA"),
    ("MOBILE_MONEY", "SERVICE", "Paiement Marchand QR"),
    ("MICROFINANCE", "CREDIT", "Microcrédit Solidaire"),
    ("MICROFINANCE", "CREDIT", "Crédit Agricole Campagne"),
    ("MICROFINANCE", "EPARGNE", "Tontine Digitalisée"),
)


def generate_products(
    n: int = cfg.REFERENTIAL_SIZES["products"],
    rng: np.random.Generator | None = None,
) -> pd.DataFrame:
    """Catalogue produits, décliné par pays.

    L'annexe ne fournit pas de schéma pour ce référentiel ; celui-ci respecte la
    contrainte transverse imposant `country_code` et `entity_type` sur toutes
    les tables, ce qui suppose des produits déclinés pays par pays plutôt qu'un
    catalogue groupe unique.
    """
    rng = rng or np.random.default_rng()

    rows: list[dict[str, object]] = []
    for entity, category, name in _PRODUCT_TEMPLATES:
        for country in cfg.countries_of(entity):
            # Les branches Vie et Prévoyance ne sont pas commercialisées au Ghana.
            if category in cfg.PRODUCT_LINE_COUNTRIES and country not in cfg.PRODUCT_LINE_COUNTRIES[category]:
                continue
            rows.append({
                "country_code": country,
                "entity_type": entity,
                "category": category,
                "product_name": name,
            })

    catalogue = pd.DataFrame(rows)
    # Échantillonnage sans remise pour atteindre exactement la volumétrie voulue
    # tout en gardant une couverture représentative des pays et des entités.
    if len(catalogue) > n:
        catalogue = catalogue.sample(n=n, random_state=int(rng.integers(0, 2**31)))
    catalogue = catalogue.sort_values(["country_code", "entity_type"]).reset_index(drop=True)

    catalogue.insert(0, "product_id", _build_ids(catalogue["country_code"].to_numpy(), "P", 3))
    catalogue["currency"] = catalogue["country_code"].map(cfg.CURRENCY_BY_COUNTRY)
    catalogue["launch_date"] = _random_dates("2015-01-01", "2025-06-30", len(catalogue), rng)
    catalogue["is_active"] = rng.random(len(catalogue)) > 0.10
    return catalogue


# =============================================================================
# Customers
# =============================================================================


def generate_customers(
    n: int = cfg.REFERENTIAL_SIZES["customers"],
    rng: np.random.Generator | None = None,
) -> pd.DataFrame:
    """Référentiel clients (schéma A.1)."""
    rng = rng or np.random.default_rng()

    countries = _draw_countries(n, rng)
    entities = _draw_entities(countries, rng)
    _, regions = _draw_localities(countries, rng)

    return pd.DataFrame({
        "customer_id": _build_ids(countries, "C", 6),
        "country_code": countries,
        "entity_type": entities,
        "segment": rng.choice(
            np.array(cfg.SEGMENTS), size=n, p=np.array(cfg.SEGMENT_WEIGHTS)
        ),
        "kyc_level": rng.choice(
            np.array(cfg.KYC_LEVELS), size=n, p=np.array(cfg.KYC_WEIGHTS)
        ),
        "onboarding_date": _random_dates("2010-01-01", "2025-12-31", n, rng),
        "region": regions,
        "is_active": rng.random(n) > 0.08,
    })


# =============================================================================
# Accounts
# =============================================================================

#: Types de compte ouverts par une entité donnée, et leur répartition. Un client
#: mobile money ne détient qu'un portefeuille, un assuré qu'une police.
_ACCOUNT_TYPES_BY_ENTITY: dict[str, tuple[tuple[str, ...], tuple[float, ...]]] = {
    "BANK": (("CURRENT", "SAVINGS", "LOAN"), (0.45, 0.35, 0.20)),
    "MICROFINANCE": (("LOAN", "SAVINGS"), (0.55, 0.45)),
    "MOBILE_MONEY": (("MOBILE_WALLET",), (1.0,)),
    "INSURANCE": (("INSURANCE_POLICY",), (1.0,)),
}

#: Paramètres lognormaux du solde, en euros, par type de compte.
_BALANCE_PARAMS_EUR: dict[str, tuple[float, float]] = {
    "CURRENT": (6.2, 1.1),
    "SAVINGS": (7.0, 1.2),
    "LOAN": (8.2, 1.0),
    "MOBILE_WALLET": (3.5, 1.3),
    "INSURANCE_POLICY": (6.8, 0.9),
}


def _draw_account_types(entities: np.ndarray, rng: np.random.Generator) -> np.ndarray:
    out = np.empty(len(entities), dtype=object)
    for entity, (types, weights) in _ACCOUNT_TYPES_BY_ENTITY.items():
        mask = entities == entity
        count = int(mask.sum())
        if count:
            out[mask] = rng.choice(np.array(types), size=count, p=np.array(weights))
    return out


def _build_ibans(
    countries: np.ndarray, sequence: np.ndarray, rng: np.random.Generator
) -> pd.Series:
    """Construit des IBAN de format plausible, nuls pour le Ghana.

    Le Ghana ne fait pas partie du registre IBAN : ses banques identifient les
    comptes par un numéro national. Ce null n'est donc pas une donnée manquante
    mais une réalité métier — et il donne à la couche Silver un vrai cas de
    gestion des valeurs nulles à traiter.
    """
    n = len(countries)
    bank = pd.Series(rng.integers(10_000, 99_999, n)).astype(str)
    branch = pd.Series(rng.integers(10_000, 99_999, n)).astype(str)
    account = pd.Series(sequence).astype(str).str.zfill(12)
    check = pd.Series(rng.integers(10, 99, n)).astype(str)
    key = pd.Series(rng.integers(10, 99, n)).astype(str)

    iban = pd.Series(countries).astype(str) + check + bank + branch + account + key
    return iban.where(pd.Series(countries) != "GH", other=None)


def generate_accounts(
    customers: pd.DataFrame,
    n: int = cfg.REFERENTIAL_SIZES["accounts"],
    rng: np.random.Generator | None = None,
) -> pd.DataFrame:
    """Référentiel comptes (schéma A.2), rattaché au référentiel clients.

    Les `n_customers` premiers comptes sont attribués à un client distinct, ce
    qui garantit qu'aucun client n'est dépourvu de compte ; le reste est
    réparti aléatoirement, produisant des clients multi-équipés.
    """
    rng = rng or np.random.default_rng()

    n_customers = len(customers)
    if n < n_customers:
        owner_idx = rng.choice(n_customers, size=n, replace=False)
    else:
        owner_idx = np.concatenate([
            np.arange(n_customers),
            rng.integers(0, n_customers, size=n - n_customers),
        ])
        rng.shuffle(owner_idx)

    countries = customers["country_code"].to_numpy()[owner_idx]
    entities = customers["entity_type"].to_numpy()[owner_idx]
    onboarding = customers["onboarding_date"].to_numpy()[owner_idx]

    account_types = _draw_account_types(entities, rng)
    currencies = np.array([cfg.CURRENCY_BY_COUNTRY[c] for c in countries])

    # Solde : loi lognormale paramétrée par type de compte, tirée en euros.
    mu = np.array([_BALANCE_PARAMS_EUR[t][0] for t in account_types])
    sigma = np.array([_BALANCE_PARAMS_EUR[t][1] for t in account_types])
    balance_eur = rng.lognormal(mean=mu, sigma=sigma)
    balance = _amounts_from_eur(balance_eur, currencies)

    # Plafond de crédit : autorisation de découvert sur les comptes courants,
    # capital emprunté sur les prêts, nul ailleurs.
    credit_limit_eur = np.zeros(n)
    is_loan = account_types == "LOAN"
    is_current = account_types == "CURRENT"
    credit_limit_eur[is_loan] = balance_eur[is_loan] * rng.uniform(1.05, 1.60, is_loan.sum())
    credit_limit_eur[is_current] = balance_eur[is_current] * rng.uniform(0.10, 0.50, is_current.sum())
    credit_limit = _amounts_from_eur(credit_limit_eur, currencies)

    # Un compte ne peut pas être ouvert avant l'entrée en relation du client.
    onboarding_days = onboarding.astype("datetime64[D]")
    horizon = np.datetime64("2025-12-31", "D")
    span = (horizon - onboarding_days).astype(int)
    opened_date = onboarding_days + (rng.random(n) * span).astype(int).astype("timedelta64[D]")

    sequence = np.arange(1, n + 1)
    account_ids = _build_ids(countries, "A", 7)

    # Jours d'impayé portés par le compte de prêt lui-même.
    #
    # Un système bancaire central classe ses créances au niveau du contrat, pas
    # en recomposant l'historique de ses échéances. La distinction est décisive
    # pour le NPL : un prêt se rembourse mensuellement, donc toute fenêtre
    # d'observation plus courte qu'un cycle ne voit qu'une fraction du
    # portefeuille et sous-estime le ratio. Cette colonne rend l'indicateur
    # calculable comme un stock, indépendamment de la période observée.
    #
    # Le champ ne concerne que les prêts : il reste nul ailleurs.
    days_past_due = np.full(n, np.nan)
    is_loan = account_types == "LOAN"
    if is_loan.any():
        loan_ids = account_ids[is_loan].reset_index(drop=True)
        loan_countries = pd.Series(countries[is_loan])
        defaulting = calib.is_defaulting_account(loan_ids, loan_countries)
        days_past_due[is_loan] = calib.overdue_days_for(loan_ids, defaulting)

    # Prime annuelle portée par la police, exprimée dans sa devise.
    #
    # Même raisonnement que pour les jours d'impayé, appliqué à l'assurance. La
    # règle de fraude du §3.3 compare un sinistre à « trois fois la prime
    # annuelle versée » : la reconstituer depuis les échéances observées ne la
    # rendrait calculable que sur les polices ayant cotisé pendant la fenêtre
    # d'observation — 21 % des sinistres réglés sur trois jours de données. Pire,
    # y substituer une prime médiane de branche produirait des faux positifs en
    # série, les primes s'étalant sur un facteur dix : un sinistre normal sur une
    # police chère dépasserait trois fois la médiane sans rien avoir d'anormal.
    #
    # Un assureur connaît la prime de ses contrats ; elle relève des données de
    # référence, pas d'une inférence. Le champ ne concerne que les polices.
    annual_premium = np.full(n, np.nan)
    is_policy = account_types == "INSURANCE_POLICY"
    if is_policy.any():
        policy_ids = account_ids[is_policy].reset_index(drop=True)
        rates = pd.Series(currencies[is_policy]).map(cfg.FX_PER_EUR).to_numpy()
        annual_premium[is_policy] = np.round(
            calib.annual_premium_eur(policy_ids) * rates, 2
        )

    return pd.DataFrame({
        "account_id": account_ids,
        "customer_id": customers["customer_id"].to_numpy()[owner_idx],
        "country_code": countries,
        "entity_type": entities,
        "account_type": account_types,
        "account_number": pd.Series(sequence).astype(str).str.zfill(11),
        "iban": _build_ibans(countries, sequence, rng),
        "currency": currencies,
        "balance": balance,
        "credit_limit": credit_limit,
        "opened_date": opened_date,
        "status": rng.choice(
            np.array(cfg.ACCOUNT_STATUSES), size=n, p=np.array(cfg.ACCOUNT_STATUS_WEIGHTS)
        ),
        "days_past_due": days_past_due,
        "annual_premium": annual_premium,
    })


# =============================================================================
# Orchestration
# =============================================================================


def generate_all(
    sizes: dict[str, int] | None = None,
    seed: int | None = None,
) -> dict[str, pd.DataFrame]:
    """Génère les quatre référentiels dans l'ordre imposé par leurs dépendances.

    L'annexe A.8 insiste sur ce point : les référentiels doivent précéder les
    transactions, et accounts dépend de customers.
    """
    sizes = {**cfg.REFERENTIAL_SIZES, **(sizes or {})}
    rng = np.random.default_rng(seed)

    customers = generate_customers(sizes["customers"], rng)
    return {
        "customers": customers,
        "accounts": generate_accounts(customers, sizes["accounts"], rng),
        "branches": generate_branches(sizes["branches"], rng),
        "products": generate_products(sizes["products"], rng),
    }
