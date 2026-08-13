"""Injection et détection des comportements frauduleux.

Le Level 3 demande que trois règles de fraude et la surveillance AML produisent
des alertes « sur données simulées ». Or aucune de ces règles ne se déclenche
sur des données tirées au hasard : une rafale de virements depuis un même
compte en moins de cinq minutes est, par construction, un événement de
probabilité nulle quand chaque ligne tire son compte indépendamment.

Ce module comble ce vide. Il expose deux familles symétriques :

* `inject_*` — introduit les motifs dans les données générées, à taux
  paramétrable, **sans ajouter de colonne indicatrice**. Marquer les lignes
  frauduleuses reviendrait à donner la réponse au pipeline de détection et
  ferait diverger le schéma de l'annexe.
* `detect_*` — implémentation de référence des règles de l'énoncé, utilisée par
  les tests et le test de fumée pour vérifier que l'injection a bien eu lieu.
  Ces fonctions constituent l'oracle que les jobs Spark Structured Streaming du
  Level 3 devront reproduire, ce qui donne un moyen objectif de valider leur
  implémentation.
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd

from generator import calibration as calib
from generator import config as cfg


@dataclass(frozen=True)
class AnomalyReport:
    """Trace d'une règle appliquée à un lot, affichée dans l'interface.

    `rows` compte les lignes **réellement modifiées**, et non les lignes qui
    déclencheraient la règle. La distinction compte : environ 6 % des virements
    dépassent naturellement le seuil AML, et les compter comme des injections
    donnerait un taux d'anomalies quatre fois supérieur à celui demandé.
    """

    rule: str
    rows: int
    detail: str


def _recompute_fees(frame: pd.DataFrame, currency: str) -> np.ndarray:
    decimals = 0 if currency == "XOF" else 2
    return np.where(
        frame["transaction_status"].to_numpy() == "SUCCESS",
        np.round(frame["amount"].to_numpy() * 0.001, decimals),
        0.0,
    )


# =============================================================================
# Règle 1 — rafales de virements de montant élevé
# =============================================================================


def inject_transaction_bursts(
    frame: pd.DataFrame,
    country_code: str,
    rate: float,
    rng: np.random.Generator,
) -> tuple[pd.DataFrame, AnomalyReport]:
    """Concentre plusieurs virements de montant élevé sur un même compte.

    Les lignes existantes sont réécrites plutôt que complétées : le nombre de
    lignes demandé par l'utilisateur reste exact, et le fichier produit conserve
    la volumétrie annoncée.
    """
    currency = cfg.CURRENCY_BY_COUNTRY[country_code]
    threshold = cfg.FRAUD_BURST_AMOUNT[currency]
    burst_size = cfg.FRAUD_BURST_MIN_COUNT

    n_bursts = int(len(frame) * rate) // burst_size
    if n_bursts == 0:
        return frame, AnomalyReport("fraude_rafale", 0, "lot trop petit pour une rafale")

    frame = frame.copy()
    positions = rng.choice(len(frame), size=n_bursts * burst_size, replace=False)
    accounts = frame["account_id"].to_numpy()[rng.integers(0, len(frame), size=n_bursts)]

    for burst, account in enumerate(accounts):
        rows = positions[burst * burst_size:(burst + 1) * burst_size]
        # Toutes les opérations de la rafale tombent dans la même fenêtre de
        # moins de cinq minutes, condition de déclenchement de la règle.
        base = frame["timestamp"].to_numpy()[rows[0]]
        offsets = np.sort(rng.integers(0, cfg.FRAUD_BURST_WINDOW_MINUTES * 60 - 30, size=burst_size))

        frame.iloc[rows, frame.columns.get_loc("account_id")] = account
        frame.iloc[rows, frame.columns.get_loc("timestamp")] = base + offsets.astype("timedelta64[s]")
        frame.iloc[rows, frame.columns.get_loc("amount")] = np.round(
            threshold * rng.uniform(1.2, 4.0, burst_size), 0 if currency == "XOF" else 2
        )
        frame.iloc[rows, frame.columns.get_loc("transaction_type")] = "TRANSFER"
        frame.iloc[rows, frame.columns.get_loc("transaction_status")] = "SUCCESS"

    frame["fee_amount"] = _recompute_fees(frame, currency)
    return frame, AnomalyReport(
        "fraude_rafale", n_bursts * burst_size,
        f"{n_bursts} rafales de {burst_size} virements > {threshold:,.0f} {currency} en moins de "
        f"{cfg.FRAUD_BURST_WINDOW_MINUTES} min",
    )


def detect_transaction_bursts(frame: pd.DataFrame) -> pd.DataFrame:
    """Règle 1 de l'énoncé : transactions multiples de montant élevé, même compte, moins de 5 min."""
    threshold = frame["currency"].map(cfg.FRAUD_BURST_AMOUNT)
    large = frame[frame["amount"] > threshold].sort_values(["account_id", "timestamp"])
    if large.empty:
        return large.iloc[:0]

    window = np.timedelta64(cfg.FRAUD_BURST_WINDOW_MINUTES, "m")
    size = cfg.FRAUD_BURST_MIN_COUNT

    flagged: list[pd.DataFrame] = []
    for _, group in large.groupby("account_id", sort=False):
        if len(group) < size:
            continue
        times = group["timestamp"].to_numpy()

        # Seules les opérations appartenant réellement à une fenêtre qualifiante
        # sont retenues. Renvoyer tout l'historique d'un compte incriminé
        # noierait l'alerte : un compte peut émettre de gros virements espacés
        # de plusieurs jours sans que cela constitue une rafale.
        in_burst = np.zeros(len(group), dtype=bool)
        for start in range(len(group) - size + 1):
            if times[start + size - 1] - times[start] <= window:
                in_burst[start:start + size] = True

        if in_burst.any():
            flagged.append(group[in_burst])

    return pd.concat(flagged) if flagged else large.iloc[:0]


# =============================================================================
# Règle 2 — paiement mobile money depuis un pays inhabituel
# =============================================================================


def inject_foreign_origin_payments(
    frame: pd.DataFrame,
    index,  # ReferentialIndex, importé paresseusement pour éviter un cycle
    country_code: str,
    rate: float,
    rng: np.random.Generator,
) -> tuple[pd.DataFrame, AnomalyReport]:
    """Fait émettre des paiements par des clients dont le pays d'origine diffère.

    Le paiement part bien du pays de l'entité émettrice, mais l'émetteur est un
    client rattaché à un autre pays : c'est le profil « client en déplacement »
    que la règle doit repérer par jointure avec le référentiel clients.
    """
    others = [
        c for c in cfg.ENTITY_COUNTRIES["MOBILE_MONEY"]
        if c != country_code and c in index.countries
    ]
    n_rows = int(len(frame) * rate)
    if n_rows == 0 or not others:
        return frame, AnomalyReport("fraude_pays_inhabituel", 0, "aucun pays émetteur alternatif")

    frame = frame.copy()
    positions = rng.choice(len(frame), size=n_rows, replace=False)
    foreign_countries = rng.choice(np.array(others), size=n_rows)

    foreign_ids = np.empty(n_rows, dtype=object)
    for code in np.unique(foreign_countries):
        mask = foreign_countries == code
        pool = index.view(code, "wallet_customers")["customer_id"].to_numpy()
        foreign_ids[mask] = pool[rng.integers(0, len(pool), size=int(mask.sum()))]

    frame.iloc[positions, frame.columns.get_loc("sender_id")] = foreign_ids
    return frame, AnomalyReport(
        "fraude_pays_inhabituel", n_rows,
        f"{n_rows} paiements émis depuis {country_code} par des clients d'un autre pays",
    )


def detect_foreign_origin_payments(
    frame: pd.DataFrame, customers: pd.DataFrame
) -> pd.DataFrame:
    """Règle 2 de l'énoncé : paiement mobile money depuis un pays inhabituel pour le client."""
    home = customers.set_index("customer_id")["country_code"]
    origin = frame["sender_id"].map(home)
    return frame[origin.notna() & (origin != frame["sender_country"])]


# =============================================================================
# Règle 3 — sinistre disproportionné par rapport à la prime
# =============================================================================


def inject_excessive_claims(
    frame: pd.DataFrame,
    country_code: str,
    rate: float,
    rng: np.random.Generator,
) -> tuple[pd.DataFrame, AnomalyReport]:
    """Porte quelques règlements de sinistre au-delà de trois années de cotisation."""
    currency = cfg.CURRENCY_BY_COUNTRY[country_code]
    claims = np.flatnonzero(frame["operation_type"].to_numpy() == "CLAIM_PAYMENT")

    n_rows = min(int(len(frame) * rate), len(claims))
    if n_rows == 0:
        return frame, AnomalyReport("fraude_sinistre_excessif", 0, "aucun règlement de sinistre dans le lot")

    frame = frame.copy()
    positions = rng.choice(claims, size=n_rows, replace=False)
    annual_eur = calib.annual_premium_eur(frame["account_id"].iloc[positions])
    multiple = rng.uniform(3.2, 8.0, n_rows)
    inflated = np.round(
        annual_eur * multiple * cfg.FX_PER_EUR[currency], 0 if currency == "XOF" else 2
    )

    frame.iloc[positions, frame.columns.get_loc("amount")] = inflated
    frame.iloc[positions, frame.columns.get_loc("claim_status")] = "PAID"
    return frame, AnomalyReport(
        "fraude_sinistre_excessif", n_rows,
        f"{n_rows} sinistres réglés entre 3,2 et 8 fois la prime annuelle de la police",
    )


def detect_excessive_claims(frame: pd.DataFrame) -> pd.DataFrame:
    """Règle 3 de l'énoncé : montant de sinistre supérieur à 3 fois la prime annuelle."""
    claims = frame[frame["operation_type"] == "CLAIM_PAYMENT"]
    if claims.empty:
        return claims
    annual_local = calib.annual_premium_eur(claims["account_id"]) * claims["currency"].map(cfg.FX_PER_EUR)
    return claims[claims["amount"] > cfg.FRAUD_CLAIM_PREMIUM_RATIO * annual_local]


# =============================================================================
# Surveillance AML
# =============================================================================


def detect_aml_events(frame: pd.DataFrame) -> pd.DataFrame:
    """Virements dépassant le seuil déclaratif de la zone.

    L'énoncé vise explicitement les virements : un retrait au distributeur ou un
    paiement marchand du même montant ne relève pas de la même obligation
    déclarative.
    """
    threshold = frame["currency"].map(cfg.AML_THRESHOLD)
    is_wire = frame["transaction_type"].isin(("TRANSFER", "INTERNATIONAL_WIRE"))
    return frame[is_wire & (frame["amount"] > threshold)]


def ensure_aml_events(
    frame: pd.DataFrame,
    country_code: str,
    minimum: int,
    rng: np.random.Generator,
) -> tuple[pd.DataFrame, AnomalyReport]:
    """Garantit un volume plancher de franchissements du seuil déclaratif.

    Sur les lots volumineux, environ 6 % des transactions dépassent
    naturellement le seuil et cette fonction ne fait rien. Elle n'intervient que
    sur les petits lots du mode continu, où un tirage malchanceux pourrait ne
    produire aucun événement et laisser la démonstration du Level 3 sans rien à
    montrer.
    """
    existing = len(detect_aml_events(frame))
    missing = minimum - existing
    if missing <= 0:
        return frame, AnomalyReport(
            "aml_seuil_declaratif", 0,
            f"{existing} virements dépassent naturellement le seuil — aucune injection nécessaire",
        )

    currency = cfg.CURRENCY_BY_COUNTRY[country_code]
    threshold = cfg.AML_THRESHOLD[currency]

    # Les lignes déjà au-dessus du seuil sont exclues du tirage : les
    # sélectionner ne ferait pas progresser le compte total.
    already = frame.index.get_indexer(detect_aml_events(frame).index)
    candidates = np.setdiff1d(np.arange(len(frame)), already)
    if candidates.size == 0:
        return frame, AnomalyReport("aml_seuil_declaratif", 0, "toutes les lignes dépassent déjà le seuil")

    frame = frame.copy()
    positions = rng.choice(candidates, size=min(missing, candidates.size), replace=False)
    frame.iloc[positions, frame.columns.get_loc("transaction_type")] = "TRANSFER"
    frame.iloc[positions, frame.columns.get_loc("amount")] = np.round(
        threshold * rng.uniform(1.1, 3.0, len(positions)), 0 if currency == "XOF" else 2
    )
    frame["fee_amount"] = _recompute_fees(frame, currency)
    return frame, AnomalyReport(
        "aml_seuil_declaratif", len(positions),
        f"{len(positions)} virements portés au-dessus de {threshold:,.0f} {currency} "
        f"(le lot n'en comptait que {existing})",
    )


# =============================================================================
# Aiguillage
# =============================================================================


def inject(
    kind: str,
    frame: pd.DataFrame,
    index,
    country_code: str,
    rate: float,
    rng: np.random.Generator,
) -> tuple[pd.DataFrame, list[AnomalyReport]]:
    """Applique les injections pertinentes pour un type de données."""
    if rate <= 0:
        return frame, []

    reports: list[AnomalyReport] = []
    if kind == "bank_txn":
        frame, report = inject_transaction_bursts(frame, country_code, rate, rng)
        reports.append(report)
        frame, report = ensure_aml_events(frame, country_code, minimum=5, rng=rng)
        reports.append(report)
    elif kind == "mobile_money":
        frame, report = inject_foreign_origin_payments(frame, index, country_code, rate, rng)
        reports.append(report)
    elif kind == "insurance_ops":
        frame, report = inject_excessive_claims(frame, country_code, rate, rng)
        reports.append(report)

    return frame, reports
